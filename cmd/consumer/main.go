package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"e-go04/internal/backpressure"
	"e-go04/internal/breaker"
	"e-go04/internal/broker"
	"e-go04/internal/dedup"
	"e-go04/internal/domain"
	"e-go04/internal/inventory"
	"e-go04/internal/metrics"
)

type inventoryTask struct {
	StreamID string
	Event    *domain.Event
}

func main() {
	redisAddr := flag.String("redis", "localhost:6379", "Redis server address")
	streamKey := flag.String("stream", "hotsale:events", "Redis stream name")
	groupName := flag.String("group", "hotsale_group", "Consumer group name")
	consumerID := flag.String("consumer", "consumer_resilient", "Consumer worker identifier")
	inventoryURL := flag.String("inventory-url", "http://localhost:8090", "Base URL for the external inventory service")
	windowSize := flag.Int("window-size", 250000, "Sliding deduplication window capacity")
	batchSize := flag.Int64("batch-size", 500, "Maximum messages per XREADGROUP fetch")
	workers := flag.Int("workers", 12, "Number of concurrent workers for inventory verification")
	idleTimeout := flag.Duration("idle-timeout", 10*time.Second, "Idle timeout to consider stream fully consumed")
	autoExit := flag.Bool("auto-exit", true, "Exit and report results after EOF and full buffer drain")

	// Resilience configuration flags
	cbFailureThreshold := flag.Int("cb-failures", 5, "Consecutive failures to trip circuit breaker to OPEN")
	cbCooldown := flag.Duration("cb-cooldown", 4*time.Second, "Cooldown duration before circuit breaker transitions to HALF_OPEN")
	cbProbes := flag.Int("cb-probes", 3, "Successful trial requests in HALF_OPEN to close circuit breaker")
	bpHighWatermark := flag.Int("bp-high", 300, "Buffer depth threshold to activate backpressure throttling")
	bpLowWatermark := flag.Int("bp-low", 30, "Buffer depth threshold to release backpressure throttling")
	bufferCap := flag.Int("buffer-cap", 50000, "Maximum capacity for the deferred retry buffer")

	flag.Parse()

	fmt.Println("================================================================================")
	fmt.Println("       HOT SALE LATAM — CONSUMIDOR RESILIENTE CON CIRCUIT BREAKER")
	fmt.Println("================================================================================")
	fmt.Printf("Configuración Operacional:\n")
	fmt.Printf("  • Broker Redis         : %s\n", *redisAddr)
	fmt.Printf("  • Stream               : %s\n", *streamKey)
	fmt.Printf("  • Consumer Group       : %s (ID: %s)\n", *groupName, *consumerID)
	fmt.Printf("  • Servicio Inventario  : %s\n", *inventoryURL)
	fmt.Printf("  • Concurrencia Workers : %d goroutines\n", *workers)
	fmt.Printf("  • Deduplicación (LRU)  : %s slots de capacidad\n", formatInt(int64(*windowSize)))
	fmt.Printf("  • Circuit Breaker      : %d fallos consec. | Cooldown: %v | Probes: %d\n",
		*cbFailureThreshold, *cbCooldown, *cbProbes)
	fmt.Printf("  • Backpressure         : High Watermark: %d | Low Watermark: %d | Capacidad: %d\n",
		*bpHighWatermark, *bpLowWatermark, *bufferCap)
	fmt.Println("--------------------------------------------------------------------------------")

	b, err := broker.NewRedisBroker(*redisAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error conectando al broker Redis: %v\n", err)
		os.Exit(1)
	}
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.EnsureConsumerGroup(ctx, *streamKey, *groupName); err != nil {
		fmt.Fprintf(os.Stderr, "Error inicializando consumer group: %v\n", err)
		os.Exit(1)
	}

	cbCfg := breaker.Config{
		FailureThreshold:         *cbFailureThreshold,
		Cooldown:                 *cbCooldown,
		HalfOpenSuccessThreshold: *cbProbes,
		OnStateChange: func(from, to breaker.State) {
			fmt.Printf("  [CIRCUIT BREAKER] Transición de estado: %s -> %s en %s\n",
				from, to, time.Now().Format("15:04:05.000"))
		},
	}
	cb := breaker.New(cbCfg)

	invClient := inventory.NewClient(*inventoryURL, cb, 2500*time.Millisecond)

	retryBuffer := backpressure.NewRetryBuffer(*bufferCap)
	bpController := backpressure.NewController(backpressure.ControllerConfig{
		HighWatermark:    *bpHighWatermark,
		LowWatermark:     *bpLowWatermark,
		MinThrottleDelay: 40 * time.Millisecond,
		MaxThrottleDelay: 250 * time.Millisecond,
	})

	deduplicator := dedup.NewSlidingLRUDeduplicator(*windowSize)
	aggregator := domain.NewAggregator()
	latencyTracker := metrics.NewLatencyTracker(100000)
	throughputMeter := metrics.NewThroughputMeter()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n[INFO] Señal de terminación recibida. Drenando buffers y generando reporte...")
		cancel()
	}()

	fmt.Printf("Esperando eventos en stream '%s'...\n", *streamKey)

	var (
		totalRead     int64
		lastLogCount  int64
		lastLogTime   = time.Now()
		startTime     time.Time
		startedTiming = false
		lastMsgTime   = time.Now()
		eofReceived   = false
	)

	// Concurrency semaphore for inventory verification workers
	workerSem := make(chan struct{}, *workers)
	ackBatch := make([]string, 0, *batchSize)
	var ackMu sync.Mutex

	for {
		select {
		case <-ctx.Done():
			goto drainAndFinish
		default:
		}

		// 1. Process deferred items waiting in the retry buffer
		drainRetryBuffer(ctx, b, *streamKey, *groupName, retryBuffer, invClient, cb, aggregator, workerSem, false)

		// 2. Evaluate backpressure signal
		isThrottled := bpController.Evaluate(retryBuffer.Len(), cb.State())
		fetchCount := *batchSize
		if isThrottled {
			throttleDelay := bpController.ThrottleDelay(retryBuffer.Len())
			if throttleDelay > 0 {
				time.Sleep(throttleDelay)
			}
			fetchCount = bpController.DynamicBatchSize(*batchSize, retryBuffer.Len())
		}

		// 3. Read batch from Redis Stream
		msgs, err := b.ReadGroupBatch(ctx, *streamKey, *groupName, *consumerID, fetchCount, 1*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				goto drainAndFinish
			}
			fmt.Fprintf(os.Stderr, "Advertencia leyendo del stream: %v\n", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if len(msgs) == 0 {
			if startedTiming && *autoExit && time.Since(lastMsgTime) >= *idleTimeout {
				fmt.Printf("\n[INFO] Idle timeout de %v alcanzado sin nuevos mensajes.\n", *idleTimeout)
				goto drainAndFinish
			}
			continue
		}

		if !startedTiming {
			startTime = time.Now()
			startedTiming = true
		}
		lastMsgTime = time.Now()

		ackBatch = ackBatch[:0]
		inventoryTasks := make([]inventoryTask, 0, len(msgs))

		// 4. Ingest and classify messages
		for _, msg := range msgs {
			if msg.EventID == "EOF_SIGNAL" {
				eofReceived = true
				ackBatch = append(ackBatch, msg.ID)
				continue
			}

			totalRead++
			aggregator.RecordLine()

			if !msg.PublishTS.IsZero() {
				lat := time.Since(msg.PublishTS)
				latencyTracker.Record(lat)
			}

			if deduplicator.IsDuplicate(msg.EventID) {
				aggregator.RecordDuplicate()
				ackBatch = append(ackBatch, msg.ID)
				continue
			}

			var ev domain.Event
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
				ackBatch = append(ackBatch, msg.ID)
				continue
			}

			if ev.EventType == domain.EventTypeInventoryReserved {
				inventoryTasks = append(inventoryTasks, inventoryTask{
					StreamID: msg.ID,
					Event:    &ev,
				})
			} else {
				aggregator.ProcessUniqueEvent(&ev)
				ackBatch = append(ackBatch, msg.ID)
			}
		}

		// 5. Execute inventory checks concurrently with bounded workers
		if len(inventoryTasks) > 0 {
			var wg sync.WaitGroup
			for _, task := range inventoryTasks {
				wg.Add(1)
				workerSem <- struct{}{}
				go func(t inventoryTask) {
					defer wg.Done()
					defer func() { <-workerSem }()

					err := invClient.CheckInventory(ctx, t.Event.OrderID, t.Event.Country)
					if err != nil {
						// Service degraded or breaker open: enqueue in retry buffer
						retryItem := &backpressure.RetryItem{
							StreamID:  t.StreamID,
							Event:     t.Event,
							Retries:   0,
							NextRetry: time.Now().Add(50 * time.Millisecond),
						}
						retryBuffer.Push(retryItem)
						bpController.Evaluate(retryBuffer.Len(), cb.State())
					} else {
						// Success
						aggregator.ProcessUniqueEvent(t.Event)
						ackMu.Lock()
						ackBatch = append(ackBatch, t.StreamID)
						ackMu.Unlock()
					}
				}(task)
			}
			wg.Wait()
		}

		// 6. Acknowledge completed messages in Redis
		if len(ackBatch) > 0 {
			if err := b.AckMessages(ctx, *streamKey, *groupName, ackBatch); err != nil {
				fmt.Fprintf(os.Stderr, "Advertencia confirmando XACK: %v\n", err)
			}
		}

		throughputMeter.Add(int64(len(msgs)))

		// Periodic progress telemetry logging every 2 seconds
		if time.Since(lastLogTime) >= 2*time.Second {
			now := time.Now()
			deltaCount := totalRead - lastLogCount
			deltaSecs := now.Sub(lastLogTime).Seconds()
			instRate := float64(deltaCount) / deltaSecs

			fmt.Printf("  -> Procesados: %9s | Rate: %6.0f ev/s | Breaker: %-9s | Buffer: %4d (max: %4d) | Duplicados: %6s\n",
				formatInt(totalRead),
				instRate,
				cb.State(),
				retryBuffer.Len(),
				retryBuffer.MaxDepth(),
				formatInt(deduplicator.DuplicateCount()),
			)

			lastLogCount = totalRead
			lastLogTime = now
		}

		if eofReceived && *autoExit {
			fmt.Println("\n[INFO] Señal EOF recibida del productor.")
			goto drainAndFinish
		}
	}

drainAndFinish:
	if retryBuffer.Len() > 0 {
		fmt.Printf("\n[INFO] Drenando buffer de reintentos (%d eventos pendientes)...\n", retryBuffer.Len())
		drainRetryBuffer(context.Background(), b, *streamKey, *groupName, retryBuffer, invClient, cb, aggregator, workerSem, true)
		fmt.Printf("[INFO] Buffer de reintentos completamente drenado. Eventos restantes: %d\n", retryBuffer.Len())
	}

	totalDuration := time.Since(startTime)
	if totalDuration <= 0 {
		totalDuration = time.Millisecond
	}

	fmt.Println("\n" + strings.Repeat("=", 88))
	fmt.Println("             REPORTE FINAL DE AUDITORÍA Y RESILIENCIA — E-GO04")
	fmt.Println(strings.Repeat("=", 88))

	// 1. Business aggregates by country
	aggregator.PrintReport()

	// 2. Circuit Breaker Telemetry
	fmt.Println(strings.Repeat("-", 88))
	cb.PrintReport()

	// 3. Backpressure & Retry Buffer Telemetry
	fmt.Println(strings.Repeat("-", 88))
	bpController.PrintReport()
	bufStats := retryBuffer.Stats()
	fmt.Printf("Retry Buffer Telemetry (internal/backpressure):\n")
	fmt.Printf("  • Profundidad Máxima del Buffer (Peak Depth) : %d eventos\n", bufStats.MaxDepth)
	fmt.Printf("  • Total de Eventos Encolados para Reintento   : %d\n", bufStats.TotalEnqueued)
	fmt.Printf("  • Total de Eventos Desencolados y Resueltos   : %d\n", bufStats.TotalDequeued)
	fmt.Printf("  • Eventos Residuales al Finalizar             : %d (Objetivo estricto: 0)\n", bufStats.CurrentDepth)

	// 4. Inventory Client Telemetry
	fmt.Println(strings.Repeat("-", 88))
	invClient.PrintReport()

	// 5. Deduplication Engine Diagnostics
	fmt.Println(strings.Repeat("-", 88))
	fmt.Println("Auditoría del Motor de Deduplicación Bounded (internal/dedup):")
	fmt.Printf("  • Capacidad de la ventana LRU        : %12s slots\n", formatInt(int64(deduplicator.Capacity())))
	fmt.Printf("  • Entradas activas en memoria        : %12s\n", formatInt(int64(deduplicator.CurrentSize())))
	fmt.Printf("  • Desalojos por límite de capacidad  : %12s\n", formatInt(deduplicator.EvictionCount()))
	fmt.Printf("  • Eventos únicos registrados         : %12s\n", formatInt(deduplicator.UniqueCount()))
	fmt.Printf("  • Duplicados detectados y descartados: %12s\n", formatInt(deduplicator.DuplicateCount()))

	// 6. Throughput & Latency Distribution
	fmt.Println(strings.Repeat("-", 88))
	avgThroughput := float64(totalRead) / totalDuration.Seconds()
	fmt.Printf("Métricas de Rendimiento Global:\n")
	fmt.Printf("  • Mensajes totales procesados : %12s\n", formatInt(totalRead))
	fmt.Printf("  • Tiempo total de consumo     : %12s\n", totalDuration.Round(time.Millisecond))
	fmt.Printf("  • Throughput del consumidor   : %12.0f eventos/segundo\n", avgThroughput)

	latStats := latencyTracker.Stats()
	latStats.PrintTable()

	// 7. Data Loss Reconciliation Check (Zero Data Loss Verification)
	fmt.Println(strings.Repeat("=", 88))
	snap := aggregator.Snapshot()
	expectedTotalLines := int64(3126750)
	expectedUnique := int64(3035188)
	expectedDuplicates := int64(91562)
	expectedInventory := int64(815590)

	fmt.Println("VERIFICACIÓN DE CONCILIACIÓN DE DATOS (CERO PÉRDIDA):")
	fmt.Printf("  • Líneas Recibidas     : %10s / Esperado: %10s  -> %s\n",
		formatInt(snap.TotalLines), formatInt(expectedTotalLines), matchStatus(snap.TotalLines, expectedTotalLines))
	fmt.Printf("  • Eventos Únicos       : %10s / Esperado: %10s  -> %s\n",
		formatInt(snap.TotalUnique), formatInt(expectedUnique), matchStatus(snap.TotalUnique, expectedUnique))
	fmt.Printf("  • Duplicados Detectados: %10s / Esperado: %10s  -> %s\n",
		formatInt(snap.TotalDuplicates), formatInt(expectedDuplicates), matchStatus(snap.TotalDuplicates, expectedDuplicates))
	fmt.Printf("  • Inventario Verificado: %10s / Esperado: %10s  -> %s\n",
		formatInt(snap.CountsByType[domain.EventTypeInventoryReserved]),
		formatInt(expectedInventory),
		matchStatus(snap.CountsByType[domain.EventTypeInventoryReserved], expectedInventory))
	fmt.Println(strings.Repeat("=", 88))
}

// drainRetryBuffer processes ready items from the retry buffer, executing inventory checks.
func drainRetryBuffer(
	ctx context.Context,
	b *broker.RedisBroker,
	streamKey, groupName string,
	buf *backpressure.RetryBuffer,
	client *inventory.Client,
	cb *breaker.CircuitBreaker,
	agg *domain.Aggregator,
	sem chan struct{},
	drainAll bool,
) {
	var ackMu sync.Mutex
	acks := make([]string, 0, 64)

	maxDrain := 30
	drainedCount := 0

	for {
		if buf.Len() == 0 {
			if drainAll {
				// Wait for in-flight worker goroutines to finish and re-check if any item failed back into buffer
				waitForWorkers(sem)
				if buf.Len() == 0 {
					break // Truly 0 items remaining
				}
				continue
			}
			break
		}

		if !drainAll && drainedCount >= maxDrain {
			break
		}

		if cb.State() == breaker.StateOpen {
			if drainAll {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			break
		}

		item := buf.PopReady(time.Now())
		if item == nil {
			if drainAll && buf.Len() > 0 {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			break
		}

		drainedCount++
		sem <- struct{}{}
		go func(it *backpressure.RetryItem) {
			defer func() { <-sem }()

			err := client.CheckInventory(ctx, it.Event.OrderID, it.Event.Country)
			if err != nil {
				it.Retries++
				backoff := time.Duration(1<<uint(min(it.Retries, 5))) * 50 * time.Millisecond
				if backoff > 1500*time.Millisecond {
					backoff = 1500 * time.Millisecond
				}
				it.NextRetry = time.Now().Add(backoff)
				buf.Push(it)
			} else {
				agg.ProcessUniqueEvent(it.Event)
				ackMu.Lock()
				acks = append(acks, it.StreamID)
				if len(acks) >= 50 {
					_ = b.AckMessages(ctx, streamKey, groupName, acks)
					acks = acks[:0]
				}
				ackMu.Unlock()
			}
		}(item)
	}

	if drainAll {
		waitForWorkers(sem)
	}

	// Drain remaining acks
	ackMu.Lock()
	if len(acks) > 0 {
		_ = b.AckMessages(ctx, streamKey, groupName, acks)
	}
	ackMu.Unlock()
}

func waitForWorkers(sem chan struct{}) {
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}
	for i := 0; i < cap(sem); i++ {
		<-sem
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func matchStatus(actual, expected int64) string {
	if actual == expected {
		return "EXACTO (100% CUMPLIDO)"
	}
	return fmt.Sprintf("DIFERENCIA: %+d", actual-expected)
}

func formatInt(n int64) string {
	in := fmt.Sprintf("%d", n)
	var out []byte
	l := len(in)
	for i, c := range in {
		if i > 0 && (l-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}
