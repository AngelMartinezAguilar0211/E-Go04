package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"e-go04/internal/broker"
	"e-go04/internal/metrics"
)

type eventHeader struct {
	EventID string `json:"event_id"`
}

type PacingPhase string

const (
	PhaseWarmup   PacingPhase = "NORMAL (WARMUP)"
	PhaseSpike    PacingPhase = "SPIKE (10x TRAFFIC)"
	PhaseCooldown PacingPhase = "NORMAL (COOLDOWN)"
)

func main() {
	filePath := flag.String("file", "eventos_hotsale.jsonl", "Path to the JSONL event log file")
	redisAddr := flag.String("redis", "localhost:6379", "Redis server address")
	streamKey := flag.String("stream", "hotsale:events", "Redis stream name")
	batchSize := flag.Int("batch-size", 50, "Maximum batch size for pipelined XADD")
	cleanStream := flag.Bool("clean", false, "Delete existing stream before publishing")

	// Pacing parameters
	normalRate := flag.Float64("normal-rate", 50, "Sustained normal publishing rate in events/second")
	spikeMultiplier := flag.Float64("spike-multiplier", 10.0, "Multiplier for the spike window (default 10x)")
	spikeDelay := flag.Duration("spike-delay", 15*time.Second, "Delay before initiating the 10x spike window")
	spikeDuration := flag.Duration("spike-duration", 120*time.Second, "Duration of the continuous 10x spike window (min 120s)")
	maxEvents := flag.Int64("max-events", 0, "Optional limit of events to publish (0 for entire file)")

	flag.Parse()

	spikeRate := (*normalRate) * (*spikeMultiplier)

	fmt.Println("================================================================================")
	fmt.Println("         HOT SALE LATAM — PACED PRODUCER CON VENTANA DE SPIKE 10X")
	fmt.Println("================================================================================")
	fmt.Printf("Configuración del Productor:\n")
	fmt.Printf("  • Archivo fuente        : %s\n", *filePath)
	fmt.Printf("  • Broker Redis          : %s\n", *redisAddr)
	fmt.Printf("  • Stream Destino        : %s\n", *streamKey)
	fmt.Printf("  • Ritmo Normal          : %.0f eventos/s\n", *normalRate)
	fmt.Printf("  • Ritmo en Spike (10x)  : %.0f eventos/s\n", spikeRate)
	fmt.Printf("  • Inicio del Spike      : tras %v de corrida\n", *spikeDelay)
	fmt.Printf("  • Duración del Spike    : %v continuos (al menos 2 minutos)\n", *spikeDuration)
	fmt.Printf("  • Tamaño de Batch       : %d eventos\n", *batchSize)
	fmt.Printf("  • Limpieza Inicial      : %t\n", *cleanStream)
	fmt.Println("--------------------------------------------------------------------------------")

	file, err := os.Open(*filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: no se pudo abrir el archivo %s: %v\n", *filePath, err)
		os.Exit(1)
	}
	defer file.Close()

	b, err := broker.NewRedisBroker(*redisAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error conectando al broker Redis: %v\n", err)
		os.Exit(1)
	}
	defer b.Close()

	ctx := context.Background()

	if *cleanStream {
		fmt.Printf("Purgando stream existente '%s'...\n", *streamKey)
		b.Client().Del(ctx, *streamKey)
	}

	meter := metrics.NewThroughputMeter()
	scanner := bufio.NewScanner(file)
	const maxCapacity = 1024 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	batch := make([]broker.PublishItem, 0, *batchSize)
	var (
		lineCount      int64
		warmupLines    int64
		spikeLines     int64
		cooldownLines  int64
		lastLogCount   int64
		currentPhase   = PhaseWarmup
		spikeStarted   = false
		spikeCompleted = false
		spikeStartTime time.Time
	)

	startTime := time.Now()
	lastLogTime := startTime

	// Rate limiter token bucket state
	tokens := 0.0
	lastTokenTime := time.Now()

	fmt.Printf("Iniciando publicación con pacing...\n")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		now := time.Now()
		elapsed := now.Sub(startTime)

		// State machine for pacing phases
		if !spikeStarted && elapsed >= *spikeDelay {
			currentPhase = PhaseSpike
			spikeStarted = true
			spikeStartTime = now
			fmt.Println("\n" + strings.Repeat("!", 80))
			fmt.Printf(">>> [ALERTA] INICIANDO VENTANA DE SPIKE 10X (%.0f ev/s) DURANTE %v <<<\n", spikeRate, *spikeDuration)
			fmt.Println(strings.Repeat("!", 80) + "\n")
		} else if spikeStarted && !spikeCompleted && now.Sub(spikeStartTime) >= *spikeDuration {
			currentPhase = PhaseCooldown
			spikeCompleted = true
			fmt.Println("\n" + strings.Repeat("-", 80))
			fmt.Printf(">>> [INFO] VENTANA DE SPIKE FINALIZADA. RETORNANDO A RITMO NORMAL (%.0f ev/s) <<<\n", *normalRate)
			fmt.Println(strings.Repeat("-", 80) + "\n")
		}

		currentRate := *normalRate
		if currentPhase == PhaseSpike {
			currentRate = spikeRate
		}

		// Replenish tokens based on elapsed duration and target rate
		tokenElapsed := now.Sub(lastTokenTime).Seconds()
		lastTokenTime = now
		tokens += tokenElapsed * currentRate
		if tokens > float64(*batchSize)*2 {
			tokens = float64(*batchSize) * 2 // Cap burst accumulation
		}

		// If out of tokens, sleep until next token is available
		if tokens < 1.0 {
			sleepNeeded := time.Duration((1.0 - tokens) / currentRate * float64(time.Second))
			if sleepNeeded > 0 {
				time.Sleep(sleepNeeded)
				now = time.Now()
				tokenElapsed = now.Sub(lastTokenTime).Seconds()
				lastTokenTime = now
				tokens += tokenElapsed * currentRate
			}
		}

		tokens -= 1.0
		lineCount++

		switch currentPhase {
		case PhaseWarmup:
			warmupLines++
		case PhaseSpike:
			spikeLines++
		case PhaseCooldown:
			cooldownLines++
		}

		header := eventHeader{EventID: extractEventIDFast(line)}
		batch = append(batch, broker.PublishItem{
			EventID: header.EventID,
			Payload: line,
		})

		// Publish batch when size is met or current rate demands flush
		if len(batch) >= *batchSize {
			pubTS := time.Now()
			if err := b.PublishBatch(ctx, *streamKey, batch, pubTS); err != nil {
				fmt.Fprintf(os.Stderr, "\nError publicando batch en línea %d: %v\n", lineCount, err)
				os.Exit(1)
			}
			meter.Add(int64(len(batch)))
			batch = batch[:0]
		}

		// Periodic status logging every 2 seconds
		if now.Sub(lastLogTime) >= 2*time.Second {
			deltaLines := lineCount - lastLogCount
			deltaSecs := now.Sub(lastLogTime).Seconds()
			instRate := float64(deltaLines) / deltaSecs

			var phaseInfo string
			if currentPhase == PhaseSpike {
				remaining := (*spikeDuration) - now.Sub(spikeStartTime)
				if remaining < 0 {
					remaining = 0
				}
				phaseInfo = fmt.Sprintf("SPIKE (restante: %s)", remaining.Round(time.Second))
			} else {
				phaseInfo = string(currentPhase)
			}

			fmt.Printf("  [%-22s] Publicados: %8s | Rate: %6.0f ev/s (obj: %4.0f) | Tiempo: %s\n",
				phaseInfo,
				formatInt(lineCount),
				instRate,
				currentRate,
				now.Sub(startTime).Round(time.Second),
			)

			lastLogCount = lineCount
			lastLogTime = now
		}

		if *maxEvents > 0 && lineCount >= *maxEvents {
			fmt.Printf("\n[INFO] Límite configurado de %d eventos alcanzado. Concluyendo publicación.\n", *maxEvents)
			break
		}
	}

	// Flush remaining items
	if len(batch) > 0 {
		pubTS := time.Now()
		if err := b.PublishBatch(ctx, *streamKey, batch, pubTS); err != nil {
			fmt.Fprintf(os.Stderr, "\nError publicando batch final: %v\n", err)
			os.Exit(1)
		}
		meter.Add(int64(len(batch)))
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error durante la lectura del archivo: %v\n", err)
		os.Exit(1)
	}

	// Publish EOF sentinel so consumers detect end-of-stream
	eofItem := broker.PublishItem{
		EventID: "EOF_SIGNAL",
		Payload: `{"event_id":"EOF_SIGNAL","event_type":"EOF","country":"EOF"}`,
	}
	_ = b.PublishBatch(ctx, *streamKey, []broker.PublishItem{eofItem}, time.Now())

	totalElapsed := time.Since(startTime)
	avgRate := float64(lineCount) / totalElapsed.Seconds()

	var actualSpikeDuration time.Duration
	if spikeStarted {
		if spikeCompleted {
			actualSpikeDuration = *spikeDuration
		} else {
			actualSpikeDuration = time.Since(spikeStartTime)
		}
	}

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("PUBLICACIÓN FINALIZADA CON ÉXITO:\n")
	fmt.Printf("  • Total líneas publicadas    : %12s\n", formatInt(lineCount))
	fmt.Printf("  • Eventos fase Normal Warmup : %12s\n", formatInt(warmupLines))
	fmt.Printf("  • Eventos fase Spike 10x     : %12s\n", formatInt(spikeLines))
	fmt.Printf("  • Eventos fase Cooldown      : %12s\n", formatInt(cooldownLines))
	fmt.Printf("  • Duración total             : %12s\n", totalElapsed.Round(time.Second))
	fmt.Printf("  • Duración real del Spike    : %12s (Requerido: >= 2m)\n", actualSpikeDuration.Round(time.Second))
	fmt.Printf("  • Throughput promedio total  : %12.0f eventos/s\n", avgRate)
	fmt.Println("================================================================================")
}

func extractEventIDFast(line string) string {
	idx := strings.Index(line, `"event_id"`)
	if idx == -1 {
		return ""
	}
	sub := line[idx+10:]
	colon := strings.Index(sub, `:`)
	if colon == -1 {
		return ""
	}
	sub = sub[colon+1:]
	firstQuote := strings.Index(sub, `"`)
	if firstQuote == -1 {
		return ""
	}
	sub = sub[firstQuote+1:]
	secondQuote := strings.Index(sub, `"`)
	if secondQuote == -1 {
		return ""
	}
	return sub[:secondQuote]
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
