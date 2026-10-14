package domain

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// CountryAggregate holds the business metrics for a specific country.
type CountryAggregate struct {
	Country                   string  `json:"country"`
	OrdersPlacedCount         int64   `json:"orders_placed_count"`
	OrdersPlacedAmountUSD     float64 `json:"orders_placed_amount_usd"`
	PaymentConfirmedCount     int64   `json:"payment_confirmed_count"`
	PaymentConfirmedAmountUSD float64 `json:"payment_confirmed_amount_usd"`
	InventoryReservedCount    int64   `json:"inventory_reserved_count"`
}

// SummaryReport contains the comprehensive final aggregates for all countries.
type SummaryReport struct {
	Countries          map[string]*CountryAggregate `json:"countries"`
	TotalLines         int64                        `json:"total_lines"`
	TotalUnique        int64                        `json:"total_unique"`
	TotalDuplicates    int64                        `json:"total_duplicates"`
	TotalOrdersAmount  float64                      `json:"total_orders_amount_usd"`
	TotalPaymentAmount float64                      `json:"total_payment_amount_usd"`
	CountsByType       map[EventType]int64          `json:"counts_by_type"`
}

// Aggregator accumulates streaming events into business metrics in a thread-safe manner.
type Aggregator struct {
	mu              sync.Mutex
	countries       map[string]*CountryAggregate
	countsByType    map[EventType]int64
	totalLines      int64
	totalUnique     int64
	totalDuplicates int64
}

// NewAggregator creates an initialized Aggregator instance.
func NewAggregator() *Aggregator {
	return &Aggregator{
		countries: make(map[string]*CountryAggregate),
		countsByType: map[EventType]int64{
			EventTypeOrderPlaced:       0,
			EventTypePaymentConfirmed:  0,
			EventTypeInventoryReserved: 0,
		},
	}
}

// RecordLine increments the total raw lines counter.
func (a *Aggregator) RecordLine() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.totalLines++
}

// RecordDuplicate records that an incoming event was discarded as a duplicate.
func (a *Aggregator) RecordDuplicate() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.totalDuplicates++
}

// ProcessUniqueEvent updates the business aggregates with a confirmed unique event.
func (a *Aggregator) ProcessUniqueEvent(event *Event) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.totalUnique++
	a.countsByType[event.EventType]++

	country := event.Country
	if country == "" {
		country = "UNKNOWN"
	}

	ca, exists := a.countries[country]
	if !exists {
		ca = &CountryAggregate{Country: country}
		a.countries[country] = ca
	}

	switch event.EventType {
	case EventTypeOrderPlaced:
		ca.OrdersPlacedCount++
		if event.AmountUSD != nil {
			ca.OrdersPlacedAmountUSD += *event.AmountUSD
		}
	case EventTypePaymentConfirmed:
		ca.PaymentConfirmedCount++
		if event.AmountUSD != nil {
			ca.PaymentConfirmedAmountUSD += *event.AmountUSD
		}
	case EventTypeInventoryReserved:
		ca.InventoryReservedCount++
	}
}

// Snapshot returns a copy of the current aggregation state.
func (a *Aggregator) Snapshot() SummaryReport {
	a.mu.Lock()
	defer a.mu.Unlock()

	countriesCopy := make(map[string]*CountryAggregate, len(a.countries))
	var totalOrdersAmount float64
	var totalPaymentAmount float64

	for k, v := range a.countries {
		cp := *v
		countriesCopy[k] = &cp
		totalOrdersAmount += v.OrdersPlacedAmountUSD
		totalPaymentAmount += v.PaymentConfirmedAmountUSD
	}

	typesCopy := make(map[EventType]int64, len(a.countsByType))
	for k, v := range a.countsByType {
		typesCopy[k] = v
	}

	return SummaryReport{
		Countries:          countriesCopy,
		TotalLines:         a.totalLines,
		TotalUnique:        a.totalUnique,
		TotalDuplicates:    a.totalDuplicates,
		TotalOrdersAmount:  totalOrdersAmount,
		TotalPaymentAmount: totalPaymentAmount,
		CountsByType:       typesCopy,
	}
}

// PrintReport formats and displays the final aggregated results in the console.
func (a *Aggregator) PrintReport() {
	snap := a.Snapshot()

	fmt.Println("\n" + strings.Repeat("=", 88))
	fmt.Println("             REPORTE FINAL DE AGREGADOS — HOT SALE STREAMING ENGINE")
	fmt.Println(strings.Repeat("=", 88))

	fmt.Printf("%-8s | %-14s | %-18s | %-14s | %-18s | %-14s\n",
		"PAÍS", "ÓRDENES", "MONTO ÓRDENES ($)", "PAGOS CONF.", "MONTO PAGOS ($)", "INV. RESERVADO")
	fmt.Println(strings.Repeat("-", 88))

	keys := make([]string, 0, len(snap.Countries))
	for k := range snap.Countries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var (
		sumOrders    int64
		sumPayments  int64
		sumInventory int64
	)

	for _, k := range keys {
		ca := snap.Countries[k]
		sumOrders += ca.OrdersPlacedCount
		sumPayments += ca.PaymentConfirmedCount
		sumInventory += ca.InventoryReservedCount

		fmt.Printf("%-8s | %14s | %18s | %14s | %18s | %14s\n",
			ca.Country,
			formatNumber(ca.OrdersPlacedCount),
			fmt.Sprintf("$%.2f", ca.OrdersPlacedAmountUSD),
			formatNumber(ca.PaymentConfirmedCount),
			fmt.Sprintf("$%.2f", ca.PaymentConfirmedAmountUSD),
			formatNumber(ca.InventoryReservedCount),
		)
	}

	fmt.Println(strings.Repeat("-", 88))
	fmt.Printf("%-8s | %14s | %18s | %14s | %18s | %14s\n",
		"TOTAL",
		formatNumber(sumOrders),
		fmt.Sprintf("$%.2f", snap.TotalOrdersAmount),
		formatNumber(sumPayments),
		fmt.Sprintf("$%.2f", snap.TotalPaymentAmount),
		formatNumber(sumInventory),
	)
	fmt.Println(strings.Repeat("=", 88))

	fmt.Printf("Auditoría de Ingesta y Deduplicación:\n")
	fmt.Printf("  • Total de líneas recibidas en stream : %12s\n", formatNumber(snap.TotalLines))
	fmt.Printf("  • Eventos únicos agregados            : %12s\n", formatNumber(snap.TotalUnique))
	fmt.Printf("  • Duplicados detectados y descartados : %12s (%.2f%%)\n",
		formatNumber(snap.TotalDuplicates),
		float64(snap.TotalDuplicates)/float64(snap.TotalUnique)*100)
	fmt.Printf("  • Desglose por tipo de evento:\n")
	fmt.Printf("      - order_placed        : %12s\n", formatNumber(snap.CountsByType[EventTypeOrderPlaced]))
	fmt.Printf("      - payment_confirmed   : %12s\n", formatNumber(snap.CountsByType[EventTypePaymentConfirmed]))
	fmt.Printf("      - inventory_reserved  : %12s\n", formatNumber(snap.CountsByType[EventTypeInventoryReserved]))
	fmt.Println(strings.Repeat("=", 88))
}

func formatNumber(n int64) string {
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
