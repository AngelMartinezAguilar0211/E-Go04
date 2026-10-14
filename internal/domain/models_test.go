package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEvent_UnmarshalJSON_Formats(t *testing.T) {
	// ISO without timezone offset
	raw1 := []byte(`{"event_id":"e1","event_type":"order_placed","order_id":"o1","country":"MX","amount_usd":150.50,"timestamp":"2026-11-14T10:15:30"}`)
	var ev1 Event
	if err := json.Unmarshal(raw1, &ev1); err != nil {
		t.Fatalf("failed to unmarshal ISO format: %v", err)
	}
	if ev1.EventID != "e1" || ev1.Country != "MX" || *ev1.AmountUSD != 150.50 {
		t.Fatalf("unexpected fields in ev1: %+v", ev1)
	}
	expectedTime := time.Date(2026, 11, 14, 10, 15, 30, 0, time.UTC)
	if !ev1.Timestamp.Equal(expectedTime) {
		t.Fatalf("expected timestamp %v, got %v", expectedTime, ev1.Timestamp)
	}

	// Inventory reserved without amount
	raw2 := []byte(`{"event_id":"e2","event_type":"inventory_reserved","order_id":"o1","country":"MX","amount_usd":null,"timestamp":"2026-11-14T10:16:00"}`)
	var ev2 Event
	if err := json.Unmarshal(raw2, &ev2); err != nil {
		t.Fatalf("failed to unmarshal null amount: %v", err)
	}
	if ev2.AmountUSD != nil {
		t.Fatalf("expected nil amount_usd, got %v", *ev2.AmountUSD)
	}
}

func TestAggregator_Accumulation(t *testing.T) {
	agg := NewAggregator()
	amt1 := 100.0
	amt2 := 50.0

	ev1 := &Event{
		EventID:   "e1",
		EventType: EventTypeOrderPlaced,
		OrderID:   "o1",
		Country:   "MX",
		AmountUSD: &amt1,
	}
	ev2 := &Event{
		EventID:   "e2",
		EventType: EventTypePaymentConfirmed,
		OrderID:   "o1",
		Country:   "MX",
		AmountUSD: &amt1,
	}
	ev3 := &Event{
		EventID:   "e3",
		EventType: EventTypeInventoryReserved,
		OrderID:   "o1",
		Country:   "MX",
		AmountUSD: nil,
	}
	ev4 := &Event{
		EventID:   "e4",
		EventType: EventTypeOrderPlaced,
		OrderID:   "o2",
		Country:   "CO",
		AmountUSD: &amt2,
	}

	agg.RecordLine()
	agg.ProcessUniqueEvent(ev1)

	agg.RecordLine()
	agg.ProcessUniqueEvent(ev2)

	agg.RecordLine()
	agg.ProcessUniqueEvent(ev3)

	agg.RecordLine()
	agg.ProcessUniqueEvent(ev4)

	// Ingest a duplicate line
	agg.RecordLine()
	agg.RecordDuplicate()

	snap := agg.Snapshot()
	if snap.TotalLines != 5 {
		t.Fatalf("expected 5 total lines, got %d", snap.TotalLines)
	}
	if snap.TotalUnique != 4 {
		t.Fatalf("expected 4 unique events, got %d", snap.TotalUnique)
	}
	if snap.TotalDuplicates != 1 {
		t.Fatalf("expected 1 duplicate, got %d", snap.TotalDuplicates)
	}

	mx := snap.Countries["MX"]
	if mx.OrdersPlacedCount != 1 || mx.OrdersPlacedAmountUSD != 100.0 {
		t.Fatalf("unexpected MX orders: %+v", mx)
	}
	if mx.PaymentConfirmedCount != 1 || mx.PaymentConfirmedAmountUSD != 100.0 {
		t.Fatalf("unexpected MX payments: %+v", mx)
	}
	if mx.InventoryReservedCount != 1 {
		t.Fatalf("unexpected MX inventory: %+v", mx)
	}

	co := snap.Countries["CO"]
	if co.OrdersPlacedCount != 1 || co.OrdersPlacedAmountUSD != 50.0 {
		t.Fatalf("unexpected CO orders: %+v", co)
	}
}
