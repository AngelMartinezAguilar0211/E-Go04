package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// EventType defines the business action represented by the event.
type EventType string

const (
	EventTypeOrderPlaced       EventType = "order_placed"
	EventTypePaymentConfirmed  EventType = "payment_confirmed"
	EventTypeInventoryReserved EventType = "inventory_reserved"
)

// Event represents a single Hot Sale domain event emitted into the stream.
type Event struct {
	EventID   string     `json:"event_id"`
	EventType EventType  `json:"event_type"`
	OrderID   string     `json:"order_id"`
	Country   string     `json:"country"`
	AmountUSD *float64   `json:"amount_usd,omitempty"`
	Timestamp time.Time  `json:"timestamp"`
}

// rawEvent is an internal helper for deserializing raw JSON fields.
type rawEvent struct {
	EventID   string    `json:"event_id"`
	EventType EventType `json:"event_type"`
	OrderID   string    `json:"order_id"`
	Country   string    `json:"country"`
	AmountUSD *float64  `json:"amount_usd,omitempty"`
	Timestamp string    `json:"timestamp"`
}

// UnmarshalJSON parses an event payload, supporting both standard ISO8601 and local timestamp formats.
func (e *Event) UnmarshalJSON(data []byte) error {
	var raw rawEvent
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	e.EventID = raw.EventID
	e.EventType = raw.EventType
	e.OrderID = raw.OrderID
	e.Country = raw.Country
	e.AmountUSD = raw.AmountUSD

	if raw.Timestamp != "" {
		// Attempt RFC3339 parsing first; fallback to ISO8601 without timezone offset.
		t, err := time.Parse(time.RFC3339, raw.Timestamp)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05", raw.Timestamp)
			if err != nil {
				return fmt.Errorf("failed to parse event timestamp %q: %w", raw.Timestamp, err)
			}
		}
		e.Timestamp = t
	}

	return nil
}

// MarshalJSON serializes an event payload into canonical JSON format.
func (e Event) MarshalJSON() ([]byte, error) {
	raw := rawEvent{
		EventID:   e.EventID,
		EventType: e.EventType,
		OrderID:   e.OrderID,
		Country:   e.Country,
		AmountUSD: e.AmountUSD,
		Timestamp: e.Timestamp.Format("2006-01-02T15:04:05"),
	}
	return json.Marshal(raw)
}
