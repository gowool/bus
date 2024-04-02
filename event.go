package bus

import (
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

var _ Event = eventData{}

type Event interface {
	ID() uuid.UUID
	Date() time.Time
	Name() string
	Data() []byte
}

type eventData struct {
	id   uuid.UUID
	date time.Time
	name string
	data []byte
}

func newEventData(e eventMessage) eventData {
	return eventData{
		id:   e.ID,
		date: e.Date,
		name: e.Name,
		data: e.Data,
	}
}

func (e eventData) ID() uuid.UUID {
	return e.id
}

func (e eventData) Date() time.Time {
	return e.date
}

func (e eventData) Name() string {
	return e.name
}

func (e eventData) Data() []byte {
	return e.data
}

type eventMessage struct {
	ID   uuid.UUID       `json:"id,omitempty"`
	Date time.Time       `json:"date,omitempty"`
	Name string          `json:"name,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

func newEventMessage(event Event) eventMessage {
	return eventMessage{
		ID:   event.ID(),
		Date: event.Date(),
		Name: event.Name(),
		Data: event.Data(),
	}
}

func (e eventMessage) validate() error {
	if e.ID == uuid.Nil {
		return ErrMissingID
	}
	if e.Date.IsZero() {
		return ErrMissingDate
	}
	if e.Name == "" {
		return ErrMissingName
	}
	return nil
}
