package bus

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID   uuid.UUID       `json:"id,omitempty"`
	Date time.Time       `json:"date,omitempty"`
	Name string          `json:"name,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

func (e *Event) Validate() error {
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

func (e *Event) SetData(data interface{}) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}

	e.Data = raw

	return nil
}

func (e *Event) GetData(data interface{}) error {
	return json.Unmarshal(e.Data, data)
}

func NewRawEvent(name string, data json.RawMessage) Event {
	return Event{
		ID:   uuid.New(),
		Date: time.Now(),
		Name: name,
		Data: data,
	}
}

func NewEvent(name string, data interface{}) (Event, error) {
	e := NewRawEvent(name, nil)
	err := e.SetData(data)

	return e, err
}
