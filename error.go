package bus

import (
	"context"
	"errors"
)

var (
	ErrMissingID           = errors.New("missing id")
	ErrMissingDate         = errors.New("missing date")
	ErrMissingName         = errors.New("missing name")
	ErrMissingHandler      = errors.New("missing handler")
	ErrHandlerAlreadyAdded = errors.New("handler already added")
)

type Error struct {
	ID       string `json:"id,omitempty"`
	Name     string `json:"name,omitempty"`
	Group    string `json:"group,omitempty"`
	Consumer string `json:"consumer,omitempty"`
	Err      error  `json:"error,omitempty"`
	Event    *Event `json:"event,omitempty"`
}

func NewError(name, group, consumer string) *Error {
	return &Error{
		Name:     name,
		Group:    group,
		Consumer: consumer,
	}
}

func (e *Error) SetID(id string) *Error {
	e.ID = id
	return e
}

func (e *Error) SetErr(err error) *Error {
	e.Err = err
	return e
}

func (e *Error) SetEvent(event Event) *Error {
	e.Event = &event
	return e
}

func (e *Error) Error() string {
	str := "bus: "

	if e.Err != nil {
		str += e.Err.Error()
	} else {
		str += "unknown error"
	}

	if e.ID != "" {
		str += "; ID: " + e.ID
	}

	str += "; Name: " + e.Name + "; Group: " + e.Group + "; Consumer: " + e.Consumer

	if e.Event != nil {
		str += "; Event: [" + e.Event.ID.String() + "][" + e.Event.Date.String() + "]"
	}

	return str
}

func (e *Error) Unwrap() error {
	return e.Err
}

func (e *Error) Is(target error) bool {
	_, ok := target.(*Error)
	return ok
}

func HandleError(ctx context.Context, errCh <-chan error, handler ErrorHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-errCh:
			if !ok {
				return
			}

			handler(ctx, err)
		}
	}
}
