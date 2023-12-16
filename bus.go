package bus

import "context"

type (
	ErrorHandler func(ctx context.Context, err error)
	Middleware   func(ctx context.Context, event Event, additional map[string]interface{}, next Handler) error
	HandlerFunc  func(ctx context.Context, event Event, additional map[string]interface{}) error
)

func (h HandlerFunc) Handle(ctx context.Context, event Event, additional map[string]interface{}) error {
	return h(ctx, event, additional)
}

type Handler interface {
	Handle(ctx context.Context, event Event, additional map[string]interface{}) error
}

type Publisher interface {
	Publish(ctx context.Context, event Event) error
}

type Subscriber interface {
	Middleware(middlewares ...Middleware)
	Subscribe(ctx context.Context, name string, handler Handler) error
	Stop(ctx context.Context) error
	Errors() <-chan error
}

type Bus interface {
	Publisher
	Subscriber
}
