package bus

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var _ Bus = (*TracingBus)(nil)

type TracingBus struct {
	Bus
	tracer trace.Tracer
}

func NewTracingBus(inner Bus) *TracingBus {
	tracer := otel.GetTracerProvider().Tracer(instrumName)

	inner.Middleware(tracingMiddleware(tracer))

	return &TracingBus{
		Bus:    inner,
		tracer: tracer,
	}
}

func (b *TracingBus) Publish(ctx context.Context, event Event) error {
	ctx, span := b.tracer.Start(ctx, fmt.Sprintf("publish.Event(%s)", event.Name()),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("event_id", event.ID().String()),
			attribute.String("event_name", event.Name()),
		),
	)
	defer span.End()

	err := b.Bus.Publish(ctx, event)

	recordError(span, err)

	return err
}

func (b *TracingBus) Subscribe(ctx context.Context, name string, handler Handler) error {
	hn := handlerName(handler)

	ctx, span := b.tracer.Start(ctx, fmt.Sprintf("subscribe.Handler(%s)", hn),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("event_name", name),
			attribute.String("handler", hn),
		),
	)
	defer span.End()

	err := b.Bus.Subscribe(ctx, name, handler)

	recordError(span, err)

	return err
}

func tracingMiddleware(tracer trace.Tracer) Middleware {
	return func(ctx context.Context, event Event, additional map[string]any, next Handler) error {
		ctx, span := tracer.Start(ctx, fmt.Sprintf("handle.Event(%s)", event.Name()),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("event_id", event.ID().String()),
				attribute.String("event_name", event.Name()),
			),
		)
		defer span.End()

		err := next.Handle(ctx, event, additional)

		recordError(span, err)

		return err
	}
}
