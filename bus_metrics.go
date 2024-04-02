package bus

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var _ Bus = (*MetricsBus)(nil)

type MetricsBus struct {
	Bus
	publishTime   metric.Float64Histogram
	subscribeTime metric.Float64Histogram
}

func NewMetricsBus(inner Bus) (*MetricsBus, error) {
	meter := otel.GetMeterProvider().Meter(instrumName)

	publishTime, err := meter.Float64Histogram(
		"bus.publish_time",
		metric.WithDescription("The time it took to publish an event."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	subscribeTime, err := meter.Float64Histogram(
		"bus.subscribe_time",
		metric.WithDescription("The time it took to subscribe a handler."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	handleTime, err := meter.Float64Histogram(
		"bus.handle_time",
		metric.WithDescription("The time it took to execute a handler."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	inner.Middleware(metricsMiddleware(handleTime))

	return &MetricsBus{
		Bus:           inner,
		publishTime:   publishTime,
		subscribeTime: subscribeTime,
	}, nil
}

func (b *MetricsBus) Publish(ctx context.Context, event Event) error {
	start := time.Now()

	err := b.Bus.Publish(ctx, event)

	dur := time.Since(start)

	attrs := make([]attribute.KeyValue, 0, 3)
	attrs = append(attrs, attribute.String("event_id", event.ID().String()))
	attrs = append(attrs, attribute.String("event_name", event.Name()))
	attrs = append(attrs, statusAttr(err))

	b.publishTime.Record(ctx, milliseconds(dur), metric.WithAttributes(attrs...))

	return err
}

func (b *MetricsBus) Subscribe(ctx context.Context, name string, handler Handler) error {
	start := time.Now()

	hn := handlerName(handler)

	err := b.Bus.Subscribe(ctx, name, handler)

	dur := time.Since(start)

	attrs := make([]attribute.KeyValue, 0, 3)
	attrs = append(attrs, attribute.String("event_name", name))
	attrs = append(attrs, attribute.String("handler", hn))
	attrs = append(attrs, statusAttr(err))

	b.subscribeTime.Record(ctx, milliseconds(dur), metric.WithAttributes(attrs...))

	return err
}

func metricsMiddleware(handleTime metric.Float64Histogram) Middleware {
	return func(ctx context.Context, event Event, additional map[string]any, next Handler) error {
		start := time.Now()

		err := next.Handle(ctx, event, additional)

		dur := time.Since(start)

		attrs := make([]attribute.KeyValue, 0, 3)
		attrs = append(attrs, attribute.String("event_id", event.ID().String()))
		attrs = append(attrs, attribute.String("event_name", event.Name()))
		attrs = append(attrs, statusAttr(err))

		handleTime.Record(ctx, milliseconds(dur), metric.WithAttributes(attrs...))

		return err
	}
}
