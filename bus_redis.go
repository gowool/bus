package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	"github.com/rs/xid"
)

var (
	_ Bus        = (*RedisBus)(nil)
	_ Publisher  = (*RedisBus)(nil)
	_ Subscriber = (*RedisBus)(nil)
)

const dataKey = "data"

type RedisBus struct {
	clientID   string
	client     redis.UniversalClient
	addArgs    *addArgs
	logError   func(err error)
	mdws       []Middleware
	registered map[string]struct{}
	errCh      chan error
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	wg         sync.WaitGroup
}

func NewRedisBus(client redis.UniversalClient, options ...Option) *RedisBus {
	ctx, cancel := context.WithCancel(context.Background())

	b := &RedisBus{
		clientID:   xid.New().String(),
		client:     &clientWrapper{UniversalClient: client},
		addArgs:    &addArgs{values: map[string]any{}},
		logError:   func(error) {},
		registered: map[string]struct{}{},
		errCh:      make(chan error, 100),
		ctx:        ctx,
		cancel:     cancel,
	}

	for _, opt := range options {
		opt(b)
	}

	return b
}

func (b *RedisBus) Stop(ctx context.Context) error {
	b.cancel()

	done := make(chan struct{})
	defer close(done)

	go func() {
		b.wg.Wait()
		close(b.errCh)
		done <- struct{}{}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-done:
			return nil
		}
	}
}

func (b *RedisBus) Errors() <-chan error {
	return b.errCh
}

func (b *RedisBus) Publish(ctx context.Context, event Event) error {
	message := newEventMessage(event)
	if err := message.validate(); err != nil {
		return err
	}

	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	args := b.addArgs.clone()
	args.stream = message.Name
	args.values[dataKey] = data

	if err = b.client.XAdd(ctx, args.toXAddArgs()).Err(); err != nil {
		return fmt.Errorf("could not publish event: %w", err)
	}

	return nil
}

func (b *RedisBus) Middleware(middlewares ...Middleware) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.mdws = append(b.mdws, middlewares...)
}

func (b *RedisBus) Subscribe(ctx context.Context, name string, handler Handler) error {
	if name == "" {
		return ErrMissingName
	}

	if handler == nil {
		return ErrMissingHandler
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	group := fmt.Sprintf("%s:%s", name, handlerName(handler))

	if _, ok := b.registered[group]; ok {
		return ErrHandlerAlreadyAdded
	}

	res, err := b.client.XGroupCreateMkStream(ctx, name, group, "$").Result()
	if err != nil {
		if !strings.HasPrefix(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("could not create consumer group: %w", err)
		}
	} else if res != "OK" {
		return fmt.Errorf("could not create consumer group: %s", res)
	}

	b.registered[group] = struct{}{}
	b.wg.Add(1)

	var handlerFunc HandlerFunc = func(ctx context.Context, event Event, additional map[string]any) error {
		return handler.Handle(ctx, event, additional)
	}

	for i := len(b.mdws) - 1; i >= 0; i-- {
		next := handlerFunc
		pipe := b.mdws[i]

		handlerFunc = func(ctx context.Context, event Event, additional map[string]any) error {
			return pipe(ctx, event, additional, next)
		}
	}

	go b.handle(name, group, handlerFunc)

	return nil
}

func (b *RedisBus) handle(stream, group string, h HandlerFunc) {
	defer b.wg.Done()

	consumer := fmt.Sprintf("%s:%s", group, b.clientID)
	handler := b.handler(stream, group, consumer, h)
	id := "0-0"

	for {
		streams, err := b.client.XReadGroup(b.ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, id},
		}).Result()
		if errors.Is(err, context.Canceled) {
			return
		} else if err != nil {
			err = fmt.Errorf("could not receive: %w", err)

			b.error(NewError(stream, group, consumer).SetErr(err))

			time.Sleep(time.Second)

			continue
		}

		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			id = ">"

			continue
		}

		for _, s := range streams {
			for _, message := range s.Messages {
				if id != ">" {
					id = message.ID
				}

				handler(b.ctx, message)

				select {
				case <-b.ctx.Done():
					return
				default:
				}
			}
		}
	}
}

func (b *RedisBus) handler(name, group, consumer string, h HandlerFunc) func(ctx context.Context, message redis.XMessage) {
	return func(ctx context.Context, message redis.XMessage) {
		event, additional, err := toEvent(message)
		busErr := NewError(name, group, consumer).
			SetID(message.ID).
			SetEventID(event.ID()).
			SetEventDate(event.Date())

		if err != nil {
			b.error(busErr.SetErr(err))
			return
		}

		if err = h(ctx, event, additional); err != nil {
			if !errors.Is(err, context.Canceled) {
				err = fmt.Errorf("could not handle event (%s): %w", group, err)

				b.error(busErr.SetErr(err))
			}

			return
		}

		if err = b.client.XAck(ctx, name, group, message.ID).Err(); err != nil && !errors.Is(err, context.Canceled) {
			err = fmt.Errorf("could not ack handled event: %w", err)

			b.error(busErr.SetErr(err))
		}
	}
}

func (b *RedisBus) error(err error) {
	select {
	case b.errCh <- err:
	default:
		b.logError(err)
	}
}
