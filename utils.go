package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"runtime"
	"strings"
	"time"
	"unsafe"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const instrumName = "github.com/gowool/bus"

// https://github.com/redis/go-redis/issues/2276
// remove this wrapper after fix issue 2276
type clientWrapper struct {
	redis.UniversalClient
}

func (c *clientWrapper) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	ch := make(chan *redis.XStreamSliceCmd, 1)

	go func() {
		ch <- c.UniversalClient.XReadGroup(ctx, a)
	}()

	select {
	case cmd := <-ch:
		return cmd
	case <-ctx.Done():
		var cmd redis.XStreamSliceCmd
		cmd.SetErr(ctx.Err())
		return &cmd
	}
}

type addArgs struct {
	stream     string
	noMkStream bool
	maxLen     int64 // MAXLEN N
	minID      string
	// Approx causes MaxLen and MinID to use "~" matcher (instead of "=").
	approx bool
	limit  int64
	id     string
	values map[string]interface{}
}

func (a *addArgs) clone() *addArgs {
	args := new(addArgs)
	*args = *a
	args.values = maps.Clone(a.values)

	return args
}

func (a *addArgs) toXAddArgs() *redis.XAddArgs {
	return &redis.XAddArgs{
		Stream:     a.stream,
		NoMkStream: a.noMkStream,
		MaxLen:     a.maxLen,
		MinID:      a.minID,
		Approx:     a.approx,
		Limit:      a.limit,
		ID:         a.id,
		Values:     a.values,
	}
}

func handlerName(h interface{}) string {
	typ := reflect.TypeOf(h)
	if typ.Kind() == reflect.Func {
		return strings.ReplaceAll(runtime.FuncForPC(reflect.ValueOf(h).Pointer()).Name(), ".", ":")
	}
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return strings.ReplaceAll(typ.String(), ".", ":")
}

func toEvent(message redis.XMessage) (*Event, map[string]interface{}, error) {
	var event Event

	if value, ok := message.Values[dataKey]; ok {
		if data, ok := value.(string); ok {
			if err := json.Unmarshal(unsafe.Slice(unsafe.StringData(data), len(data)), &event); err != nil {
				return nil, nil, fmt.Errorf("event data is of incorrect type %T: %w", message.Values[dataKey], err)
			}
		}
	}

	additional := maps.Clone(message.Values)
	delete(additional, dataKey)

	if err := event.Validate(); err != nil {
		return &event, additional, fmt.Errorf("event data is not valid: %w", err)
	}

	return &event, additional, nil
}

func milliseconds(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func statusAttr(err error) attribute.KeyValue {
	if err != nil {
		return attribute.String("status", "error")
	}
	return attribute.String("status", "ok")
}

func recordError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
