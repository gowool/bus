package bus

import (
	"maps"
)

type (
	Option        func(b *RedisBus)
	PublishOption func(a *addArgs)
)

func WithClientID(clientID string) Option {
	return func(b *RedisBus) {
		if clientID != "" {
			b.clientID = clientID
		}
	}
}

func WithLogError(logError func(err error)) Option {
	return func(b *RedisBus) {
		if logError != nil {
			b.logError = logError
		}
	}
}

func WithPublishOption(options ...PublishOption) Option {
	return func(b *RedisBus) {
		for _, option := range options {
			option(b.addArgs)
		}
	}
}

func WithApproxMaxLen(maxLen int64) PublishOption {
	return func(a *addArgs) {
		WithMaxLen(maxLen)(a)
		WithApprox(maxLen > 0)(a)
	}
}

func WithMaxLen(maxLen int64) PublishOption {
	return func(a *addArgs) {
		a.maxLen = maxLen
	}
}

func WithMinID(minID string) PublishOption {
	return func(a *addArgs) {
		a.minID = minID
	}
}

func WithApprox(approx bool) PublishOption {
	return func(a *addArgs) {
		a.approx = approx
	}
}

func WithLimit(limit int64) PublishOption {
	return func(a *addArgs) {
		a.limit = limit
	}
}

func WithID(id string) PublishOption {
	return func(a *addArgs) {
		a.id = id
	}
}

func WithValues(values map[string]interface{}) PublishOption {
	return func(a *addArgs) {
		if values == nil {
			return
		}

		maps.Copy(a.values, values)

		delete(a.values, dataKey)
	}
}
