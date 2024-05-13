package ubatch

import "time"

type options struct {
	size    int
	timeout time.Duration
}

type Option interface {
	apply(opts *options)
}

// WithSize config size of batch
func WithSize(size int) Option {
	return sizeOption{size: size}
}

type sizeOption struct {
	size int
}

func (o sizeOption) apply(opts *options) {
	opts.size = o.size
}

// WithTimeout config timeout.
func WithTimeout(timeout time.Duration) Option {
	return timeoutOption{timeout: timeout}
}

type timeoutOption struct {
	timeout time.Duration
}

func (o timeoutOption) apply(opts *options) {
	opts.timeout = o.timeout
}
