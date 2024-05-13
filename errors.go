package ubatch

import "errors"

var (
	ErrNoProcessor = errors.New("no processor")
	ErrSize        = errors.New("size should be greater than zero")
	ErrTimeout     = errors.New("timeout should be greater than zero")
)
