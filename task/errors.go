package task

import (
	"errors"

	api "github.com/livepeer/go-api-client"
)

type UnretriableError struct{ error }

func (e UnretriableError) Error() string { return e.error.Error() }

func (e UnretriableError) Unwrap() error { return e.error }

func IsUnretriable(err error) bool {
	return errors.Is(err, api.ErrNotExists) ||
		errors.As(err, &UnretriableError{})
}
