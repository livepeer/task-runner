package task

import (
	"errors"
	"fmt"

	api "github.com/livepeer/go-api-client"
)

var assetNotFound = errors.New("asset not found")

type UnretriableError struct{ error }

func (e UnretriableError) Error() string { return e.error.Error() }

func (e UnretriableError) Unwrap() error { return e.error }

func IsUnretriable(err error) bool {
	return errors.Is(err, api.ErrNotExists) ||
		errors.As(err, &UnretriableError{})
}

type CatalystError struct {
	msg string
}

func NewCatalystError(msg string, unretriable bool) error {
	var err error = CatalystError{msg}
	if unretriable {
		err = UnretriableError{err}
	}
	return err
}

func (e CatalystError) Error() string {
	return fmt.Sprintf("catalyst error: %s", e.msg)
}
