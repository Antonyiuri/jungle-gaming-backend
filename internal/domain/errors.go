package domain

import "errors"

// Error identifies a stable domain failure without relying on its message.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func (e *Error) Is(target error) bool {
	other, ok := target.(*Error)
	return ok && e.Code == other.Code
}

func Code(err error) string {
	var failure *Error
	if errors.As(err, &failure) {
		return failure.Code
	}
	return ""
}

func fail(code, message string) error { return &Error{Code: code, Message: message} }
