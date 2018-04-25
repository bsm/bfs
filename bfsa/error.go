package bfsa

import "fmt"

type unsupportedSchemeError struct {
	Scheme string
}

func (e unsupportedSchemeError) Error() string {
	return fmt.Sprintf("bfsa: unsupported scheme %q", e.Scheme)
}
