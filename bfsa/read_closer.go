package bfsa

import "io"

type readCloser struct {
	io.ReadCloser
	closer func(error) error
}

func (c *readCloser) Close() error {
	return c.closer(c.ReadCloser.Close())
}
