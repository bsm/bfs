package bfsa

import "io"

type writeCloser struct {
	io.WriteCloser
	closer func(error) error
}

func (c *writeCloser) Close() error {
	return c.closer(c.WriteCloser.Close())
}
