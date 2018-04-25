package bfsa

import "github.com/bsm/bfs"

type iterator struct {
	bfs.Iterator
	toURI  func(string) string
	closer func(error) error
}

func (c *iterator) Name() string {
	name := c.Iterator.Name()
	return c.toURI(name)
}

func (c *iterator) Close() error {
	return c.closer(c.Iterator.Close())
}
