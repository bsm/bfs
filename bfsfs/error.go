package bfsfs

import (
	"os"

	"github.com/bsm/bfs"
)

// normError normalizes error.
func normError(err error) error {
	switch {
	case err == nil:
		return nil
	case os.IsNotExist(err):
		return bfs.ErrNotFound
	default:
		return err
	}
}
