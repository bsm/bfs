package bfsos

import (
	"sync"
)

// iterator implements an iterator over file path list.
type iterator struct {
	mutex sync.Mutex
	names []string // hold relative (non-rooted) file names/paths
	index int
}

// newIterator constructs new iterator.
//
// WARNING! Iterator uses provided names slice, so it shouldn't be mutated after being passed here.
func newIterator(names []string) *iterator {
	return &iterator{
		names: names,
		index: -1,
	}
}

// Next advances the cursor to the next position.
func (it *iterator) Next() bool {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	it.index++
	return it.isValid()
}

// Name returns the name at the current cursor position.
func (it *iterator) Name() string {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	if it.isValid() {
		return it.names[it.index]
	}
	return ""
}

// Error returns the last iterator error, if any.
func (it *iterator) Error() error {
	return nil
}

// Close closes the iterator, should always be deferred.
func (it *iterator) Close() error {
	it.mutex.Lock()
	defer it.mutex.Unlock()

	it.names = nil
	return nil
}

// isValid tells if current iterator is valid (not exhausted).
func (it *iterator) isValid() bool {
	return it.index < len(it.names)
}
