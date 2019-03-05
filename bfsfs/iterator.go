package bfsfs

import (
	"time"
)

// iterator implements an iterator over file list.
type iterator struct {
	files []file // hold relative (non-rooted) files
	index int
}

type file struct {
	name    string
	size    int64
	modTime time.Time
}

// newIterator constructs new iterator.
//
// WARNING! Iterator uses provided names slice, so it shouldn't be mutated after being passed here.
func newIterator(files []file) *iterator {
	return &iterator{
		files: files,
		index: -1,
	}
}

// Next advances the cursor to the next position.
func (it *iterator) Next() bool {
	it.index++
	return it.isValid()
}

// Name returns the name at the current cursor position.
func (it *iterator) Name() string {
	if it.isValid() {
		return it.files[it.index].name
	}
	return ""
}

// Size returns the content length in bytes at the current cursor position.
func (it *iterator) Size() int64 {
	if it.isValid() {
		return it.files[it.index].size
	}
	return 0
}

// ModTime returns the modification time at the current cursor position.
func (it *iterator) ModTime() time.Time {
	if it.isValid() {
		return it.files[it.index].modTime
	}
	return time.Time{}
}

// Error returns the last iterator error, if any.
func (it *iterator) Error() error {
	return nil
}

// Close closes the iterator, should always be deferred.
func (it *iterator) Close() error {
	it.files = nil
	return nil
}

// isValid tells if current iterator is valid (not exhausted).
func (it *iterator) isValid() bool {
	return it.index < len(it.files)
}
