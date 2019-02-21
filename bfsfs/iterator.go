package bfsfs

import (
	"github.com/bsm/bfs"
)

// iterator implements an iterator over file list.
type iterator struct {
	files []file // hold relative (non-rooted) files
	index int
}

type file struct {
	name string
	meta *bfs.MetaInfo
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

// Meta returns the *bfs.MetaInfo at the current cursor position.
func (it *iterator) Meta() *bfs.MetaInfo {
	if it.isValid() {
		return it.files[it.index].meta
	}
	return nil
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
