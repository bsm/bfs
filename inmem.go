package bfs

import (
	"bytes"
	"context"
	"io"
	"path"
	"sync"
	"time"
)

// InMem is an in-memory Bucket implementation which can be used for mocking.
type InMem struct {
	objects map[string]*inMemObject
	mu      sync.RWMutex
}

// NewInMem returns an initialised Bucket.
func NewInMem() *InMem {
	return &InMem{
		objects: make(map[string]*inMemObject),
	}
}

// Glob implements Bucket.
func (b *InMem) Glob(_ context.Context, pattern string) (Iterator, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var matches []string
	for key := range b.objects {
		if ok, err := path.Match(pattern, key); err != nil {
			return nil, err
		} else if ok {
			matches = append(matches, key)
		}
	}
	return &inMemIterator{entries: matches, pos: -1}, nil
}

// Head implements Bucket.
func (b *InMem) Head(_ context.Context, name string) (*MetaInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	obj, ok := b.objects[name]
	if !ok {
		return nil, ErrNotFound
	}

	return &MetaInfo{
		Name:    name,
		Size:    int64(len(obj.data)),
		ModTime: obj.modTime,
	}, nil
}

// Open implements Bucket.
func (b *InMem) Open(_ context.Context, name string) (io.ReadCloser, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	obj, ok := b.objects[name]
	if !ok {
		return nil, ErrNotFound
	}
	return &inMemReader{
		Reader: bytes.NewReader(obj.data),
	}, nil
}

// Create implements Bucket.
func (b *InMem) Create(_ context.Context, name string) (io.WriteCloser, error) {
	return &inMemWriter{bucket: b, name: name}, nil
}

// Remove implements Bucket.
func (b *InMem) Remove(_ context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.objects, name)
	return nil
}

// Close implements Bucket.
func (*InMem) Close() error { return nil }

func (b *InMem) store(name string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.objects[name] = &inMemObject{
		data:    data,
		modTime: time.Now(),
	}
}

// --------------------------------------------------------

type inMemObject struct {
	data    []byte
	modTime time.Time
}

type inMemReader struct{ *bytes.Reader }

func (*inMemReader) Close() error { return nil }

type inMemWriter struct {
	bytes.Buffer

	bucket *InMem
	name   string
}

func (w *inMemWriter) Close() error {
	w.bucket.store(w.name, w.Bytes())
	return nil
}

type inMemIterator struct {
	entries []string
	pos     int
}

func (i *inMemIterator) Next() bool {
	i.pos++
	return i.pos < len(i.entries)
}

func (i *inMemIterator) Name() string {
	if i.pos < len(i.entries) {
		return i.entries[i.pos]
	}
	return ""
}
func (*inMemIterator) Error() error { return nil }

func (i *inMemIterator) Close() error {
	i.pos = len(i.entries)
	return nil
}
