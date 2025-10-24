package bfs

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/bmatcuk/doublestar/v4"
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

	var matches []*inMemObject
	for key := range b.objects {
		if ok, err := doublestar.Match(pattern, key); err != nil {
			return nil, err
		} else if ok {
			matches = append(matches, b.objects[key])
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

	return &obj.info, nil
}

// Open implements Bucket.
func (b *InMem) Open(_ context.Context, name string) (Reader, error) {
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
func (b *InMem) Create(ctx context.Context, name string, opts *WriteOptions) (Writer, error) {
	ctx, cancel := context.WithCancel(ctx)
	return &inMemWriter{
		ctx:    ctx,
		cancel: cancel,
		bucket: b,
		name:   name,
		opts:   opts,
	}, nil
}

// Remove implements Bucket.
func (b *InMem) Remove(_ context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.objects, name)
	return nil
}

// RemoveAll implements Bucket extension.
func (b *InMem) RemoveAll(_ context.Context, pattern string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for key := range b.objects {
		if ok, err := doublestar.Match(pattern, key); err != nil {
			return err
		} else if ok {
			delete(b.objects, key)
		}
	}
	return nil
}

// ObjectSizes return a map of object sizes by name
func (b *InMem) ObjectSizes() map[string]int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sizes := make(map[string]int64, len(b.objects))
	for key, obj := range b.objects {
		sizes[key] = obj.Size()
	}
	return sizes
}

// Close implements Bucket.
func (*InMem) Close() error { return nil }

func (b *InMem) store(name string, data []byte, opts *WriteOptions) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.objects[name] = &inMemObject{
		data: data,
		info: MetaInfo{
			Name:        name,
			Size:        int64(len(data)),
			ModTime:     time.Now(),
			ContentType: opts.GetContentType(),
			Metadata:    opts.GetMetadata(),
		},
	}
}

// --------------------------------------------------------

type inMemObject struct {
	data []byte
	info MetaInfo
}

func (o *inMemObject) Size() int64 {
	return int64(len(o.data))
}

type inMemReader struct{ *bytes.Reader }

func (*inMemReader) Close() error { return nil }

type inMemWriter struct {
	bytes.Buffer

	ctx    context.Context
	cancel context.CancelFunc
	bucket *InMem
	name   string
	opts   *WriteOptions
}

func (w *inMemWriter) Discard() error {
	err := w.ctx.Err()
	w.cancel()
	return err
}

func (w *inMemWriter) Commit() error {
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
	}

	w.bucket.store(w.name, w.Bytes(), w.opts)
	return w.Discard()
}

type inMemIterator struct {
	entries []*inMemObject
	pos     int
}

func (i *inMemIterator) Next() bool {
	i.pos++
	return i.pos < len(i.entries)
}

func (i *inMemIterator) Name() string {
	if i.pos < len(i.entries) {
		return i.entries[i.pos].info.Name
	}
	return ""
}

func (i *inMemIterator) Size() int64 {
	if i.pos < len(i.entries) {
		return i.entries[i.pos].info.Size
	}
	return 0
}

func (i *inMemIterator) ModTime() time.Time {
	if i.pos < len(i.entries) {
		return i.entries[i.pos].info.ModTime
	}
	return time.Time{}
}

func (*inMemIterator) Error() error { return nil }

func (i *inMemIterator) Close() error {
	i.pos = len(i.entries)
	return nil
}
