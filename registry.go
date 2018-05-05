package bfs

import (
	"fmt"
	"path"

	"github.com/bsm/bfs/bfsurl"
)

var reg registry

func init() {
	reg = registry{}
}

// Resolver represents a function, that can resolve/construct bucket/path from given URL components.
// Scheme comes without "://" suffix.
type Resolver func(scheme, bucket, prefix, rel string) (bkt Bucket, path string, err error)

// Register registers a global resolver for a given scheme.
// Scheme comes without "://" suffix.
//
// It panics, if resolver for given scheme is already registered.
//
// Registration is meant to be done before any bucket operations, like in `init` functions:
//
//   func init() {
//     Register("my-scheme", myResolver)
//   }
//
func Register(scheme string, resolver Resolver) {
	reg.Register(scheme, resolver)
}

// RegisterBucket registers a bucket instance for given scheme.
// It resolves final bucket path as "bucket/prefix/rel",
// so bucket name, passed to Resolve, is used in a bucket path.
//
// Example:
//
//   import "github.com/bsm/bfs"
//
//   var mem bfs.InMem // so it can be globally accessed
//
//   func init() {
//     mem = NewInMem()
//     RegisterBucket("mem", mem)
//   }
//
//   func myFunc() {
//     bkt, path, err := bfs.Resolve("mem://...")
//     checkErr(err)
//     defer bkt.Close()
//
//     process(bkt, path)
//   }
//
func RegisterBucket(scheme string, bkt Bucket) {
	Register(scheme, func(_, bucket, prefix, rel string) (Bucket, string, error) {
		bktPath := path.Join(bucket, prefix, rel)
		return bkt, bktPath, nil
	})
}

// Resolve resolves given bucket file URL into bucket/path.
func Resolve(url string) (bkt Bucket, path string, err error) {
	scheme, bucket, prefix, rel, err := bfsurl.ParseURL(url)
	return reg.resolve(scheme, bucket, prefix, rel)
}

// ----------------------------------------------------------------------------

// registry maintains scheme -> Resolver associations.
type registry map[string]Resolver

// Register registers a resolver for a given scheme.
// Scheme comes without "://" suffix.
// Panics, if resolver for given scheme is already registered.
func (r registry) Register(scheme string, resolver Resolver) {
	if _, ok := r[scheme]; ok {
		panic(fmt.Sprintf("bfs: resolver for scheme %q is already registered", scheme))
	}
	r[scheme] = resolver
}

// resolve runs resolver, if it exists; panics otherwise.
func (r registry) resolve(scheme, bucket, prefix, rel string) (Bucket, string, error) {
	resolver, ok := r[scheme]
	if !ok {
		panic(fmt.Sprintf("bfs: resolver for scheme %q is not registered", scheme))
	}
	return resolver(scheme, bucket, prefix, rel)
}
