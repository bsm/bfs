package bfsa

import (
	"context"
	"fmt"

	"github.com/bsm/bfs"
)

var reg registry

func init() {
	reg = registry{}
}

func Register(scheme string, resolver Resolver) {
	reg.Register(scheme, resolver)
}

// ----------------------------------------------------------------------------

type registry map[string]Resolver

func (r registry) Register(scheme string, resolver Resolver) {
	r[scheme] = resolver
}

func (r registry) resolve(ctx context.Context, uri string) (bfs.Bucket, string, func(string) string, error) {
	scheme := detectScheme(uri)
	if scheme == "" {
		return nil, "", nil, fmt.Errorf("no scheme in %q", uri)
	}

	resolver, ok := r[uri]
	if !ok {
		return nil, "", nil, unsupportedSchemeError{Scheme: scheme}
	}
	return resolver(ctx, uri)
}
