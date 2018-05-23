// Package bfsurl provides utilities for parsing bucket file URLs.
package bfsurl

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

// ParseURL parses a file URL, splitting it into scheme/bucket/prefix/relative-path components.
// It completely ignores search ("?...") and hash ("#...").
func ParseURL(s string) (scheme, bucket, prefix, rel string, err error) {
	u, err := url.Parse(s)
	if err != nil {
		err = fmt.Errorf("bfsurl: failed to parse URL %q: %s", s, err)
		return
	}

	scheme = u.Scheme
	bucket = u.Host
	prefix, rel = parsePath(u.Path)
	return
}

// parsePath parses a path, splitting it into prefix and relative path.
// It handles glob patterns (non-globbed part goes into prefix).
// It expects normalized path (with no consecutive multiple slashes).
func parsePath(s string) (prefix, rel string) {
	if starIdx := strings.IndexByte(s, '*'); starIdx != -1 {
		if slashIdx := strings.LastIndexByte(s[:starIdx], '/'); slashIdx != -1 {
			// prefix/*/suffix or prefix/*suffix:
			prefix = s[:slashIdx+1]
			rel = s[slashIdx+1:]
			return
		}
		// */suffix or prefix*/suffix:
		rel = strings.TrimPrefix(s, "/")
		return
	}
	// prefix/suffix:
	return path.Split(s)
}
