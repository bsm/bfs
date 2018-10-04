package internal

import (
	"path"
)

// WithinNamespace generates a full path scoped within a namespace.
func WithinNamespace(ns, name string) string {
	return path.Join(ns, path.Clean("/"+name))
}
