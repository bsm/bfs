package internal

import (
	"path"
	"path/filepath"
	"strings"
)

// WithinNamespace generates a full path scoped within a namespace.
func WithinNamespace(ns, name string) string {
	return path.Join(ns, path.Clean("/"+name))
}

// NormObjectName normalises object names
func NormObjectName(name string) string {
	return strings.Trim(filepath.ToSlash(name), "/")
}
