package bfsos

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

// atomicFile represents a file, that's written only on Close.
type atomicFile struct {
	*os.File
	name string
}

// openAtomicFile opens atomic file for writing.
// tmpDir defaults to standard temporary dir if blank.
func openAtomicFile(name string, tmpDir string) (*atomicFile, error) {
	f, err := ioutil.TempFile(tmpDir, "bfsos")
	if err != nil {
		return nil, err
	}
	return &atomicFile{
		File: f,
		name: name,
	}, nil
}

// Close commits the file.
func (f *atomicFile) Close() error {
	defer f.cleanup()

	if err := f.File.Close(); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(f.name), 0755); err != nil {
		return err
	}

	return os.Rename(f.Name(), f.name)
}

// cleanup removes temporary file.
func (f *atomicFile) cleanup() {
	_ = os.Remove(f.Name())
}
