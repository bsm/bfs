package bfsscp

import "github.com/bsm/bfs"

func BucketConfig(b bfs.Bucket) *Config {
	return b.(*bucket).config
}

