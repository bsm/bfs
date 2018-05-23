package bfsfs

import (
	"path/filepath"

	"github.com/bsm/bfs"
)

// Resolver returns a bfs.Resolver for file system URL handler registration.
//
// Example:
//
//   import (
//     "io/ioutil"
//
//     "github.com/bsf/bfs"
//     "github.com/bsf/bfs/bfsfs"
//   )
//
//   func init() {
//     tempDir, err := ioutil.TempDir("", "my-app")
//     checkErr(err)
//
//     bfs.Register("file", bfsfs.Resolver("root/dir", tempDir))
//   }
//
//   func main() {
//     bkt, path, err := bfs.Resolve("file://...")
//     checkErr(err)
//     defer bkt.Close()
//
//     process(bkt, path)
//   }
//
func Resolver(root, tmpDir string) bfs.Resolver {
	if root == "" {
		root = "."
	}

	return func(_, bucket, prefix, rel string) (bfs.Bucket, string, error) {
		// normalized bucket root ("chroot"-ed, cannot go above the top-level root):
		bktRoot := filepath.Join(root, filepath.Join("/", bucket, prefix))

		bkt, err := New(bktRoot, tmpDir)
		if err != nil {
			return nil, "", err
		}
		return bkt, rel, nil
	}
}
