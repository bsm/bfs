package bfsgs

import (
	"context"
	"path"

	"github.com/bsm/bfs"
)

// Resolver returns a bfs.Resolver for Google Cloud Storage URL handler registration.
//
// Example:
//
//   import (
//     "github.com/bsf/bfs"
//     "github.com/bsf/bfs/bfsgs"
//   )
//
//   func init() {
//     ctx := context.Background() // not many other options are available in init
//     bfs.Register("gs", bfsgs.Resolver(ctx, &bfsgs.Config{
//       // TODO
//     }))
//   }
//
//   func main() {
//     bkt, path, err := bfs.Resolve("gs://...")
//     checkErr(err)
//     defer bkt.Close()
//
//     process(bkt, path)
//   }
//
func Resolver(ctx context.Context, cfg *Config) bfs.Resolver {
	if cfg == nil {
		cfg = new(Config)
	}

	return func(_, bucket, prefix, rel string) (bfs.Bucket, string, error) {
		// merge resolve-specific params into given top-level config:
		c := *cfg
		c.Prefix = path.Join(c.Prefix, prefix)

		bkt, err := New(ctx, bucket, &c)
		if err != nil {
			return nil, "", err
		}
		return bkt, rel, nil
	}
}
