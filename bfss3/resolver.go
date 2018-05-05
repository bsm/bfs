package bfss3

import (
	"path"

	"github.com/bsm/bfs"
)

// Resolver returns a bfs.Resolver for Amazon S3 URL handler registration.
//
// Example:
//
//   import (
//     "github.com/bsf/bfs"
//     "github.com/bsf/bfs/bfss3"
//   )
//
//   func init() {
//     bfs.Register("s3", bfss3.Resolver(ctx, &bfss3.Config{
//       // TODO
//     }))
//   }
//
//   func main() {
//     bkt, path, err := bfs.Resolve("s3://...")
//     checkErr(err)
//     defer bkt.Close()
//
//     process(bkt, path)
//   }
//
func Resolver(cfg *Config) bfs.Resolver {
	if cfg == nil {
		cfg = new(Config)
	}

	return func(_, bucket, prefix, rel string) (bfs.Bucket, string, error) {
		// merge resolve-specific params into given top-level config:
		c := *cfg
		c.Prefix = path.Join(c.Prefix, prefix)

		bkt, err := New(bucket, &c)
		if err != nil {
			return nil, "", err
		}
		return bkt, rel, nil
	}
}
