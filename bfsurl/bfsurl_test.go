package bfsurl_test

import (
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	. "github.com/bsm/bfs/bfsurl"
)

var _ = DescribeTable("ParseURL",
	func(url, xScheme, xBucket, xPrefix, xRel string) {
		scheme, bucket, prefix, rel, err := ParseURL(url)
		Expect(err).NotTo(HaveOccurred())
		Expect(scheme).To(Equal(xScheme))
		Expect(bucket).To(Equal(xBucket))
		Expect(prefix).To(Equal(xPrefix))
		Expect(rel).To(Equal(xRel))
	},

	Entry("scheme/bucket", "file://bkt",
		"file", "bkt", "", ""),

	Entry("scheme/bucket/prefix/rel", "file://bkt/prefix/rel",
		"file", "bkt", "/prefix/", "rel"),

	Entry("scheme/bucket/pre/fix/rel", "file://bkt/pre/fix/rel",
		"file", "bkt", "/pre/fix/", "rel"),

	Entry("with trailing slash", "file://bkt/pre/fix/rel/",
		"file", "bkt", "/pre/fix/rel/", ""),

	// glob patterns:

	Entry("all-glob path", "file://bkt/*",
		"file", "bkt", "/", "*"),

	Entry("glob with prefix", "file://bkt/prefix*",
		"file", "bkt", "/", "prefix*"),

	Entry("glob with prefix with trailing slash", "file://bkt/prefix/*",
		"file", "bkt", "/prefix/", "*"),

	Entry("glob with prefix with multiple slashes", "file://bkt/pre/fix*",
		"file", "bkt", "/pre/", "fix*"),

	Entry("glob with prefix with multiple (+trailing) slashes", "file://bkt/pre/fix/*",
		"file", "bkt", "/pre/fix/", "*"),

	Entry("glob with suffix", "file://bkt/*suffix",
		"file", "bkt", "/", "*suffix"),

	Entry("glob with suffix with leading slash", "file://bkt/*/suffix",
		"file", "bkt", "/", "*/suffix"),

	Entry("glob with suffix with inner slashes", "file://bkt/*suf/fix",
		"file", "bkt", "/", "*suf/fix"),

	Entry("glob with suffix with multilple (+leading) slashes", "file://bkt/*/suf/fix",
		"file", "bkt", "/", "*/suf/fix"),

	Entry("glob with prefix and suffix", "file://bkt/prefix*suffix",
		"file", "bkt", "/", "prefix*suffix"),

	Entry("glob with prefix and suffix with slashes", "file://bkt/pre/fix/*/suf/fix",
		"file", "bkt", "/pre/fix/", "*/suf/fix"),
)
