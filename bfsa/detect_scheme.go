package bfsa

import "strings"

func detectScheme(uri string) string {
	res := strings.SplitAfterN(uri, "//", 2)
	if len(res) != 2 {
		return ""
	}
	return res[0]
}
