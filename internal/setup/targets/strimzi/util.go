package strimzi

import "regexp"

func mustRE(s string) *regexp.Regexp { return regexp.MustCompile(s) }
