package redact

// more runs the passes added for #510, #511 and #534 over out, the output
// of every other rule; orig is the text before any rule ran. Each pass only
// replaces text of out with masks, so nothing an earlier rule masked can
// come back.
func more(orig, out string, st *Stats) string {
	_ = orig
	return applyCounted(&addXMLAttrRule, out, "", st)
}
