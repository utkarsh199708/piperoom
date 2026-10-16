package redact

// more runs the passes added for #510, #511 and #534 over out, the output
// of every other rule; orig is the text before any rule ran. Each pass only
// replaces text of out with masks, so nothing an earlier rule masked can
// come back.
func more(orig, out string, st *Stats) string {
	out = applyCounted(&addXMLAttrRule, out, "", st)
	out = project(orig, out, origSpans(orig), st)
	// A mask can complete a shape with its neighbours ("[redacted 8]""X");
	// reading the output too keeps Text(Text(x)) == Text(x).
	for range maxPasses {
		next := project(out, out, origSpans(out), st)
		if next == out {
			break
		}
		out = next
	}
	return out
}

// origSpans finds the spans of the passes that read the original text.
func origSpans(s string) []span {
	return shellSpans(s)
}
