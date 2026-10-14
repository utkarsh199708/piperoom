#!/usr/bin/env bash
# Fails when govulncheck reports a vulnerability that the code calls and that
# is not in .github/govulncheck-allow.txt, or when an allowlist entry's
# review-by date has passed. Usage: govulncheck-gate.sh <govulncheck-json> <allowlist>
set -euo pipefail

json="$1"
allow="$2"
today="$(date -u +%F)"

# `govulncheck -format json` exits 0 whatever it finds, so the gate reads the
# findings itself. A finding whose first trace frame has a function is one the
# code can reach (the ones the text mode fails on); module-level and
# package-level findings are not called and are ignored here, as in text mode.
jq -r 'select(.finding != null) | select(.finding.trace[0].function != null) | .finding.osv' "$json" | sort -u >found.txt

# Allowlist line: ID | reason | owner | review-by (YYYY-MM-DD). '#' starts a comment.
status=0
: >allowed.txt
while IFS= read -r line; do
	line="${line%%#*}"
	[[ -z "${line//[[:space:]]/}" ]] && continue
	IFS='|' read -r id reason owner review <<<"$line"
	id="$(echo "$id" | xargs)"
	review="$(echo "${review:-}" | xargs)"
	owner="$(echo "${owner:-}" | xargs)"
	reason="$(echo "${reason:-}" | xargs)"
	if [[ ! "$id" =~ ^GO-[0-9]{4}-[0-9]+$ || -z "$reason" || -z "$owner" || ! "$review" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
		echo "::error::bad allowlist line (want 'GO-YYYY-N | reason | owner | YYYY-MM-DD'): $line"
		status=1
		continue
	fi
	if [[ "$review" < "$today" ]]; then
		echo "::error::allowlist entry $id passed its review-by date $review (owner $owner): fix it or re-review it"
		status=1
	fi
	echo "$id" >>allowed.txt
done <"$allow"
sort -u -o allowed.txt allowed.txt

new="$(comm -23 found.txt allowed.txt)"
if [[ -n "$new" ]]; then
	while IFS= read -r id; do
		echo "::error::govulncheck: $id is called by this code and is not in $allow (https://pkg.go.dev/vuln/$id)"
	done <<<"$new"
	status=1
fi
fixed="$(comm -13 found.txt allowed.txt)"
if [[ -n "$fixed" ]]; then
	# Not an error: a stale entry is housekeeping, and the DB can change under us.
	echo "note: allowlisted but no longer reported (remove from the allowlist):" $fixed
fi
echo "reachable: $(wc -l <found.txt), allowlisted: $(wc -l <allowed.txt)"
exit $status
