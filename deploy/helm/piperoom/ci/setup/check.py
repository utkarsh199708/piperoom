#!/usr/bin/env python3
"""Checks the two setup releases of the chart (docs/design/setup-strimzi.md §4).

Usage: helm template ... | check.py pipeline|runner

pipeline: every Strimzi CR is a paused shell, nothing else is a Strimzi kind,
and the server's Deployment is absent.
runner: the Role grants no create, no escalate/bind/impersonate, no "*", no
list but on events, and every rule but the events ones names its objects.
The golden files pin the full render; these checks say why it must look so.
"""
import sys

import yaml

PAUSE = "strimzi.io/pause-reconciliation"


def fail(msg):
    print("check.py: " + msg, file=sys.stderr)
    sys.exit(1)


def main():
    mode = sys.argv[1] if len(sys.argv) == 2 else ""
    docs = [d for d in yaml.safe_load_all(sys.stdin) if d]
    kinds = [d["kind"] for d in docs]
    if "Deployment" in kinds or "Service" in kinds:
        fail("a setup release rendered the server")
    if mode == "pipeline":
        shells = [d for d in docs if d["apiVersion"].startswith("kafka.strimzi.io/")]
        if not shells:
            fail("no Strimzi shells rendered")
        for d in shells:
            ann = d["metadata"].get("annotations") or {}
            if ann.get(PAUSE) != "true":
                fail("%s %s is not paused" % (d["kind"], d["metadata"]["name"]))
            if "spec" not in d:
                fail("%s %s has no spec (v1 requires one)" % (d["kind"], d["metadata"]["name"]))
        secrets = [d for d in docs if d["kind"] == "Secret"]
        for s in secrets:
            if s.get("type") != "Opaque" or s.get("data") or s.get("stringData"):
                fail("Secret %s must be an empty Opaque Secret" % s["metadata"]["name"])
    elif mode == "runner":
        roles = [d for d in docs if d["kind"] in ("Role", "ClusterRole")]
        if [r["kind"] for r in roles] != ["Role"]:
            fail("the runner release must render exactly one Role and no ClusterRole")
        if any(d["kind"] in ("ClusterRoleBinding",) for d in docs):
            fail("a ClusterRoleBinding rendered")
        for rule in roles[0]["rules"]:
            verbs = set(rule["verbs"])
            for bad in ("create", "escalate", "bind", "impersonate", "*", "deletecollection", "update"):
                if bad in verbs:
                    fail("the Role grants %s on %s" % (bad, rule["resources"]))
            if "*" in rule["resources"] or "*" in rule["apiGroups"]:
                fail("the Role has a wildcard")
            if rule["resources"] == ["events"]:
                if verbs != {"list"}:
                    fail("events may only be listed")
                continue
            if "list" in verbs:
                fail("list on %s" % rule["resources"])
            if not rule.get("resourceNames"):
                fail("rule on %s names no objects" % rule["resources"])
            for r in rule["resources"]:
                if r in ("pods/exec", "pods/attach", "pods/portforward", "serviceaccounts/token"):
                    fail("the Role reaches %s" % r)
        job = [d for d in docs if d["kind"] == "Job"][0]
        pod = job["spec"]["template"]["spec"]
        c = pod["containers"][0]
        if c["securityContext"].get("readOnlyRootFilesystem") is not True or pod["securityContext"].get("runAsNonRoot") is not True:
            fail("the Job must run as non-root with a read-only root filesystem")
        for v in pod["volumes"]:
            if "hostPath" in v:
                fail("the Job mounts a hostPath")
        for e in c.get("env", []):
            if "code" in e["name"].lower():
                fail("the pairing code is never an environment variable")
    else:
        fail("usage: check.py pipeline|runner")
    print("check.py: %s ok (%d objects)" % (mode, len(docs)))


if __name__ == "__main__":
    main()
