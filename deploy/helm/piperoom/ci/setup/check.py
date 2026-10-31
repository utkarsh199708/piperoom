#!/usr/bin/env python3
"""Checks the two setup releases of the chart (docs/design/setup-strimzi.md §4).

Usage: helm template ... | check.py pipeline|runner

pipeline: every Strimzi CR is a paused shell, nothing else is a Strimzi kind,
the server's Deployment is absent, and the D11 admission policy (when
rendered) fails closed, is bound with Deny to this pipeline's named objects
and holds each rule the runner's allow-list holds.
runner: the Role grants no create, no escalate/bind/impersonate, no "*", no
list but on events, every rule but the events ones names its objects, and the
Secret, KafkaConnect and pod rules grant exactly the verbs the runner sends;
the RoleBinding binds the Role to the Job's ServiceAccount only; a
NetworkPolicy (when rendered) selects the Job's pod, admits nothing and lets
it reach DNS, the API server and the server only.
The golden files pin the full render; these checks say why it must look so.
"""
import sys

import yaml

PAUSE = "strimzi.io/pause-reconciliation"


def fail(msg):
    print("check.py: " + msg, file=sys.stderr)
    sys.exit(1)


def by_kind(docs, kind):
    return [d for d in docs if d["kind"] == kind]


def check_policy(docs):
    """The D11 policy: fails closed, bound with Deny to the pipeline's objects,
    and mirrors the runner's field allow-list rule by rule."""
    policies = by_kind(docs, "ValidatingAdmissionPolicy")
    bindings = by_kind(docs, "ValidatingAdmissionPolicyBinding")
    if not policies and not bindings:
        return
    if len(policies) != 1 or len(bindings) != 1:
        fail("the admission policy renders %d policies and %d bindings, want one each" % (len(policies), len(bindings)))
    pol, bind = policies[0], bindings[0]
    spec = pol["spec"]
    if spec.get("failurePolicy") != "Fail":
        fail("the admission policy does not fail closed")
    want_res = {"kafkaconnects", "kafkaconnectors", "kafkausers"}
    got_res = set()
    for r in spec["matchConstraints"]["resourceRules"]:
        got_res |= set(r["resources"])
        if set(r["operations"]) != {"CREATE", "UPDATE"}:
            fail("the policy does not match CREATE and UPDATE")
    if got_res != want_res:
        fail("the policy matches %s, want %s" % (sorted(got_res), sorted(want_res)))
    b = bind["spec"]
    if b.get("policyName") != pol["metadata"]["name"] or bind["metadata"]["name"] != pol["metadata"]["name"]:
        fail("the binding does not bind the pipeline's policy")
    if b.get("validationActions") != ["Deny"]:
        fail("the binding's validationActions are %s, want [Deny]" % b.get("validationActions"))
    shells = {}
    for d in docs:
        if d["apiVersion"].split("/", 1)[0] == "kafka.strimzi.io" and d["kind"] in ("KafkaConnect", "KafkaConnector", "KafkaUser"):
            shells.setdefault(d["kind"].lower() + "s", set()).add(d["metadata"]["name"])
    bound = {}
    for r in b["matchResources"]["resourceRules"]:
        if not r.get("resourceNames"):
            fail("a binding rule on %s names no objects" % r["resources"])
        for res in r["resources"]:
            bound.setdefault(res, set()).update(r["resourceNames"])
    if bound != shells:
        fail("the binding names %s, the shells are %s" % (bound, shells))
    exprs = "\n".join(v["expression"] for v in spec["validations"])
    for kind, needles in {
        "KafkaConnect": ["!has(object.spec.image)", "!has(object.spec.template)", "pushSecret ==", "-cluster-ca-cert'",
                         "certificateAndKey.secretName ==", "passwordSecret.secretName ==", "config.providers",
                         "io.strimzi.kafka.KubernetesSecretConfigProvider", "a.sha512sum.matches", "a.url.startsWith(h)",
                         "logging.valueFrom.configMapKeyRef.name ==", "metricsConfig.valueFrom.configMapKeyRef.name =="],
        "KafkaConnector": ["object.spec.class.startsWith('io.debezium.connector.')", "override", "initial.statements",
                           "bootstrap.servers", "heartbeat.action.query", "-db-credentials:"],
        "KafkaUser": ["!has(object.spec.authentication.password)", "a.resource.type == 'topic'", "patternType == 'prefix'",
                      "a.resource.type == 'group'", "a.operations.all("],
    }.items():
        if "object.kind != '%s'" % kind not in exprs:
            fail("the policy has no rule for %s" % kind)
        for n in needles:
            if n not in exprs:
                fail("the policy's %s rules do not hold %r" % (kind, n))
    # A KafkaUser's topic ACL is the pipeline's prefix, never "*" or "".
    if "== '*.'" in exprs or "== '.'" in exprs or "'*'" in exprs.split("a.resource.type == 'topic'", 1)[1].split("a.resource.type == 'group'", 1)[0]:
        fail("the KafkaUser rule admits a wildcard or empty topic prefix")
    if "'https://" not in exprs:
        fail("the build artifact rule pins no host")


# The verbs each named Secret, the KafkaConnect and the pods get: exactly what
# the runner's ops send (internal/setup/targets/strimzi), nothing more.
# Messages about these rules print fixed text only: never a Secret's name or
# the suffix table (CodeQL: clear-text logging of sensitive information).
SECRET_VERBS = {"-pairing": {"delete"}, "-confirm": {"get", "delete"}, "-db-credentials": {"get", "patch"}, "-piperoom-ro": {"get"}}


def check_runner_rules(docs, role):
    secrets = {}
    for rule in role["rules"]:
        res = rule["resources"]
        verbs = set(rule["verbs"])
        if "watch" in verbs:
            fail("the Role grants watch on %s; no op watches" % res)
        if res == ["secrets"]:
            for n in rule["resourceNames"]:
                if n in secrets:
                    fail("the Role names one Secret twice")
                secrets[n] = verbs
        if res == ["kafkaconnects"] and verbs != {"get", "patch"}:
            fail("kafkaconnects: verbs %s, want get and patch" % sorted(verbs))
        if res in (["pods"], ["pods/log"]) and verbs != {"get"}:
            fail("%s: verbs %s, want get" % (res[0], sorted(verbs)))
        if res == ["events"] and rule["apiGroups"] != [""]:
            fail("events are listed in the core group only")
    if len(secrets) != len(SECRET_VERBS):
        fail("the Role does not name exactly one rule per runner Secret")
    for name, verbs in secrets.items():
        want = [v for suffix, v in SECRET_VERBS.items() if name.endswith(suffix)]
        if len(want) != 1 or verbs != want[0]:
            fail("a named Secret's verbs are not the ones the runner sends")
    pods = [r for r in role["rules"] if r["resources"] == ["pods"]][0]["resourceNames"]
    logs = [r for r in role["rules"] if r["resources"] == ["pods/log"]][0]["resourceNames"]
    if pods != logs or len(pods) != 4:
        fail("pods/log must name exactly the four Connect and build pods")
    rbs = by_kind(docs, "RoleBinding")
    sas = by_kind(docs, "ServiceAccount")
    if len(rbs) != 1 or len(sas) != 1:
        fail("the runner release renders %d RoleBindings and %d ServiceAccounts, want one each" % (len(rbs), len(sas)))
    rb = rbs[0]
    if rb["roleRef"] != {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role["metadata"]["name"]}:
        fail("the RoleBinding does not bind the runner's Role")
    subj = rb["subjects"]
    if len(subj) != 1 or subj[0].get("kind") != "ServiceAccount" or subj[0].get("name") != sas[0]["metadata"]["name"]:
        fail("the RoleBinding's subject is not the Job's ServiceAccount alone")


def check_network_policy(docs, job):
    nps = by_kind(docs, "NetworkPolicy")
    if not nps:
        return
    if len(nps) != 1:
        fail("the runner release renders %d NetworkPolicies" % len(nps))
    np = nps[0]["spec"]
    labels = job["spec"]["template"]["metadata"]["labels"]
    for k, v in np["podSelector"]["matchLabels"].items():
        if labels.get(k) != v:
            fail("the NetworkPolicy does not select the Job's pod (%s)" % k)
    if sorted(np["policyTypes"]) != ["Egress", "Ingress"] or np.get("ingress"):
        fail("the Job's NetworkPolicy must admit no ingress and govern egress")
    ports = set()
    for rule in np["egress"]:
        for p in rule.get("ports", []):
            ports.add((p.get("protocol", "TCP"), p["port"]))
    for want in (("UDP", 53), ("TCP", 443)):
        if want not in ports:
            fail("the Job's egress lacks %s/%s" % want)


def main():
    mode = sys.argv[1] if len(sys.argv) == 2 else ""
    docs = [d for d in yaml.safe_load_all(sys.stdin) if d]
    kinds = [d["kind"] for d in docs]
    if "Deployment" in kinds or "Service" in kinds:
        fail("a setup release rendered the server")
    if mode == "pipeline":
        shells = [d for d in docs if d["apiVersion"].split("/", 1)[0] == "kafka.strimzi.io"]
        if not shells:
            fail("no Strimzi shells rendered")
        for d in shells:
            ann = d["metadata"].get("annotations") or {}
            if ann.get(PAUSE) != "true":
                fail("%s %s is not paused" % (d["kind"], d["metadata"]["name"]))
            if "spec" not in d:
                fail("%s %s has no spec (v1 requires one)" % (d["kind"], d["metadata"]["name"]))
        # The message names no object: the check is about shape, and a
        # Secret's fields are never echoed.
        for obj in (d for d in docs if d["kind"] == "Secret"):
            if obj.get("type") != "Opaque" or obj.get("data") or obj.get("stringData"):
                fail("a Secret in the pipeline release is not an empty Opaque Secret")
        check_policy(docs)
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
        check_runner_rules(docs, roles[0])
        job = [d for d in docs if d["kind"] == "Job"][0]
        check_network_policy(docs, job)
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
