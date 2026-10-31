#!/usr/bin/env python3
"""Checks the two setup releases of the chart (docs/design/setup-strimzi.md §4).

Usage: helm template ... -f values.yaml | check.py pipeline|runner values.yaml

The values file is the one the render used (merged over the chart's own
values.yaml here): the checks compare the render with what the values ask for.

pipeline: every Strimzi CR is a paused shell, nothing else is a Strimzi kind,
the server's Deployment is absent, and the D11 admission policy (when
rendered) fails closed, is bound with Deny to this pipeline's named objects
(the KafkaConnect, KafkaConnectors, KafkaUsers and the canary KafkaTopic),
holds each rule the runner's allow-list holds, and pins the values' topic
prefix, registries and plugin hosts. The CEL itself is run by celcheck/.
runner: the Role grants no create, no escalate/bind/impersonate, no "*", no
list but on events, every rule but the events ones names its objects, and the
Secret, KafkaConnect and pod rules grant exactly the verbs the runner sends;
the RoleBinding binds the Role to the Job's ServiceAccount only; a
NetworkPolicy (when rendered) selects the Job's pod, admits nothing, and has
exactly the egress rules the values ask for: DNS (port 53, to
networkPolicy.dns.to), the API server (its ports, to apiServer.to) and the
server (serverURL's port, to server.to), each "to" exactly the values' (open
only when the values leave it empty), then extraEgress verbatim, and no
other rule.
The golden files pin the full render; these checks say why it must look so.
"""
import os
import sys
from urllib.parse import urlsplit

import yaml

PAUSE = "strimzi.io/pause-reconciliation"


def fail(msg):
    print("check.py: " + msg, file=sys.stderr)
    sys.exit(1)


def merge(base, over):
    """Helm's values merge: maps merge key by key, anything else replaces."""
    if isinstance(base, dict) and isinstance(over, dict):
        out = dict(base)
        for k, v in over.items():
            out[k] = merge(base.get(k), v) if k in base else v
        return out
    return over


def load_values(path):
    chart = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "values.yaml")
    with open(chart) as f:
        base = yaml.safe_load(f)
    with open(path) as f:
        return merge(base, yaml.safe_load(f) or {})


def quote_meta(s):
    """Go's regexp.QuoteMeta (Helm's regexQuoteMeta)."""
    return "".join("\\" + c if c in "\\.+*?()|[]{}^$" else c for c in s)


def by_kind(docs, kind):
    return [d for d in docs if d["kind"] == kind]


def check_policy(docs, values):
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
    want_res = {"kafkaconnects", "kafkaconnectors", "kafkausers", "kafkatopics"}
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
        if d["apiVersion"].split("/", 1)[0] == "kafka.strimzi.io" and d["kind"] in ("KafkaConnect", "KafkaConnector", "KafkaUser", "KafkaTopic"):
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
        "KafkaConnect": ["object.spec.all(k, k in ['replicas', 'version', 'bootstrapServers', 'tls', 'authentication', 'config', 'build', 'resources', 'logging', 'metricsConfig', 'jvmOptions'])",
                         "object.spec.jvmOptions.all(k, k in ['-Xms', '-Xmx'])", "-kafka-bootstrap(", "output.type == 'docker'",
                         "string(object.spec.build.output.image).matches(", "'config.providers.secrets.class']",
                         "!string(object.spec.config[k]).contains('${')", "pushSecret ==", "-cluster-ca-cert'",
                         "certificateAndKey.secretName ==", "passwordSecret.secretName ==", "config.providers",
                         "io.strimzi.kafka.KubernetesSecretConfigProvider", "a.sha512sum.matches", "a.url.startsWith(h)",
                         "logging.valueFrom.configMapKeyRef.name ==", "metricsConfig.valueFrom.configMapKeyRef.name =="],
        "KafkaConnector": ["object.spec.class.startsWith('io.debezium.connector.')", "override", "initial.statements",
                           "bootstrap.servers", "heartbeat.action.query", "-db-credentials:",
                           "object.spec.all(k, k in ['class', 'tasksMax', 'config', 'autoRestart'])", "object.spec.tasksMax == 1",
                           r"r'^([^$]|\$+[^${]|\$\{secrets:", "'${strimzienv:CERTS_STORE_PASSWORD}'", "'/tmp/kafka/cluster.truststore.p12'",
                           "'org.apache.kafka.connect.transforms.RegexRouter'", "'io.debezium.transforms.Filter'", "trustservercertificate=true"],
        "KafkaTopic": ["piperoom_canary$'", "'strimzi.io/pause-reconciliation' in object.metadata.annotations", "object.spec.partitions == 1"],
        "KafkaUser": ["!has(object.spec.authentication.password)", "a.resource.type == 'topic'", "patternType == 'prefix'",
                      "a.resource.type == 'group'", "a.operations.all(", "a.operation in ["],
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
    pv = values["setup"]["pipeline"]
    prefix = pv["topicPrefix"]
    if "a.resource.name == '%s.'" % prefix not in exprs or "r'^%s\\." % quote_meta(prefix) not in exprs:
        fail("the KafkaUser and KafkaTopic rules do not pin the values' topicPrefix")
    for h in pv["pluginHosts"]:
        if "'https://%s/'" % h not in exprs:
            fail("the build artifact rule does not hold a pluginHosts entry")
    regs = "|".join(quote_meta(r) for r in pv["imageRegistries"])
    if not regs or "matches(r'^(%s)/" % regs not in exprs:
        fail("the build output rule does not pin the values' imageRegistries")


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


def check_network_policy(docs, job, values):
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
    rv = values["setup"]["runner"]
    nv = rv["networkPolicy"]
    u = urlsplit(rv["serverURL"])
    server_port = u.port or (443 if u.scheme == "https" else 80)

    def rule(ports, to):
        r = {"ports": [{"port": p, "protocol": proto} for proto, p in ports]}
        if to:
            r["to"] = to
        return r

    want = [
        rule([("UDP", 53), ("TCP", 53)], values["networkPolicy"]["dns"].get("to")),
        rule([("TCP", p) for p in nv["apiServer"]["ports"]], nv["apiServer"].get("to")),
        rule([("TCP", server_port)], nv["server"].get("to")),
    ] + list(nv.get("extraEgress") or [])
    got = np.get("egress") or []
    names = ["DNS", "API server", "server"]
    for i, (g, w) in enumerate(zip(got, want)):
        what = names[i] if i < len(names) else "extraEgress[%d]" % (i - len(names))
        if g.get("ports") != w.get("ports"):
            fail("the Job's %s egress has other ports than the values ask for" % what)
        if g.get("to") != w.get("to"):
            fail("the Job's %s egress reaches other destinations than the values' (an unconstrained \"to\" where the values give one, or another one)" % what)
        if set(g) - {"ports", "to"}:
            fail("the Job's %s egress has fields beyond ports and to" % what)
    if len(got) != len(want):
        fail("the Job's NetworkPolicy has %d egress rules, the values ask for %d" % (len(got), len(want)))


def main():
    if len(sys.argv) != 3:
        fail("usage: check.py pipeline|runner values.yaml")
    mode = sys.argv[1]
    values = load_values(sys.argv[2])
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
        check_policy(docs, values)
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
        check_network_policy(docs, job, values)
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
