---
id: strimzi-not-ready
title: Strimzi KafkaConnector or KafkaConnect not ready, or the cluster operator is down
keywords: strimzi, kubernetes, k8s, operator, cluster operator, kafkaconnector, kafkaconnect, not ready, notready, ready false, paused, pause-reconciliation, use-connector-resources, crashloopbackoff, oomkilled, build failed, plugin not found, namespace, test namespace
---
## Symptoms
- `kubectl get kafkaconnector` shows READY False or empty; the KafkaConnect or its pods are not ready.
- The connector is missing from the Connect REST API, or Connect shows it but the KafkaConnector resource says otherwise.
- The cluster operator pod is not ready or restarting: no KafkaConnector changes are applied while it is down, but running connectors keep running.
- Common causes: the KafkaConnect lacks the `strimzi.io/use-connector-resources: "true"` annotation; the connector class is not in the Connect image (build failed or plugin missing); the resource carries `strimzi.io/pause-reconciliation: "true"`; the operator does not watch this namespace; Connect pods OOMKilled or in CrashLoopBackOff.

## Find it with Piperoom tools
- `connect_status` — what Connect itself reports for the connector and its tasks
- `connect_list_connectors` — whether Connect knows the connector at all
- `connect_config` — the configuration Connect is running, to compare with the KafkaConnector spec
- `pg_replication_slots` — WAL kept on the source while the connector is down

## Safe checks
- With the Strimzi reads configured (strimzi_connector_status, strimzi_connect_status, strimzi_operator_status, k8s_object_events): the resource's Ready condition with its reason and message, observedGeneration against generation, pod restarts and termination reasons, and recent events.
- By hand: `kubectl get kafkaconnector <name> -o yaml` (read `.status.conditions` and `.status.connectorStatus`), `kubectl get kafkaconnect <name> -o yaml`, `kubectl get pods -l strimzi.io/kind=KafkaConnect`, `kubectl get events --field-selector involvedObject.name=<name>`.
- Check which namespaces the operator watches (`STRIMZI_NAMESPACE` on the operator Deployment) when a test namespace is new.

## Fixes
- Action `connect_restart_connector` — when the resource is Ready but a task is FAILED; it restarts only the failed tasks.
- Manual — add `strimzi.io/use-connector-resources: "true"` to the KafkaConnect so the operator manages KafkaConnector resources.
- Manual — remove `strimzi.io/pause-reconciliation` when the pause is no longer wanted (check who set it and why first).
- Manual — fix the Connect image or `spec.build` so the connector class is installed; check the build pod's events and logs.
- Manual — raise the Connect pods' memory limit and JVM heap after OOMKilled; restore the operator Deployment and let it reconcile.
- Manual — add the namespace to the operator's watched namespaces and give it the RBAC it needs there.

## Risks
- Changing a connector through the Connect REST API while the operator manages it is reverted by the operator.
- Removing a pause annotation applies every pending change at once.
- A KafkaConnect spec change rolls every Connect pod; all connectors on it stop during the roll.

## Sources
- https://strimzi.io/docs/operators/latest/deploying — deploying KafkaConnect and KafkaConnector, `use-connector-resources`
- https://strimzi.io/docs/operators/latest/configuring — resource status conditions, pausing reconciliation
- https://kafka.apache.org/documentation/#connect_rest — status and restart endpoints
