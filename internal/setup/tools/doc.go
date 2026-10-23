// Package setuptools holds the setup-only agent tools of a setup room
// (docs/design/setup-agent.md §1, §3.3, §3.4; board row U1-7):
//
//   - one KindRead tool per T0 op of the runner's catalogue (env_detect,
//     file_read, dir_list, jar_list, java_version, http_get, runner_net_probe,
//     docker_info, compose_ps, compose_config, container_inspect,
//     container_logs). Each forwards its input, checked against the op's
//     schema, as one T0 protocol.OpRequest to the runner paired to the room,
//     and returns the runner's redacted result. They take no client that
//     writes: their only dependency is the executor, and the runner refuses a
//     T0 request that names a write op (tier_below_floor). ReadTools builds
//     them; RegisterReads adds them to a read-only registry
//     (tools.NewReadOnlyRegistry), which refuses anything else;
//   - setup_plan (KindRoom): the model records its plan; the tool fills what
//     the server decides (index, target, tier, layer) with plan.Resolve
//     against the catalogue, refuses an op that is not in it, and emits
//     setup.plan through the agent's Sink;
//   - setup_request_verification (KindRoom): the model asks the server's
//     ladder runner to verify the setup. The tool holds no Sink, so it cannot
//     emit anything, setup.verified least of all; the room's agent Sink
//     refuses that type too (sink_test.go).
//
// The package is named setuptools so a file can import it next to
// piperoom/internal/tools.
//
// The net_probe op is offered as runner_net_probe: Q6a's server-side
// net_probe tool keeps its name in a setup room, and this one probes from the
// runner's machine.
package setuptools
