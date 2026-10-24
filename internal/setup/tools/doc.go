// Package setuptools holds the setup room's tools
// (docs/design/setup-agent.md §3.1, §3.3, §3.4; row U1-7):
//
//   - the setup-only read tools (env_detect, file_read, dir_list, jar_list,
//     java_version, docker_info, compose_ps, compose_config,
//     container_inspect, container_logs), each one fixed runner op at T0 and
//     nothing else. Register puts them in a tools.ReadOnlyRegistry, so a tool
//     that ever claimed KindRoom would fail registration;
//   - setup_config_help (row U3b), a KindRead tool over the shipped SMT,
//     predicate, sink and tuning index (internal/setup/knowledge) that holds
//     no runner and reads no customer system; RegisterConfigHelp adds it;
//   - the connector config builder (ConfigBuilder): SMTs, predicates and
//     sink connector configs are added only through the same index, and
//     SinkRegistrationParams frames a sink as Q5c's connect_create_connector
//     step, which runs over the room's (remote) Connect client;
//   - the two KindRoom tools, setup_plan and setup_request_verification,
//     built by RoomTools for the agent to add beside its own room tools.
//
// A forwarding tool never takes an op name from its input: the op is a
// constant in this package (readSpec), the input only supplies that op's
// bounded parameters, and an unknown field is refused with
// tools.ErrBadInput. There is no generic "run this op" tool.
//
// Reads reach the runner through protocol.RunnerExecutor, the same executor
// the remote clients use; a room without a paired runner answers
// protocol.ErrNoRunner before an OpRequest is built. The runner redacts and
// caps every result (design §2.4, §2.5), and this package truncates a Summary
// again with tools.Truncate so the model never sees more than the contract
// allows.
//
// The KindRoom tools emit only what they own: setup_plan emits setup.plan and
// setup_request_verification emits the verification request event. Neither
// can report an outcome: setup.verified is the server ladder runner's alone
// (events.SetupVerified, docs/design/setup-agent.md §3.4), and the test with a
// refusing sink proves the request tool never writes it.
package setuptools
