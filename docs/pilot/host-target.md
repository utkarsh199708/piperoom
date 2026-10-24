# Host target: Kafka Connect as a systemd user unit or a Windows service

For the engineer who will let `piperoom-setup` run a Kafka Connect worker on a plain Linux or Windows host (no Docker,
no Kubernetes). It says which systems are supported, what the target installs and where, which steps need `sudo` or an
Administrator prompt and which do not, which account the service runs as, what approvals each step needs, how to undo
it, and what it does not do.

Checked against `main` at `7f835196`. Every claim names the file and line it comes from (`path:line`), or a section of
a design document. If a file changes, update the claim and its citation in the same PR. Where the repository does not
say something, this page says so (section 9) instead of guessing. Nothing here was run on a real machine: the real runs
(row U8-R, `tasks/U-board.md:180`) have no write-up in the tree.

## Status on this release: written, not yet wired

| Piece | State | Where |
|---|---|---|
| The host driver: `service_status`, `journal_tail`, `properties_read` (T0); `properties_write` (T1); `service_install`, `service_restart` (T2) | code and unit tests exist | `internal/setup/targets/host/host.go:11-13`, `internal/setup/targets/host/host.go:195-198` |
| Registering the driver in the runner | **not done**. `host.Register` is called only from tests and the doc comment calls it "one line outside this package". The shipped `piperoom-setup` builds its catalogue with `ops.New`, which registers the nine generic ops only | `internal/setup/targets/host/host.go:53-59`; `internal/setup/setupcmd/setupcmd.go:104`; `internal/setup/ops/op.go:213-220` |
| The server's action catalogue for the agent | **no host entry**. `EnvironmentSpecs` lists `file_write`, `file_delete`, `compose_write`, `properties_write`, `compose_up`, `compose_restart`, `compose_down`, `plugin_install`; its `properties_write` file enum is `.env`, `worker.properties`, `connect-distributed.properties`, not the host driver's two file names | `internal/setup/actions/specs.go:26,39,51,72,77,88,104,116,128` |
| The `run-service` entry point the Windows service runs | wired: `setupcmd.Run` dispatches it before it parses any flag | `internal/setup/setupcmd/setupcmd.go:41-44` |
| Verification ladder for a `host` target | defined (rungs 1 and 2 read the Connect REST status) | `internal/setup/plan/ladder.go:31-39` |

So today the agent cannot propose a host step from a room, and a runner started by hand has no `service_install`. The
rest of this page describes what the driver does once it is registered. The names are the driver's own.

## 1. Supported systems

| System | Service manager | Scope | Where |
|---|---|---|---|
| Linux with systemd | `systemctl --user` (a user-level unit) | the OS user that runs `piperoom-setup` | `internal/setup/targets/host/host.go:1-9`; `internal/setup/targets/host/host.go:190-191` |
| Windows | the service control manager, through `golang.org/x/sys/windows/svc/mgr` | the machine | `internal/setup/targets/host/host.go:192-193`; `internal/setup/targets/host/winsvc.go:27-32` |
| anything else (macOS, other init systems) | none: `service_status`, `service_install` and `service_restart` answer "service ops are not supported on this OS" | | `internal/setup/targets/host/host.go:135-138`; `internal/setup/targets/host/host.go:226-232` |

- **System-scope systemd units are not offered.** The reason in the code: a system unit must be root-owned and outside
  every user-writable directory, and every file the runner writes is under the project directory, so a system unit
  linked from there would let its owner edit it into a root service. A root-owned unit directory outside the project
  "needs a design ruling and is not in this row" (`internal/setup/targets/host/host.go:28-34`).
- `properties_read`, `properties_write` and `journal_tail` are registered on every OS (`internal/setup/targets/host/host.go:195-198`), but
  `journal_tail` runs `journalctl`, so it works only on a systemd host (`internal/setup/targets/host/journal.go:162-167`).
- The target runs one thing: a Kafka Connect worker, `distributed` (default) or `standalone`
  (`internal/setup/targets/host/unit.go:132-151`). It installs no Kafka broker, no database and no JDK; section 8 has the
  details.

## 2. The ops

| Op | Floor | What it does | Source |
|---|---|---|---|
| `service_status` | T0 | Linux: `systemctl --user show` of one `piperoom-<name>.service`. Windows: the service control manager's query | `internal/setup/targets/host/ops.go:47-49`; `internal/setup/targets/host/systemd.go:150-174`; `internal/setup/targets/host/winsvc.go:372-425` |
| `journal_tail` | T0 | `journalctl [--user] -u <unit> -n <=200> --no-pager -o short-iso`; output capped, redacted, newest last | `internal/setup/targets/host/journal.go:215-233`; `internal/setup/targets/host/journal.go:25` |
| `properties_read` | T0 | reads a worker file and reports missing keys, keys of the other mode and an unset `plugin.path` | `internal/setup/targets/host/ops.go:290-292`; `internal/setup/targets/host/ops.go:309-363` |
| `properties_write` | T1 | writes `connect-distributed.properties` or `connect-standalone.properties` at the project root from allow-listed keys | `internal/setup/targets/host/ops.go:208-210`; `internal/setup/targets/host/properties.go:27-30` |
| `service_install` | T2 | Linux: renders the unit file and enables it. Windows: stages a release and registers the service | `internal/setup/targets/host/ops.go:124-126`; `internal/setup/targets/host/systemd.go:212-280`; `internal/setup/targets/host/winsvc.go:452-582` |
| `service_restart` | T2 | Linux: `systemctl --user restart`. Windows: stop if running, then start | `internal/setup/targets/host/ops.go:80-82`; `internal/setup/targets/host/systemd.go:282-332`; `internal/setup/targets/host/winsvc.go:941-994` |

No op takes a command string. The one program the Linux side runs is `systemctl`, with a fixed verb table (`show`,
`enable`, `daemon-reload`, `restart`), always `--user`, and an operand that is a `piperoom-*` unit name or the absolute
path of a unit file in the project's `systemd/` directory (`internal/setup/targets/host/systemd.go:34-36`;
`internal/setup/targets/host/systemd.go:50-94`). `journal_tail` takes a unit name that must start with one of a fixed
list of pipeline services (`piperoom`, `kafka`, `connect`, `confluent`, `redpanda`, `zookeeper`, `schema-registry`,
`debezium`, `postgresql`, `postgres`, `mysql`, `mysqld`, `mariadb`, `mongod`, `mongodb`, `mssql-server`), so it cannot read
`sshd` or a desktop session (`internal/setup/targets/host/journal.go:48-60`; `internal/setup/targets/host/journal.go:257-275`).
`service_install` takes only a `name` and a `mode`; the unit text, the executable, its arguments and the JVM command line
come from templates and from detection (`internal/setup/targets/host/host.go:15-26`;
`internal/setup/targets/host/ops.go:132-138`). An AST test guards that no shell is run
(`internal/setup/targets/host/host_test.go:251`).

The service name is `piperoom-<name>` with `<name>` matching `^[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?$` (default
`connect`); on Linux it is `piperoom-<name>.service`, on Windows there is no suffix
(`internal/setup/targets/host/unit.go:141-144`; `internal/setup/targets/host/unit.go:178-199`;
`internal/setup/targets/host/host.go:93-95`).

### What you must put in the project directory first

| Needed | Checked by | Source |
|---|---|---|
| a Kafka distribution extracted at `<project>/kafka/` (Linux: with an executable `bin/connect-distributed.sh` or `bin/connect-standalone.sh`; Windows: with `libs/`) | `service_install` fails with a message naming `kafka/` if it is missing. No op downloads it | `internal/setup/targets/host/unit.go:261-265`; `internal/setup/targets/host/unit.go:307-311`; `internal/setup/targets/host/integration_test.go:28-31` |
| the worker file, written by `properties_write` | `service_install` requires it to exist | `internal/setup/targets/host/unit.go:266-268`; `internal/setup/targets/host/unit.go:312-314` |
| `java` on `PATH`, resolving (symlinks followed) to `<JDK>/bin/java` | the JDK is pinned into the unit or service, not looked up again at start | `internal/setup/targets/host/unit.go:416-442`; `internal/setup/targets/host/unit.go:449-468` |

The driver reads the JDK path but compares no Java version: I found no version check in the package (section 9).
`java_version` (T0, in the generic catalogue) reports the version (`internal/setup/ops/envops.go:105-121`).

## 3. What it installs and where

### Linux (systemd user unit)

| What | Path | Written by | Source |
|---|---|---|---|
| worker properties, mode 0600 | `<project>/connect-distributed.properties` (or `-standalone`) | `properties_write` | `internal/setup/targets/host/ops.go:239`; `internal/setup/targets/host/properties.go:27-30` |
| unit file, mode 0644 | `<project>/systemd/piperoom-<name>.service` | `service_install` | `internal/setup/targets/host/systemd.go:237`; `internal/setup/targets/host/host.go:85-86` |
| link into the user's unit search path | made by `systemctl --user enable <absolute unit path>`, then `systemctl --user daemon-reload` | `service_install` | `internal/setup/targets/host/systemd.go:241-251` |

`service_install` enables the unit; it does not start it. Its summary says "service_restart starts it"
(`internal/setup/targets/host/systemd.go:272`). The unit file is the template and nothing else
(`internal/setup/targets/host/unit.go:42-66`): `Type=simple`; `WorkingDirectory` is the project directory; `Environment`
sets `JAVA_HOME` (the detected JDK) and `LOG_DIR` (`<project>/logs`); `ExecStart` is
`<project>/kafka/bin/connect-<mode>.sh <project>/connect-<mode>.properties`; `Restart=on-failure`, `RestartSec=10`,
`StartLimitIntervalSec=300`, `StartLimitBurst=5`, `SuccessExitStatus=143`, `NoNewPrivileges=yes`, `UMask=0077`,
`WantedBy=default.target`; there is no `User=`, `Group=`, `DynamicUser=`, `ExecStartPre=` or drop-in
(`internal/setup/targets/host/unit.go:42-44`; `internal/setup/targets/host/unit.go:273-286`).
The project's `kafka/`, `connect-plugins/` and `systemd/` directories are registered as mount directories, so the
generic `file_write` and `file_delete` refuse them (`internal/setup/targets/host/host.go:203-214`;
`internal/setup/targets/host/files.go:37-39`).

Path and name rules, all "refuse, never escape":

- The project directory, the JDK and every unit value may use only `A-Z a-z 0-9 . _ / + -`. A space, `%`, `$`, a quote or a
  backslash is refused because systemd would split or expand it. A project under a path with a space fails on Linux
  (`internal/setup/targets/host/unit.go:68-74`; `internal/setup/targets/host/unit.go:86-96`;
  `internal/setup/targets/host/unit.go:257-260`).
- Unit names are per OS user, not per project. If systemd already loads the name from another file, `service_install`
  refuses and asks for another name, and `service_restart` and `service_status` say the unit is not this project's
  (`internal/setup/targets/host/systemd.go:160-167`; `internal/setup/targets/host/systemd.go:221-235`;
  `internal/setup/targets/host/systemd.go:296-302`).
- A JDK inside the project directory is accepted only under `kafka/`; a project directory inside the JDK is refused. The
  reason: a JDK file that a T1 write could change would alter what the T2-approved unit runs
  (`internal/setup/targets/host/unit.go:470-536`).

### Windows (service)

The service never runs from the project directory. `service_install` stages a **release** into an Administrators-only
home and points the service at it (`internal/setup/targets/host/servicehome.go:20-48`;
`internal/setup/targets/host/host.go:34-43`):

```
%ProgramFiles%\Piperoom\piperoom-<name>\
    releases\<id>\bin\piperoom-setup.exe   the runner copy the service control manager starts
    releases\<id>\kafka\libs\...           the classpath
    releases\<id>\kafka\config\...         log4j configuration (if the project has one)
    releases\<id>\connect-plugins\...      the plugins (if the project has any)
    releases\<id>\config\connect-<mode>.properties   the worker file, re-validated, paths rewritten
    logs\                                   the service writes here
    data\                                   a standalone worker's offsets
    service.txt                             the definition install recorded
```

(`internal/setup/targets/host/servicehome.go:31-39`.) Also written: `<project>\services\piperoom-<name>.txt`, a marker
of the definition; the record install trusts is the one in the home (`internal/setup/targets/host/host.go:45-49`;
`internal/setup/targets/host/winsvc.go:532-538`).

- `%ProgramFiles%` comes from the known-folder API, not from the environment
  (`internal/setup/targets/host/servicehome_windows.go:15-24`).
- The release id is the first 16 hex digits of a SHA-256 over the file list and digests, so the preview names exactly what
  will run; a file that changes between the plan and the copy is refused. A release holds plain files only (a link or
  reparse point is refused) and at most 20000 files and 4 GiB
  (`internal/setup/targets/host/servicehome.go:60-69`; `internal/setup/targets/host/servicehome.go:177-195`;
  `internal/setup/targets/host/servicehome.go:221-232`; `internal/setup/targets/host/servicehome.go:355-380`).
- The service is registered with start type automatic, and runs
  `piperoom-setup.exe run-service --release <release> --name <name> --mode <mode> --java <JDK home>`
  (`internal/setup/targets/host/winsvc.go:329-332`; `internal/setup/targets/host/winsvc.go:684-693`). The service control
  manager cannot run `java.exe`: the installed service is the runner itself, which starts the JVM as a child
  (`internal/setup/targets/host/servicehost.go:13-20`). The JVM command line is the detected `java.exe`, `-Xms256M -Xmx2G`,
  `-Dkafka.logs.dir=<home>\logs`, the log4j argument if the distribution has one, `-cp <release>\kafka\libs\*`, the Connect
  main class and the worker file (`internal/setup/targets/host/servicehost.go:51-96`).
- `service_install` must run through `piperoom-setup`. The umbrella binary is accepted only if a `piperoom-setup.exe` sits
  next to it; any other binary is refused before anything is written, because the service would fail with Windows error
  1053 (`internal/setup/targets/host/winsvc.go:724-744`).
- The service's own log is `<home>\logs\piperoom-service.log`; the JVM's console output goes there too
  (`internal/setup/targets/host/servicehost.go:27-31`; `internal/setup/targets/host/child.go:40-43`).
- Paths must be drive-absolute (`C:\` or `C:/`). A space or a parenthesis in a path is served; UNC, `\\?\`, rooted (`\x`)
  and drive-relative (`C:foo`) paths are refused (`internal/setup/targets/host/pathrule.go:43-56`).
- A change under the project (`properties_write`, a re-extracted distribution, plugins) takes effect only at the next
  `service_install`, because the service runs from the staged release
  (`internal/setup/targets/host/winsvc.go:562`; `internal/setup/targets/host/winsvc.go:984`).

## 4. Does it need sudo or Administrator?

### Linux: no sudo, no root

Nothing in the driver runs `sudo`, `su` or any privileged command: the grep for `sudo` in the package finds no match, and
the only program run is `systemctl --user` (`internal/setup/targets/host/systemd.go:34-36`;
`internal/setup/targets/host/systemd.go:62-94`). Every file is written under the project directory through an `os.Root`
rooted there, by an atomic replace (`internal/setup/targets/host/files.go:29-41`;
`internal/setup/targets/host/files.go:62-121`). All Linux steps therefore run as the OS user that started
`piperoom-setup`, and that user needs only write access to the project directory.

What the Linux side does need:

- **A reachable user systemd manager.** When `systemctl --user` fails with "Failed to connect to bus", the step's summary
  says "start the runner from a login session, or enable lingering for this user (`loginctl enable-linger`)"
  (`internal/setup/targets/host/systemd.go:128-130`). The driver never enables lingering itself (no other mention in the
  package). The Linux integration test is documented to skip without a reachable user manager and says a CI runner needs
  `loginctl enable-linger` and `XDG_RUNTIME_DIR` (`internal/setup/targets/host/integration_test.go:41-44`).
- **For `journal_tail` of a system unit** (for example `postgresql.service`), journal access. The op reports `restricted`
  and the summary says "journal access is restricted (not in the systemd-journal or adm group), so entries may be
  missing" (`internal/setup/targets/host/journal.go:92-94`; `internal/setup/targets/host/journal.go:283-286`;
  `internal/setup/targets/host/journal.go:489`). The `user` param reads the per-user journal instead
  (`internal/setup/targets/host/journal.go:70-74`).

### Windows: an elevated prompt for the service steps

`service_install` and `service_restart` "install and start a machine service, so they need an elevated prompt"
(`internal/setup/targets/host/winsvc.go:344-352`; `internal/setup/targets/host/host.go:33-36`). The connection to the
service control manager is where a missing elevation shows: "a refusal here is how 'not elevated' surfaces"
(`internal/setup/targets/host/winsvc_windows.go:24-33`). `service_status` goes through the same `open()`
(`internal/setup/targets/host/winsvc.go:373`), so by that comment it needs the elevated prompt too even though its tier is
T0. I have not run it on Windows to confirm.

`properties_read`, `properties_write` and `journal_tail` need no elevation (they touch only the project directory, or
`journalctl`).

Windows install also refuses, rather than weakens, three conditions (`internal/setup/targets/host/winsvc.go:66-99`):

1. **The JDK must be changeable only by administrators.** One under a user profile is refused at preview with "it must be a
   JDK only administrators can change (one installed under Program Files)"; the service checks the JDK and its release
   again at every start (`internal/setup/targets/host/winsvc.go:306-313`; `internal/setup/targets/host/servicehost.go:213-220`).
2. **The staged release and its parents must be Administrators-only.** After creating the service, install hardens the
   home, then checks the release, the home and every directory above them; a service that fails is removed
   (`internal/setup/targets/host/winsvc.go:522-531`). The check allows only SYSTEM, Administrators and TrustedInstaller as
   owners or writers, and refuses a link or reparse point on the way
   (`internal/setup/targets/host/servicehome_windows.go:32-39`; `internal/setup/targets/host/servicehome_windows.go:62-102`).
3. **The Windows ACL work needs the rights to change owners and DACLs of files under Program Files**, which is why it runs
   elevated (`internal/setup/targets/host/acl_windows.go:24-31`; `internal/setup/targets/host/acl_windows.go:144-145`).

## 5. The account the service runs as

### Windows: `NT SERVICE\piperoom-<name>`, not LocalSystem

- The service logs on as its own virtual account, `NT SERVICE\<service name>`; `LocalSystem` is never used
  (`internal/setup/targets/host/winsvc.go:99`; `internal/setup/targets/host/winsvc.go:108-109`;
  `internal/setup/targets/host/winsvc.go:219-221`). The service SID type is "unrestricted" so the virtual account's SID is
  in the token (`internal/setup/targets/host/winsvc.go:106`; `internal/setup/targets/host/winsvc_windows.go:67-73`).
- **Privileges: `SeChangeNotifyPrivilege` only** (bypass traverse checking). The reason in the code: every service token
  otherwise carries `SeImpersonatePrivilege`, the step the public "potato" tools use to reach SYSTEM
  (`internal/setup/targets/host/winsvc.go:111-122`). It is set with a second call after `CreateService`; if that call
  fails the service is deleted rather than left wider than its record
  (`internal/setup/targets/host/winsvc_windows.go:84-94`).
- **File permissions** of the home and everything in it: owner Administrators and a protected DACL (inheritance from the
  parent is dropped). Administrators and SYSTEM have full control; the service account has read and execute, and modify only
  on `logs\` and `data\`; `Users` have read and execute
  (`internal/setup/targets/host/acl.go:10-17`; `internal/setup/targets/host/acl_windows.go:63-76`;
  `internal/setup/targets/host/acl_windows.go:124-149`; `internal/setup/targets/host/servicehome.go:491-500`). The project
  directory is never re-permissioned (`internal/setup/targets/host/acl.go:15-17`).
- **Domain note.** The code comment says a virtual account authenticates to a domain as the machine account
  (`DOMAIN\HOST$`) and that this row documents it (`internal/setup/targets/host/winsvc.go:97-98`). Take that as the fact
  and plan for it: anything the worker reaches with Windows (Kerberos or NTLM) authentication is reached as the computer
  account. The repository does not say which Connect setups that affects (section 9).
- **Stop behaviour.** Stop and Shutdown are relayed to the JVM, which is killed at once (`TerminateProcess`): a Windows
  service has no console to send a stop request to. A distributed worker killed this way does not leave its group, so its
  tasks are reassigned only when the member is evicted, `scheduled.rebalance.max.delay.ms` (5 minutes by default, per the
  code comment). The JVM is also held in a job object that kills it if the service host dies
  (`internal/setup/targets/host/child.go:89-113`; `internal/setup/targets/host/job_windows.go:12-17`). The SCM is told a
  30 s wait hint (`internal/setup/targets/host/servicehost.go:38-41`).
- **Failure.** If the JVM exits, the service stops with the JVM's exit code as its service-specific exit code, which
  `service_status` reads back (`internal/setup/targets/host/servicehost.go:284-289`;
  `internal/setup/targets/host/winsvc.go:195-203`). The code sets no service recovery actions (I found none in the
  package), so a crashed worker stays stopped until `service_restart` or a reboot.

### Linux: the OS user that ran the setup

A user unit runs as the user who owns the user manager; the template has no `User=`
(`internal/setup/targets/host/unit.go:42-44`). The unit is hardened with `NoNewPrivileges=yes` and `UMask=0077`
(`internal/setup/targets/host/unit.go:61-62`). Give that user the least rights the worker needs: read on `kafka/` and
`connect-plugins/`, write on `logs/`. The repository does not name a recommended dedicated account; section 9.

## 6. Tiers and approvals

| Step | Op floor | What the plan and design say it needs |
|---|---|---|
| `service_status`, `journal_tail`, `properties_read` | T0 | a read: auto-run, never a proposal (`internal/setup/protocol/protocol.go:173`; `docs/product/setup-2026-10.md:206`) |
| `properties_write` | T1 | a templated file under the project directory: auto-run after a 20 s cancellable countdown (`proposal.scheduled`, grant `policy`); a file the validator refuses is T2 (`internal/setup/protocol/protocol.go:174`; `docs/product/setup-2026-10.md:207`) |
| `service_install`, `service_restart` | T2 | an ordinary proposal with a preview. Approver: the owners of the existing pipeline the target belongs to, else a tenant admin on a team server; the driver in solo or standalone mode (`internal/setup/protocol/protocol.go:175`; `docs/product/setup-2026-10.md:208`; `docs/design/setup-agent.md:470-476`) |

- The tier of a step is the higher of the op's floor and the class of the endpoint. A target on a `production` class
  (the person said so, or the host matches `PIPEROOM_SETUP_PROD_PATTERNS`) makes the step T3: approval plus a typed confirm
  `<pipeline>/<layer>`; a restart of a database or a `wal_level` change also needs a recorded maintenance window. The
  guard table says nothing about restarting a Connect service in particular
  (`docs/design/setup-agent.md:444-460`; `docs/product/setup-2026-10.md:209`; `docs/product/setup-2026-10.md:239-249`).
  A class can be raised, never lowered, and the runner keeps its own floors (`internal/setup/protocol/protocol.go:179-187`).
- All six ops sit in the owners layer `environment` (`internal/setup/targets/host/host.go:112`).
- The class also changes one default: every class gets a loopback REST listener (`http://127.0.0.1:8083`) unless the entries
  name one; a local target must keep it on loopback, a `shared` or `production` target may name another host explicitly
  (`internal/setup/targets/host/properties.go:144-147`; `internal/setup/targets/host/properties.go:176-180`;
  `internal/setup/targets/host/properties.go:254-256`; `internal/setup/targets/host/properties.go:384-388`). The code
  comment is explicit that Connect's own default binds every interface and its REST API is unauthenticated.
- `service_install` has a `Preview` that shows the unit file (or the Windows definition) as a line diff against what is on
  disk, ending with a sentence naming what will run; the catalogue does not call it yet because `ops` lacks the optional
  interface (`internal/setup/targets/host/ops.go:160-175`; `internal/setup/targets/host/ops.go:176-185`).

What `properties_write` accepts: only keys in an allow-list, per mode, with a value format per key; at most 64 entries and
1024 bytes per value; a value that looks like a credential is refused. The common keys are `bootstrap.servers`,
`key.converter`, `value.converter`, the two `*.schemas.enable` keys, `plugin.path`, `listeners`,
`rest.advertised.host.name`, `rest.advertised.port`, `offset.flush.interval.ms` and `topic.creation.enable`. Distributed
adds `group.id`, the three `*.storage.topic` keys and their replication factors; standalone adds
`offset.storage.file.filename`. A key of the other mode is refused as the wrong-mode mistake. `plugin.path` entries must be
`connect-plugins/` or a directory under it; the standalone offsets file must be a `*.offsets` file at the project root
(`internal/setup/targets/host/properties.go:19-23`; `internal/setup/targets/host/properties.go:54-95`;
`internal/setup/targets/host/properties.go:214-216`; `internal/setup/targets/host/properties.go:275-280`). Required keys
are listed in `internal/setup/targets/host/properties.go:92-95`. Defaults it adds: `plugin.path`, `listeners` and, for
standalone, the offsets file (`internal/setup/targets/host/properties.go:176-180`).

## 7. How to undo

There is **no rollback op for any host step** on this release: the op interface has no `Rollback` method
(`internal/setup/ops/op.go:85-107`), the design lists one for a later shape (`docs/design/setup-agent.md:271`), and the
"roll back this setup" room row is U1-14b (`tasks/U-board.md:102`). What exists:

| Step | Undo that the code gives | Source |
|---|---|---|
| `properties_write` | none. The file is replaced atomically and no backup is kept; the preview's diff is the only record of the old text. Run `properties_write` again with the old entries | `internal/setup/targets/host/files.go:98-119`; `internal/setup/targets/host/ops.go:160-175` |
| `service_install`, Linux | a failed `enable` removes a unit file this install created. After a successful install there is no uninstall op | `internal/setup/targets/host/systemd.go:259-264` |
| `service_install`, Windows | a failed install puts the previous service back (created from its validated record and started again if it was running), restores the record and the marker, and removes the release it staged. The replace order is: stop, stage next to the old release, delete and create, prune the old release | `internal/setup/targets/host/winsvc.go:441-451`; `internal/setup/targets/host/winsvc.go:617-653` |
| uninstall, Windows | `uninstall` exists (stops and deletes the service, removes its releases, the record and the marker; `logs\` and `data\` stay) but **no op exposes it**; only the package tests call it | `internal/setup/targets/host/winsvc.go:996-1042` |
| `service_restart` | a restart is not undone; `service_status` and `journal_tail` show the state afterwards | `internal/setup/targets/host/systemd.go:317-331` |

Doing it by hand is outside what the runner does. These are the standard OS commands, not taken from this repository, so
check them on your system:

- Linux: `systemctl --user disable --now piperoom-<name>.service`, delete `<project>/systemd/piperoom-<name>.service`, then
  `systemctl --user daemon-reload`.
- Windows, from an elevated prompt: `sc.exe stop piperoom-<name>`, `sc.exe delete piperoom-<name>`, then remove
  `%ProgramFiles%\Piperoom\piperoom-<name>\` and `<project>\services\piperoom-<name>.txt`. The code notes that a deleted
  service may be "marked for deletion" until every handle to it is closed
  (`internal/setup/targets/host/winsvc.go:130-133`).

## 8. Known limits

- **Not wired** (the status table at the top). Treat this page as the contract of the driver, not as a feature you can use
  from a room yet.
- **Connect worker only.** No op installs a Kafka distribution, a JDK or a broker. `service_install` needs `kafka/` already
  extracted (`internal/setup/targets/host/unit.go:262-265`; `internal/setup/targets/host/integration_test.go:28-31`).
- **No plugin install on a host registry.** `plugin_install` is registered by the Compose driver, not by `host.Register`,
  and the two drivers cannot share a registry (`internal/setup/targets/compose/plugin.go:233`;
  `internal/setup/targets/host/host.go:55-59`; `internal/setup/targets/host/host.go:195-198`). The integration test uses a
  separate Compose registry for it (`internal/setup/targets/host/integration_test.go:94`).
- **No security keys in the worker file.** The allow-list has no SASL, TLS or `config.providers` keys, on purpose
  (`internal/setup/targets/host/properties.go:54-58`). On Windows the file is re-validated at install, so a hand-added key
  outside the list makes the install fail (`internal/setup/targets/host/servicehome.go:204-212`;
  `internal/setup/targets/host/properties.go:161-163`). On Linux the install only checks that the file exists
  (`internal/setup/targets/host/unit.go:266-268`).
- **Linux: user scope only**, and no check or change of lingering (section 4).
- **Windows: stop is a kill** and no recovery actions are set (section 5). The JVM heap is fixed at `-Xms256M -Xmx2G`
  (`internal/setup/targets/host/servicehost.go:83-86`).
- **Windows: a project edit needs a new `service_install`**, which stages a new release (section 3).
- **One service name per machine (Windows) or per OS user (Linux).** The driver reports a name another project owns as
  "not this project's" and refuses to replace or restart it (`internal/setup/targets/host/winsvc.go:354-369`;
  `internal/setup/targets/host/winsvc.go:477-484`; `internal/setup/targets/host/winsvc.go:942-947`).
- **The design and the code differ on one point.** The design says `properties_write` is "T1 under the project dir, T2
  elsewhere" (`docs/design/setup-agent.md:698`); the driver writes only the two worker files at the project root and
  refuses any other name, with "nothing outside the project directory is written"
  (`internal/setup/targets/host/ops.go:274-278`).
- **Windows was designed against a spike and estimated, not measured** (`docs/design/setup-agent.md:808`). The lead ruling
  that fixed the privilege model is recorded at `docs/design/setup-agent.md:809-822`.

## 9. What this page could not cite

- A **Java version** the worker needs: the driver pins the detected JDK and compares no version (no version check in
  `internal/setup/targets/host/`; the only version reader is the T0 `java_version`).
- A **recommended dedicated Linux account**, and whether the unit starts at boot without a login: the code mentions
  lingering only in an error hint and never sets it. Boot behaviour of `WantedBy=default.target` under a user manager is
  systemd behaviour, not stated in this repository.
- Which **Connect configurations are affected** by the Windows virtual account authenticating as the machine account: the
  code comment states the fact (`internal/setup/targets/host/winsvc.go:97-98`) and nothing more.
- Whether **`service_status` on Windows works without elevation**: inferred from `open()`, not run.
- **Real-machine behaviour** of either OS: row U8-R has no write-up in the tree (`tasks/U-board.md:180`). The Linux
  integration test (`internal/setup/targets/host/integration_test.go:41-44`) and the Windows-only unit tests
  (`internal/setup/targets/host/acl_windows_test.go`, `internal/setup/targets/host/child_windows_test.go`) exist; I did not
  run them.
- The manual **undo commands** in section 7: standard OS commands, not from this repository.
