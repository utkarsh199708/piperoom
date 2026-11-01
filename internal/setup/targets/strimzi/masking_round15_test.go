package strimzi

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// round15Secret marks the round 15 inputs' secrets.
const round15Secret = "S7vQmXw2Lp"

// round15Cases are the round 15 security re-review's leaks: every read
// must mask them. They are in the corpus.
func round15Cases() []corpusCase {
	s := round15Secret
	var cs []corpusCase
	add := func(line string, markers ...string) {
		if len(markers) == 0 {
			markers = []string{s}
		}
		cs = append(cs, corpusCase{name: "round 15 " + strings.ReplaceAll(line[:min(len(line), 200)], "\n", `\n`), line: line, markers: markers})
	}
	// F1: a later key line adds columns; it never drops one.
	add("name,password,kind,note\nsvc,S1x,token,\"rotated, weekly\"\ncarol,QxZkTrVbNmWq77,api,ok", "QxZkTrVbNmWq77")
	for _, l := range []string{
		"user,password,host\nbob,x,y,token,z\ncarol," + s + ",db2",
		"user,password,host\na,b,c,secret,d\ncarol," + s + ",db2",
		// F2, F3: an unbalanced quote, a blank or a final colon in a key cell.
		"id,\"password\n1," + s, "id,password\"\n1," + s, "user,db password,host\nalice," + s + ",db1",
		"user,DB Password,host\nalice," + s + ",db1", "user,Admin Password:,host\nalice," + s + ",db1",
		// F4: a key in the comment after a JSON key's block indicator.
		`"password": | # token=` + s, `"password": | # token: ` + s + "\n  x",
		// F5: -p after any blank.
		"mysql -u root\f-p" + s, "mysql -u root\v-p" + s,
		// psql and mysql tables with rule lines.
		" user  | password \n-------+----------------\n alice | " + s,
		"+-------+----------+\n| user  | password |\n+-------+----------+\n| alice | " + s + " |\n+-------+----------+",
		// -p in a read that names mysql or mariadb, whatever is before it.
		"image: mysql:8\nargs: [-uroot, -p" + s + "]", "image: mariadb\nargs: -uroot -p" + s, "command:\n- mysql\n-\t-p" + s,
		`{"command":["mysql","-pAbc\"` + s + `"]}`,
		// Grouped short flags and the shortest long-option prefix.
		"mysql -Bp" + s, "mysql --pa=" + s,
		// A row past 4096 bytes under a credential header.
		"user,password,host\nalice," + s + "," + strings.Repeat("x", 5000),
	} {
		add(l)
	}
	// F6: a 0x session value outside ZooKeeper's own shape.
	for _, l := range []string{"sessionid=0x1a2b3c4d", `{"sessionid":"0x1a2b3c4d"}`, "user,sessionid\nbob,0x1a2b3c4d", "sessionid:0x1a2b3c4d"} {
		add(l, "1a2b3c4d")
	}
	// F7: a column takes the most sensitive kind seen for it.
	add("id,sessionid\nid,password\n1,1234567", "1234567")
	return cs
}

// round15KeepCases: ZooKeeper's session diagnostic stays readable.
func round15KeepCases() []corpusCase {
	l := "processing sessionid:0x100000b3ca50000 type:create cxid:0x1 zxid:0x2 (org.apache.zookeeper.server.FinalRequestProcessor)"
	return []corpusCase{{name: "round 15 keep " + l, line: l, keep: l}}
}

// Round 15: every read masks the security re-review's inputs.
func TestRound15Regressions(t *testing.T) {
	for _, c := range round15Cases() {
		got := readsOf(t, nil, c.line)
		for _, m := range c.markers {
			if bad := leaksIn(got, m); len(bad) > 0 {
				t.Errorf("%q: %q leaks in %v", c.line, m, bad)
			}
		}
	}
	for _, c := range round15KeepCases() {
		if got := readsOf(t, nil, c.keep)["k8s_logs"]; strings.Contains(got, "masked") {
			t.Errorf("%q is masked: %s", c.keep, got)
		}
	}
}

// connectLogLines are n log4j-timestamped Kafka Connect lines with commas
// in their timestamps and messages.
func connectLogLines(n int) []string {
	var ls []string
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			ls = append(ls, fmt.Sprintf("2026-11-01 10:00:%02d,1%02d INFO [orders-sink|task-0] [Consumer clientId=connector-consumer-orders-sink-0, groupId=connect-orders-sink] Seeking to offset %d for partition orders-0 (org.apache.kafka.clients.consumer.KafkaConsumer) [task-thread-orders-sink-0]", i%60, i%100, i))
		} else {
			ls = append(ls, fmt.Sprintf("2026-11-01 10:00:%02d,2%02d INFO [orders-sink|task-0] WorkerSinkTask{id=orders-sink-0} Committing offsets for partitions orders-0, orders-1, orders-2 (org.apache.kafka.connect.runtime.WorkerSinkTask) [task-thread-orders-sink-0]", i%60, i%100))
		}
	}
	return ls
}

// round15LogTriggers are prose lines naming a credential in a comma list
// (round 15 correctness review): the lines after them are no table rows.
var round15LogTriggers = []string{
	"2026-11-01 10:00:06,900 INFO [debezium] column.exclude.list = inventory.users.ssn, inventory.users.password, inventory.users.card (io.debezium.connector.mysql.MySqlConnectorConfig) [main]",
	"ReplaceField excluding fields: id, password, ssn", "Missing keys in secret app-creds: username, password",
	"Unsupported SASL mechanisms: PLAIN, token", "Required fields: user, password, host",
	"Credentials: user, password, token have been rotated",
	"Unsupported grant types: password, client_credentials, refresh_token",
}

// Round 15, correctness: after a trigger line, the Connect log lines come
// back unchanged through the line reads (k8s_events joins and cuts a
// message: no lines there); only the trigger's own cells may be masked. A Debezium line before 150 Connect lines masks no other line.
func TestRound15LogLinesAreNoTable(t *testing.T) {
	for _, trig := range round15LogTriggers {
		after := connectLogLines(5)
		got := readsOf(t, nil, trig+"\n"+strings.Join(after, "\n"))
		for _, op := range []string{"k8s_logs", "strimzi_build_status"} {
			for _, l := range after {
				j, _ := json.Marshal(l)
				if !strings.Contains(got[op], strings.Trim(string(j), `"`)) {
					t.Errorf("%s after %q: %q is masked: %s", op, trig, l, got[op])
				}
			}
		}
	}
	after := connectLogLines(150)
	got := readsOf(t, nil, round15LogTriggers[0]+"\n"+strings.Join(after, "\n"))["k8s_logs"]
	if n := strings.Count(got, "[masked:credential]"); n != 1 {
		t.Errorf("Debezium: %d masks, want 1 (the trigger line's own cell)", n)
	}
	for _, l := range after[len(after)-20:] {
		if !strings.Contains(got, l) {
			t.Errorf("Debezium: %q is masked", l)
		}
	}
	// A mysql connector's class names no mysql client: -p after it is no
	// password.
	if got := readsOf(t, nil, "io.debezium.connector.mysql.MySqlConnector started\nmkdir -p /tmp/x -pod")["k8s_logs"]; strings.Contains(got, "masked") {
		t.Errorf("mysql class: %s", got)
	}
}
