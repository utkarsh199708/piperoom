package actions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/tools"
)

// hbQuery is the query source_create_heartbeat_table names for
// public.debezium_heartbeat (sourceactions.heartbeatQuery): it must pass.
const hbQuery = `INSERT INTO "public"."debezium_heartbeat" (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts`

const pgSourceClass = "io.debezium.connector.postgresql.PostgresConnector"

func hbParams(t *testing.T, changes map[string]string) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(map[string]any{"connector": "pg-orders-connector", "changes": changes})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// hbConnect is a config fake for a Debezium Postgres source connector.
func hbConnect(extra map[string]string) *configConnect {
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	c.cfg = map[string]string{"connector.class": pgSourceClass, "database.password": "hunter2"}
	for k, v := range extra {
		c.cfg[k] = v
	}
	return c
}

func pipelineCtx() context.Context {
	return tools.WithPipeline(context.Background(), testPipeline())
}

func TestHeartbeatIntervalValues(t *testing.T) {
	for _, v := range []string{"0", "1000", "10000", "3600000"} {
		if err := checkHeartbeatInterval(v); err != nil {
			t.Errorf("%q refused: %v", v, err)
		}
	}
	for _, v := range []string{"", "1", "999", "3600001", "99999999", "-1", "+1000", "01000", "1e4", "1000 ", " 1000", "1000ms", "10000.0", "0x3e8", "１０００"} {
		err := checkHeartbeatInterval(v)
		if err == nil {
			t.Errorf("%q accepted", v)
			continue
		}
		if !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%q: error %v is not ErrBadInput", v, err)
		}
	}
}

func TestHeartbeatQueryAccepted(t *testing.T) {
	cases := map[string]string{
		"source_create_heartbeat_table form": hbQuery,
		"trailing semicolon":                 hbQuery + ";",
		"trailing semicolon and space":       hbQuery + " ;  \n",
		"lower case keywords":                `insert into public.debezium_heartbeat (id, ts) values (1, now()) on conflict (id) do update set ts = excluded.ts`,
		"do nothing":                         `INSERT INTO public.hb (id) VALUES (1) ON CONFLICT (id) DO NOTHING`,
		"string literal":                     `INSERT INTO public.hb (text) VALUES ('test_heartbeat')`,
		"mysql duplicate key":                "INSERT INTO `public`.`hb` (id, ts) VALUES (1, NOW()) ON DUPLICATE KEY UPDATE ts = NOW()",
		"update":                             `UPDATE public.hb SET ts = CURRENT_TIMESTAMP WHERE id = 1`,
		"update no where":                    `UPDATE public.hb SET ts = clock_timestamp()`,
		"multi line":                         "INSERT INTO public.hb (id, ts)\n\tVALUES (1, now())",
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			hq, err := parseHeartbeatQuery(q)
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if !strings.HasPrefix(hq.table, "public.") {
				t.Fatalf("table %q", hq.table)
			}
		})
	}
}

// Every hostile or merely unusual query is refused as ErrBadInput: the text
// runs on the source database with the connector's credentials.
func TestHeartbeatQueryHostile(t *testing.T) {
	cases := map[string]string{
		"empty":                    ``,
		"select":                   `SELECT 1`,
		"delete":                   `DELETE FROM public.hb`,
		"drop":                     `DROP TABLE public.orders`,
		"truncate":                 `TRUNCATE public.orders`,
		"second statement":         `INSERT INTO public.hb (id) VALUES (1); DROP TABLE public.orders`,
		"second statement, ending": `INSERT INTO public.hb (id) VALUES (1); DROP TABLE public.orders;`,
		"two semicolons":           `INSERT INTO public.hb (id) VALUES (1);;`,
		"line comment":             `INSERT INTO public.hb (id) VALUES (1) -- x`,
		"block comment":            `INSERT INTO public.hb /* x */ (id) VALUES (1)`,
		"mysql comment":            `INSERT INTO public.hb (id) VALUES (1) # x`,
		"unqualified table":        `INSERT INTO hb (id) VALUES (1)`,
		"sub-select values":        `INSERT INTO public.hb (id) SELECT usesysid FROM pg_user`,
		"sub-query value":          `INSERT INTO public.hb (id) VALUES ((SELECT 1))`,
		"function call":            `INSERT INTO public.hb (id) VALUES (pg_terminate_backend(1))`,
		"pg_sleep":                 `UPDATE public.hb SET ts = pg_sleep(1000)`,
		"pg_read_file":             `INSERT INTO public.hb (t) VALUES (pg_read_file('x'))`,
		"dblink":                   `INSERT INTO public.hb (t) VALUES (dblink('host=evil', 'x'))`,
		"cast":                     `INSERT INTO public.hb (id) VALUES (1::int)`,
		"escaped quote":            `INSERT INTO public.hb (t) VALUES ('a''b')`,
		"backslash literal":        `INSERT INTO public.hb (t) VALUES ('a\b')`,
		"dollar quote":             `INSERT INTO public.hb (t) VALUES ($$x$$)`,
		"e-string":                 `INSERT INTO public.hb (t) VALUES (E'x')`,
		"unterminated literal":     `INSERT INTO public.hb (t) VALUES ('x)`,
		"quoted ident with quote":  `INSERT INTO "pub""lic".hb (id) VALUES (1)`,
		"quoted ident with space":  `INSERT INTO "public"."hb x" (id) VALUES (1)`,
		"column count mismatch":    `INSERT INTO public.hb (id, ts) VALUES (1)`,
		"returning":                `INSERT INTO public.hb (id) VALUES (1) RETURNING id`,
		"update where or":          `UPDATE public.hb SET ts = now() WHERE id = 1 OR 1 = 1`,
		"update from":              `UPDATE public.hb SET ts = now() FROM public.orders`,
		"with cte":                 `WITH x AS (DELETE FROM public.orders) INSERT INTO public.hb (id) VALUES (1)`,
		"copy":                     `COPY public.hb TO PROGRAM 'rm -rf /'`,
		"call":                     `CALL do_evil()`,
		"negative number":          `INSERT INTO public.hb (id) VALUES (-1)`,
		"null byte":                "INSERT INTO public.hb (id) VALUES (1)\x00",
		"unicode lookalike":        "INSERT INTO public.hb (id) VALUES (１)",
		"three-part name":          `INSERT INTO db.public.hb (id) VALUES (1)`,
		"leading space keyword":    `INSERTINTO public.hb (id) VALUES (1)`,
		"excluded outside upsert":  `UPDATE public.hb SET ts = EXCLUDED.ts`,
		"too many columns":         `INSERT INTO public.hb (a, b, c, d, e) VALUES (1, 2, 3, 4, 5)`,
		"long literal":             `INSERT INTO public.hb (t) VALUES ('` + strings.Repeat("a", 65) + `')`,
		"identifier starts digit":  `INSERT INTO public."1hb" (id) VALUES (1)`,
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parseHeartbeatQuery(q)
			if err == nil {
				t.Fatal("accepted")
			}
			if !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("error %v is not ErrBadInput", err)
			}
		})
	}
}

func TestHeartbeatQueryScope(t *testing.T) {
	p := testPipeline() // source_tables: public.orders
	// hbQuery is the exact query source_create_heartbeat_table proposes.
	for _, q := range []string{
		hbQuery,
		`UPDATE public.heartbeat SET ts = now() WHERE id = 1`,
		`INSERT INTO public.test_heartbeat_table (text) VALUES ('test_heartbeat')`,
	} {
		if err := heartbeatQueryInScope(p, q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
	// A heartbeat table listed in source_tables (a filtered publication
	// needs it there) stays admitted.
	withHB := p
	withHB.SourceTables = append([]string{"public.debezium_heartbeat"}, p.SourceTables...)
	if err := heartbeatQueryInScope(withHB, hbQuery); err != nil {
		t.Errorf("heartbeat table in source_tables: %v", err)
	}
	for _, q := range []string{
		// A captured data table: Debezium would write literals into
		// customer rows on every heartbeat.
		`UPDATE public.orders SET ts = now() WHERE id = 1`,
		`INSERT INTO public.orders (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts`,
		// Another table next to the captured ones.
		`UPDATE public.customers SET status = 'x'`,
		`INSERT INTO public.hb (id) VALUES (1)`,
		`INSERT INTO inventory.heartbeat (id) VALUES (1)`,
		`INSERT INTO inventory.hb (id) VALUES (1)`,
		`INSERT INTO pg_catalog.pg_authid (id) VALUES (1)`,
		`INSERT INTO "Public".heartbeat (id) VALUES (1)`, // exact match: no folding
	} {
		err := heartbeatQueryInScope(p, q)
		if !errors.Is(err, tools.ErrOutOfScope) {
			t.Errorf("%s: want ErrOutOfScope, got %v", q, err)
		}
	}
	// CheckScope applies it.
	a := &updateConnectorConfig{d: testDeps()}
	if err := a.CheckScope(p, hbParams(t, map[string]string{keyHeartbeatQuery: hbQuery})); err != nil {
		t.Fatalf("CheckScope in scope: %v", err)
	}
	err := a.CheckScope(p, hbParams(t, map[string]string{keyHeartbeatQuery: `INSERT INTO inventory.hb (id) VALUES (1)`}))
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("CheckScope out of scope: %v", err)
	}
	err = a.CheckScope(p, hbParams(t, map[string]string{keyHeartbeatQuery: `DROP TABLE public.orders`}))
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("CheckScope hostile: %v", err)
	}
}

func TestHeartbeatQuerySecretLiteral(t *testing.T) {
	a := &updateConnectorConfig{d: testDeps()}
	err := a.Validate(context.Background(), hbParams(t, map[string]string{keyHeartbeatQuery: `INSERT INTO public.hb (t) VALUES ('password')`}))
	if err == nil || !strings.Contains(err.Error(), "credential") {
		t.Fatalf("err %v", err)
	}
	// SQL keywords alone do not look like a credential ("DUPLICATE KEY").
	if _, err := parseHeartbeatQuery("INSERT INTO public.hb (id) VALUES (1) ON DUPLICATE KEY UPDATE id = 1"); err != nil {
		t.Fatal(err)
	}
	if heartbeatValueLooksSecret(keyHeartbeatQuery, "INSERT INTO public.hb (id) VALUES (1) ON DUPLICATE KEY UPDATE id = 1") {
		t.Fatal("DUPLICATE KEY flagged as a credential")
	}
}

func TestHeartbeatKeysSourceOnly(t *testing.T) {
	cases := []struct {
		name, class, key, value, want string
	}{
		{"jdbc sink", "io.debezium.connector.jdbc.JdbcSinkConnector", keyHeartbeatInterval, "10000", "Debezium source"},
		{"other vendor", "io.confluent.connect.jdbc.JdbcSourceConnector", keyHeartbeatInterval, "10000", "Debezium source"},
		{"unset class", "", keyHeartbeatQuery, hbQuery, "Debezium source"},
		{"mongodb query", "io.debezium.connector.mongodb.MongoDbConnector", keyHeartbeatQuery, hbQuery, "MongoDB"},
		{"mongodb interval ok", "io.debezium.connector.mongodb.MongoDbConnector", keyHeartbeatInterval, "10000", ""},
		{"postgres ok", pgSourceClass, keyHeartbeatQuery, hbQuery, ""},
		{"mysql ok", "io.debezium.connector.mysql.MySqlConnector", keyHeartbeatInterval, "5000", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := hbConnect(map[string]string{"connector.class": tc.class})
			c.allowWrites = true
			a := configAction(t, c)
			p := hbParams(t, map[string]string{tc.key: tc.value})
			verr := a.Validate(pipelineCtx(), p)
			_, perr := a.Preview(pipelineCtx(), p)
			_, eerr := a.Execute(pipelineCtx(), p)
			for what, err := range map[string]error{"Validate": verr, "Preview": perr, "Execute": eerr} {
				switch {
				case tc.want == "" && err != nil:
					t.Errorf("%s: %v", what, err)
				case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
					t.Errorf("%s: want %q, got %v", what, tc.want, err)
				}
			}
			if tc.want != "" && len(c.updates) != 0 {
				t.Fatalf("a write happened: %v", c.updates)
			}
		})
	}
}

func TestHeartbeatValidateRefusesBadValues(t *testing.T) {
	a := &updateConnectorConfig{d: testDeps()}
	for _, ch := range []map[string]string{
		{keyHeartbeatInterval: "500"},
		{keyHeartbeatInterval: "ten seconds"},
		{keyHeartbeatQuery: `DELETE FROM public.orders`},
		{keyHeartbeatQuery: `INSERT INTO public.hb (id) VALUES (1); DROP TABLE public.orders`},
		{keyHeartbeatQuery: strings.Repeat("x", maxConfigValueLen+1)},
	} {
		if err := a.Validate(context.Background(), hbParams(t, ch)); err == nil {
			t.Errorf("%v accepted", ch)
		}
	}
	// The old one-key form takes the keys too, with the same checks.
	if err := a.Validate(context.Background(), paramsJSON(t, keyHeartbeatQuery, `SELECT 1`)); !errors.Is(err, tools.ErrBadInput) {
		t.Errorf("one-key hostile query: %v", err)
	}
}

// Execute writes both keys in one request, on a Debezium source and within
// the pipeline in ctx; without a pipeline it refuses the query.
func TestHeartbeatExecute(t *testing.T) {
	c := hbConnect(nil)
	c.allowWrites = true
	a := configAction(t, c)
	p := hbParams(t, map[string]string{keyHeartbeatQuery: hbQuery, keyHeartbeatInterval: "10000"})
	if err := a.Validate(pipelineCtx(), p); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if _, err := a.Execute(context.Background(), p); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("Execute without a pipeline: %v", err)
	}
	if len(c.updates) != 0 {
		t.Fatalf("written without a pipeline: %v", c.updates)
	}
	out, err := a.Execute(pipelineCtx(), p)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	want := []string{"pg-orders-connector heartbeat.action.query=" + hbQuery, "pg-orders-connector heartbeat.interval.ms=10000"}
	if strings.Join(c.updates, "\n") != strings.Join(want, "\n") {
		t.Fatalf("updates %q", c.updates)
	}
	if !strings.Contains(out, "heartbeat.interval.ms: unset → 10000") || strings.Contains(out, "re-read shows") {
		t.Fatalf("out %q", out)
	}
	// Out of scope: refused before any write.
	c2 := hbConnect(nil)
	c2.allowWrites = true
	q := hbParams(t, map[string]string{keyHeartbeatQuery: `INSERT INTO inventory.hb (id) VALUES (1)`})
	if _, err := configAction(t, c2).Execute(pipelineCtx(), q); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("Execute out of scope: %v", err)
	}
	if len(c2.updates) != 0 {
		t.Fatalf("written out of scope: %v", c2.updates)
	}
}

func TestHeartbeatPreview(t *testing.T) {
	type rollback struct {
		Connector string            `json:"connector"`
		Changes   map[string]string `json:"changes"`
	}
	parse := func(t *testing.T, s string) rollback {
		t.Helper()
		js, ok := strings.CutPrefix(s, "connect_update_connector_config ")
		if !ok {
			t.Fatalf("rollback %q", s)
		}
		var r rollback
		if err := json.Unmarshal([]byte(js), &r); err != nil {
			t.Fatalf("rollback %q: %v", s, err)
		}
		return r
	}
	oldQ := `UPDATE public.heartbeat SET ts = now() WHERE id = 1`

	t.Run("both unset: undo turns heartbeats off", func(t *testing.T) {
		a := configAction(t, hbConnect(nil))
		res, err := a.Preview(pipelineCtx(), hbParams(t, map[string]string{keyHeartbeatQuery: hbQuery, keyHeartbeatInterval: "10000"}))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.Current, "heartbeat.action.query = unset") || !strings.Contains(res.Current, "heartbeat.interval.ms = unset") {
			t.Errorf("current %q", res.Current)
		}
		if !strings.Contains(res.Expected, "heartbeat.action.query = "+hbQuery) || !strings.Contains(res.Expected, "heartbeat.interval.ms = 10000") {
			t.Errorf("expected %q", res.Expected)
		}
		if !res.Reversible || !strings.Contains(res.Expected, "never runs") {
			t.Fatalf("res %+v", res)
		}
		r := parse(t, res.Rollback)
		if len(r.Changes) != 1 || r.Changes[keyHeartbeatInterval] != "0" {
			t.Fatalf("rollback %+v", r)
		}
	})
	t.Run("set values: undo restores them", func(t *testing.T) {
		a := configAction(t, hbConnect(map[string]string{keyHeartbeatQuery: oldQ, keyHeartbeatInterval: "60000"}))
		res, err := a.Preview(pipelineCtx(), hbParams(t, map[string]string{keyHeartbeatQuery: hbQuery, keyHeartbeatInterval: "10000"}))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.Current, "heartbeat.action.query = "+oldQ) || !strings.Contains(res.Current, "heartbeat.interval.ms = 60000") {
			t.Errorf("current %q (the whole old query must show)", res.Current)
		}
		r := parse(t, res.Rollback)
		if !res.Reversible || r.Changes[keyHeartbeatQuery] != oldQ || r.Changes[keyHeartbeatInterval] != "60000" || len(r.Changes) != 2 {
			t.Fatalf("res %+v rollback %+v", res, r)
		}
		// The rollback itself is a valid proposal.
		if err := a.Validate(pipelineCtx(), json.RawMessage(strings.TrimPrefix(res.Rollback, "connect_update_connector_config "))); err != nil {
			t.Fatalf("rollback refused: %v", err)
		}
	})
	t.Run("query unset while heartbeats run: no undo", func(t *testing.T) {
		a := configAction(t, hbConnect(map[string]string{keyHeartbeatInterval: "60000"}))
		res, err := a.Preview(pipelineCtx(), hbParams(t, map[string]string{keyHeartbeatQuery: hbQuery}))
		if err != nil {
			t.Fatal(err)
		}
		if res.Reversible || res.Rollback != "" {
			t.Fatalf("partial undo offered: %+v", res)
		}
	})
	t.Run("old query not admitted: no undo", func(t *testing.T) {
		a := configAction(t, hbConnect(map[string]string{keyHeartbeatQuery: `INSERT INTO hb (id) VALUES (1)`, keyHeartbeatInterval: "60000"}))
		res, err := a.Preview(pipelineCtx(), hbParams(t, map[string]string{keyHeartbeatQuery: hbQuery}))
		if err != nil {
			t.Fatal(err)
		}
		if res.Reversible || res.Rollback != "" {
			t.Fatalf("undo to a query the action refuses: %+v", res)
		}
	})
	t.Run("old query out of scope: no undo", func(t *testing.T) {
		a := configAction(t, hbConnect(map[string]string{keyHeartbeatQuery: `INSERT INTO inventory.hb (id) VALUES (1)`, keyHeartbeatInterval: "60000"}))
		res, err := a.Preview(pipelineCtx(), hbParams(t, map[string]string{keyHeartbeatQuery: hbQuery}))
		if err != nil {
			t.Fatal(err)
		}
		if res.Reversible {
			t.Fatalf("undo out of scope: %+v", res)
		}
	})
	t.Run("interval only, legacy form", func(t *testing.T) {
		a := configAction(t, hbConnect(nil))
		res, err := a.Preview(pipelineCtx(), paramsJSON(t, keyHeartbeatInterval, "10000"))
		if err != nil {
			t.Fatal(err)
		}
		if res.Rollback != `connect_update_connector_config {"connector":"pg-orders-connector","key":"heartbeat.interval.ms","value":"0"}` {
			t.Fatalf("rollback %q", res.Rollback)
		}
	})
	t.Run("out of scope query", func(t *testing.T) {
		a := configAction(t, hbConnect(nil))
		_, err := a.Preview(pipelineCtx(), hbParams(t, map[string]string{keyHeartbeatQuery: `INSERT INTO inventory.hb (id) VALUES (1)`}))
		if !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("err %v", err)
		}
	})
}

func TestHeartbeatSchema(t *testing.T) {
	a := &updateConnectorConfig{d: testDeps()}
	cp := a.ParamsSchema()["properties"].(map[string]any)["changes"].(map[string]any)["properties"].(map[string]any)
	for _, k := range []string{keyHeartbeatInterval, keyHeartbeatQuery} {
		prop, ok := cp[k].(map[string]any)
		if !ok {
			t.Fatalf("schema lacks %s", k)
		}
		if _, enum := prop["enum"]; enum || prop["type"] != "string" {
			t.Errorf("%s schema %v", k, prop)
		}
	}
	if LooksSecret(keyHeartbeatQuery) || LooksSecret(keyHeartbeatInterval) {
		t.Fatal("heartbeat key looks secret")
	}
	if len(hbQuery) > maxConfigValueLen {
		t.Fatal("the heartbeat table's query exceeds the value cap")
	}
	// The longest table name source_create_heartbeat_table admits still fits.
	long := strings.Repeat("a", 63)
	q := `INSERT INTO "` + long + `"."` + long + `" (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts`
	if len(q) > maxConfigValueLen {
		t.Fatalf("longest heartbeat query is %d bytes", len(q))
	}
	if _, err := parseHeartbeatQuery(q); err != nil {
		t.Fatal(err)
	}
}
