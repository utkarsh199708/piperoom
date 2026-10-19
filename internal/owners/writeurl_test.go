package owners

import (
	"slices"
	"strings"
	"testing"
)

// TestSourceDBWriteURL: a pipeline's own connections: block may name the
// source write role (P1a-3b) only as an env: or file: reference, never as a
// literal, and never as the same reference as source_db_url.
func TestSourceDBWriteURL(t *testing.T) {
	const secret = "s3cr3t-Wr1te"
	const rest = "      warehouse_db_url: {env: A_WH}\n      kafka_brokers: {env: A_KAFKA}\n" +
		"      connect_url: {env: A_CONNECT}\n      schema_registry_url: {env: A_SR}\n"
	cdc := func(src, write string) string {
		conns := rest + "      source_db_url: " + src + "\n"
		if write != "" {
			conns += "      source_db_write_url: " + write + "\n"
		}
		return "pipelines:\n" + pipelineYAML("orders", "    connections:\n"+conns) + sixLayers
	}
	batch := "pipelines:\n" + batchYAML("daily-revenue", "    dags: [revenue_daily]\n    connections:\n"+
		"      warehouse_db_url: {env: REV_WH}\n      source_db_write_url: {env: REV_W}\n") + "layers:\n  warehouse: meera\n"
	cases := []struct {
		name    string
		yaml    string
		wantErr string // "" = loads
	}{
		{"absent", cdc("{env: A_SRC}", ""), ""},
		{"env", cdc("{env: A_SRC}", "{env: A_SRC_WRITE}"), ""},
		{"file", cdc("{env: A_SRC}", "{file: /run/secrets/orders-write-dsn}"), ""},
		{"file next to a file source", cdc("{file: /run/secrets/src}", "{file: /run/secrets/src-write}"), ""},
		// Different forms cannot be compared without reading them: wiring
		// checks the resolved values (P1a-3c).
		{"env and file of the same name", cdc("{env: A_SRC}", "{file: /A_SRC}"), ""},
		{"plain literal", cdc("{env: A_SRC}", "'postgres://writer:"+secret+"@db/orders'"), "a plain value is refused"},
		{"value literal", cdc("{env: A_SRC}", "{value: 'postgres://writer:"+secret+"@db/orders'}"),
			"connections.source_db_write_url: value: is allowed only for non-secret settings"},
		{"same env", cdc("{env: A_SRC}", "{env: A_SRC}"), "same reference as source_db_url"},
		// Windows variable names ignore case, so this is the same variable.
		{"same env other case", cdc("{env: A_SRC}", "{env: a_src}"), "same reference as source_db_url"},
		{"same file", cdc("{file: /run/secrets/src}", "{file: /run/secrets/../secrets/src}"), "same reference as source_db_url"},
		{"relative file", cdc("{env: A_SRC}", "{file: secrets/w}"), "connections.source_db_write_url: file: must be an absolute path"},
		{"batch", batch, "connections.source_db_write_url is a cdc setting, but the pipeline kind is batch"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, c.yaml))
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, c.wantErr)
				}
				// Not even yaml.v3's 7-character prefix of a longer value.
				for _, leak := range []string{secret[:7], "postgres://", "writer:"} {
					if strings.Contains(err.Error(), leak) {
						t.Fatalf("error echoes the literal: %v", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
		})
	}
}

// The write role's reference counts as a connection reference, so the
// shared-reference rule covers it like any other secret.
func TestSourceDBWriteURLIsAConnRef(t *testing.T) {
	o, err := Load(writeTemp(t, "pipelines:\n"+pipelineYAML("orders", "    connections:\n"+
		"      source_db_url: {env: A_SRC}\n      source_db_write_url: {file: /run/secrets/W}\n"+
		"      warehouse_db_url: {env: A_WH}\n      kafka_brokers: {env: A_KAFKA}\n"+
		"      connect_url: {env: A_CONNECT}\n      schema_registry_url: {env: A_SR}\n")+sixLayers))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if refs := o.Pipelines[0].ConnRefs(); !slices.Contains(refs, "file:/run/secrets/w") {
		t.Fatalf("ConnRefs = %v, want the write role's file", refs)
	}
	if !slices.Contains(ProcessRefs(), "env:SOURCE_DB_WRITE_URL") {
		t.Fatalf("ProcessRefs = %v, want SOURCE_DB_WRITE_URL", ProcessRefs())
	}
}
