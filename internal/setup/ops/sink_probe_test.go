package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

type sinkProbeRoundTripper func(*http.Request) (*http.Response, error)

func (f sinkProbeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newSinkProbeRegistry(t *testing.T, properties string, targets []events.SetupTarget, rt http.RoundTripper) *Registry {
	t.Helper()
	dir := t.TempDir()
	if properties != "" {
		writeFile(t, dir, serverPropertiesPath, properties)
	}
	var client *http.Client
	if rt != nil {
		client = &http.Client{Transport: rt}
	}
	r, err := New(Config{ProjectDir: dir, Targets: targets, HTTPClient: client})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func validSinkProperties(rawURL string) string {
	return fmt.Sprintf("# generated\ndebezium.sink.type=http\ndebezium.sink.http.url=%s\ndatabase.password=do-not-echo-password\n", rawURL)
}

func sinkProbeResponse(req *http.Request, status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("response body must not be read")),
		Request:    req,
	}
}

func TestSinkProbeTable(t *testing.T) {
	const secretQuery = "token=never-return-this"
	target := events.SetupTarget{Kind: "http", Address: "sink.example:8090"}
	cases := []struct {
		name       string
		roundTrip  sinkProbeRoundTripper
		wantStatus int
		wantErr    error
	}{
		{
			name: "reachable and configured",
			roundTrip: func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodHead {
					t.Errorf("method = %s, want HEAD", req.Method)
				}
				if req.Body != nil && req.Body != http.NoBody {
					t.Errorf("HEAD body = %#v, want no body", req.Body)
				}
				if req.URL.Host != target.Address {
					t.Errorf("request host = %q, want %q", req.URL.Host, target.Address)
				}
				if req.URL.RawQuery != secretQuery {
					t.Errorf("query = %q, want configured query", req.URL.RawQuery)
				}
				return sinkProbeResponse(req, http.StatusNoContent), nil
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "unreachable",
			roundTrip: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("dial failed")
			},
			wantErr: ErrSinkProbeFailed,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newSinkProbeRegistry(t, validSinkProperties("http://sink.example:8090/ingest?"+secretQuery), []events.SetupTarget{target}, tc.roundTrip)
			res, err := runOpErr(t, r, "sink_probe", map[string]string{"target": "http"})
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("sink_probe: %v", err)
			}
			if tc.wantStatus != 0 && !strings.Contains(res.Summary, fmt.Sprint(tc.wantStatus)) {
				t.Fatalf("summary = %q, want status %d", res.Summary, tc.wantStatus)
			}
			serialized := string(res.Data) + res.Summary
			if strings.Contains(serialized, secretQuery) || strings.Contains(serialized, "do-not-echo-password") {
				t.Fatalf("result exposed configuration detail: %s", serialized)
			}
			if res.Receipt.Kind != "http" || res.Receipt.Target != target.Address {
				t.Fatalf("receipt = %+v", res.Receipt)
			}
			var data sinkProbeData
			if err := json.Unmarshal(res.Data, &data); err != nil {
				t.Fatal(err)
			}
			if tc.wantStatus != 0 && (!data.Reachable || !data.Configured || data.Status != tc.wantStatus) {
				t.Fatalf("data = %+v", data)
			}
		})
	}
}

func TestSinkProbeRejectsNon2xx(t *testing.T) {
	target := events.SetupTarget{Kind: "http", Address: "sink.example:8090"}
	rt := sinkProbeRoundTripper(func(req *http.Request) (*http.Response, error) {
		return sinkProbeResponse(req, http.StatusUnauthorized), nil
	})
	r := newSinkProbeRegistry(t, validSinkProperties("http://sink.example:8090/ingest"), []events.SetupTarget{target}, rt)
	res, err := runOpErr(t, r, "sink_probe", map[string]string{"target": "http"})
	if !errors.Is(err, ErrSinkProbeFailed) {
		t.Fatalf("err = %v, want ErrSinkProbeFailed", err)
	}
	var data sinkProbeData
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if !data.Reachable || data.Configured || data.Status != http.StatusUnauthorized {
		t.Fatalf("data = %+v, want reachable but unconfigured HTTP 401", data)
	}
}
func TestSinkProbeBadConfigurationTable(t *testing.T) {
	target := events.SetupTarget{Kind: "http", Address: "sink.example:8090"}
	cases := []struct {
		name       string
		properties string
		target     string
		wantErr    error
	}{
		{
			name:    "missing properties",
			target:  "http",
			wantErr: ErrSinkProbeConfig,
		},
		{
			name:       "unsupported sink type",
			properties: "debezium.sink.type=redis\ndebezium.sink.redis.address=redis.example:6379\n",
			target:     "http",
			wantErr:    ErrSinkProbeConfig,
		},
		{
			name:       "missing HTTP URL",
			properties: "debezium.sink.type=http\n",
			target:     "http",
			wantErr:    ErrSinkProbeConfig,
		},
		{
			name:       "URL outside registered target",
			properties: validSinkProperties("http://elsewhere.example:8090/ingest"),
			target:     "http",
			wantErr:    ErrTarget,
		},
		{
			name:       "URL with credentials",
			properties: validSinkProperties("http://alice:secret@sink.example:8090/ingest"),
			target:     "http",
			wantErr:    ErrSinkProbeConfig,
		},
		{
			name:       "duplicate sink key",
			properties: "debezium.sink.type=http\ndebezium.sink.type=redis\ndebezium.sink.http.url=http://sink.example:8090/ingest\n",
			target:     "http",
			wantErr:    ErrSinkProbeConfig,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			rt := sinkProbeRoundTripper(func(*http.Request) (*http.Response, error) {
				called = true
				return sinkProbeResponse(nil, http.StatusNoContent), nil
			})
			r := newSinkProbeRegistry(t, tc.properties, []events.SetupTarget{target}, rt)
			_, err := runOpErr(t, r, "sink_probe", map[string]string{"target": tc.target})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if called {
				t.Fatal("HTTP request ran for invalid configuration")
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatalf("error leaked a credential: %v", err)
			}
		})
	}
}

func TestSinkProbeRejectsBadInput(t *testing.T) {
	r := newSinkProbeRegistry(t, validSinkProperties("http://sink.example:8090/ingest"), []events.SetupTarget{{Kind: "http", Address: "sink.example:8090"}}, nil)
	for _, tc := range []struct {
		name   string
		params map[string]string
		want   error
	}{
		{name: "missing target", params: map[string]string{}, want: ErrParams},
		{name: "unregistered target", params: map[string]string{"target": "postgres"}, want: ErrTarget},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runOpErr(t, r, "sink_probe", tc.params); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSinkProbeTimeout(t *testing.T) {
	rt := sinkProbeRoundTripper(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	r := newSinkProbeRegistry(t, validSinkProperties("http://sink.example:8090/ingest"), []events.SetupTarget{{Kind: "http", Address: "sink.example:8090"}}, rt)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	params, _ := json.Marshal(map[string]string{"target": "http"})
	res, err := r.Execute(ctx, protocol.OpRequest{ID: "op", Op: "sink_probe", Params: params, Tier: protocol.TierT0})
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrSinkProbeFailed) {
		t.Fatalf("err = %v, want sink probe deadline failure", err)
	}
	if res.OK || !strings.Contains(res.Summary, "request failed") {
		t.Fatalf("result = %+v", res)
	}
}

func TestSinkProbeRefusesMetadataAddress(t *testing.T) {
	r, err := New(Config{
		ProjectDir: t.TempDir(),
		Targets:    []events.SetupTarget{{Kind: "http", Address: "169.254.169.254:80"}},
		HTTPClient: &http.Client{Transport: sinkProbeRoundTripper(func(*http.Request) (*http.Response, error) {
			t.Fatal("request reached a client for a metadata address")
			return nil, errors.New("unexpected request")
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	writeFile(t, r.env.ProjectDir, serverPropertiesPath, validSinkProperties("http://169.254.169.254/ingest"))
	_, err = runOpErr(t, r, "sink_probe", map[string]string{"target": "http"})
	if !errors.Is(err, ErrTarget) {
		t.Fatalf("err = %v, want ErrTarget", err)
	}
}
