package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

func targetFor(rawURL string) events.SetupTarget {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return events.SetupTarget{Kind: "connect", Address: u.Host}
}

func TestHTTPGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"version":"7.6.0"}`)
	}))
	defer srv.Close()
	r := testRegistry(t, targetFor(srv.URL))

	res, err := runOpErr(t, r, "http_get", map[string]string{"url": srv.URL + "/"})
	if err != nil {
		t.Fatalf("http_get: %v", err)
	}
	var data httpGetData
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Status != 200 || !strings.Contains(data.Body, "7.6.0") {
		t.Fatalf("data = %+v", data)
	}
	if res.Receipt.Kind != "http" {
		t.Fatalf("receipt = %+v", res.Receipt)
	}
}

func TestHTTPGetHEAD(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("method = %s, want HEAD", r.Method)
		}
		w.WriteHeader(204)
	}))
	defer srv.Close()
	r := testRegistry(t, targetFor(srv.URL))
	if _, err := runOpErr(t, r, "http_get", map[string]string{"url": srv.URL + "/", "method": "HEAD"}); err != nil {
		t.Fatalf("http_get HEAD: %v", err)
	}
}

func TestHTTPGetNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no such connector", http.StatusNotFound)
	}))
	defer srv.Close()
	r := testRegistry(t, targetFor(srv.URL))
	res, err := runOpErr(t, r, "http_get", map[string]string{"url": srv.URL + "/connectors/x"})
	if !errors.Is(err, ErrHTTPStatus) {
		t.Fatalf("err = %v, want ErrHTTPStatus", err)
	}
	if res.OK || !strings.Contains(res.Summary, "404") {
		t.Fatalf("result = %+v", res)
	}
}

func TestHTTPGetRefusesHostOutsideTargets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	r := testRegistry(t, events.SetupTarget{Kind: "connect", Address: "127.0.0.1:1"})
	if _, err := runOpErr(t, r, "http_get", map[string]string{"url": srv.URL}); !errors.Is(err, ErrTarget) {
		t.Fatalf("err = %v, want ErrTarget", err)
	}
	r2 := testRegistry(t)
	if _, err := runOpErr(t, r2, "http_get", map[string]string{"url": srv.URL}); !errors.Is(err, ErrTarget) {
		t.Fatalf("no targets: err = %v, want ErrTarget", err)
	}
}

func TestHTTPGetURLRules(t *testing.T) {
	r := testRegistry(t, events.SetupTarget{Kind: "connect", Address: "127.0.0.1:8083"})
	cases := map[string]string{
		"userinfo": "http://user:pass@127.0.0.1:8083/",
		"scheme":   "ftp://127.0.0.1:8083/",
		"fragment": "http://127.0.0.1:8083/#x",
		"no host":  "http:///x",
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := runOpErr(t, r, "http_get", map[string]string{"url": raw})
			if !errors.Is(err, ErrParams) && !errors.Is(err, ErrTarget) {
				t.Fatalf("err = %v, want a params or target refusal", err)
			}
		})
	}
}

func TestHTTPGetOversizedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(bytesOf('a', maxHTTPBody+1024))
	}))
	defer srv.Close()
	r := testRegistry(t, targetFor(srv.URL))
	res, err := runOpErr(t, r, "http_get", map[string]string{"url": srv.URL})
	if err != nil {
		t.Fatalf("http_get: %v", err)
	}
	if !res.Truncated {
		t.Fatal("Truncated = false")
	}
	var data httpGetData
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if len(data.Body) != maxHTTPBody {
		t.Fatalf("body = %d bytes, want %d", len(data.Body), maxHTTPBody)
	}
	if len(res.Data) > protocol.MaxDataBytes {
		t.Fatalf("Data = %d bytes, cap %d", len(res.Data), protocol.MaxDataBytes)
	}
}

func TestHTTPGetRefusesRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:1/elsewhere", http.StatusFound)
	}))
	defer srv.Close()
	r := testRegistry(t, targetFor(srv.URL))
	if _, err := runOpErr(t, r, "http_get", map[string]string{"url": srv.URL}); !errors.Is(err, errRedirectRefused) {
		t.Fatalf("err = %v, want errRedirectRefused", err)
	}
}

func TestHTTPGetContextCancellation(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	r := testRegistry(t, targetFor(srv.URL))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	raw, _ := json.Marshal(map[string]string{"url": srv.URL})
	_, err := r.Execute(ctx, protocol.OpRequest{ID: "op", Op: "http_get", Params: raw, Tier: protocol.TierT0})
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want a context error", err)
	}
}

func bytesOf(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
