package schemareg

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

func TestSchemaTyped(t *testing.T) {
	tests := []struct {
		body string
		want string
	}{
		{`{"subject":"s","version":2,"id":7,"schema":"{}"}`, "AVRO"},
		{`{"subject":"s","version":2,"id":7,"schema":"{}","schemaType":"JSON"}`, "JSON"},
		{`{"subject":"s","version":2,"id":7,"schema":"syntax = \"proto3\";","schemaType":"PROTOBUF"}`, "PROTOBUF"},
	}
	for _, tt := range tests {
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/subjects/s/versions/latest" {
				http.NotFound(w, r)
				return
			}
			io.WriteString(w, tt.body)
		})
		sv, typ, err := c.SchemaTyped(context.Background(), "s", -1)
		if err != nil {
			t.Fatal(err)
		}
		if typ != tt.want || sv.Version != 2 || sv.ID != 7 {
			t.Fatalf("got %+v %q, want type %q", sv, typ, tt.want)
		}
	}
}

func TestErrorBodySnippetCapped(t *testing.T) {
	html := "<html>" + strings.Repeat("<p>gateway error</p>", 300) + "</html>"
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		io.WriteString(w, html)
	})
	_, err := c.Subjects(context.Background())
	if err == nil || !strings.Contains(err.Error(), "status 502") {
		t.Fatalf("err = %v", err)
	}
	if n := len(err.Error()); n > maxErrorSnippet+100 {
		t.Fatalf("error is %d bytes", n)
	}

	long := strings.Repeat("m", 2000)
	c = newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"error_code":40401,"message":"`+long+`"}`)
	})
	_, err = c.Versions(context.Background(), "s")
	var serr *Error
	if !errors.Is(err, clients.ErrNotFound) || !errors.As(err, &serr) || serr.Code != 40401 {
		t.Fatalf("err = %v", err)
	}
	if len(serr.Message) > maxErrorSnippet+4 {
		t.Fatalf("message is %d bytes", len(serr.Message))
	}
}
