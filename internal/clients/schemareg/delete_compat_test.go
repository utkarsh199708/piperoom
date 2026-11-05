package schemareg

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/clients"
)

func TestDeleteSubjectCompatibility(t *testing.T) {
	var path, body, ct string
	c := compatServer(t, 200, `{"compatibilityLevel":"NONE"}`, &path, &body, &ct)
	if err := c.DeleteSubjectCompatibility(context.Background(), "a/b-value"); err != nil {
		t.Fatal(err)
	}
	if path != "DELETE /config/a%2Fb-value" || body != "" {
		t.Fatalf("request %q body %q", path, body)
	}

	c = compatServer(t, 404, `{"error_code":40408,"message":"Subject 's' does not have subject-level compatibility configured"}`, &path, &body, &ct)
	if err := c.DeleteSubjectCompatibility(context.Background(), "s"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("no subject level: %v, want ErrNotFound", err)
	}
	c = compatServer(t, 500, `{"error_code":50001,"message":"boom"}`, &path, &body, &ct)
	var se *Error
	if err := c.DeleteSubjectCompatibility(context.Background(), "s"); !errors.As(err, &se) || se.Code != 50001 {
		t.Fatalf("server error: %v", err)
	}
	path = ""
	if err := c.DeleteSubjectCompatibility(context.Background(), ""); err == nil || path != "" {
		t.Fatalf("empty subject: %v (request %q)", err, path)
	}
}
