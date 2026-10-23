package schemareg

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// DeleteSubjectCompatibility removes a subject's own compatibility level
// (DELETE /config/{subject}), so the subject follows the global level again.
// X8 uses it only on a staging Registry, to put back a subject that had no
// level of its own before a stage of schema_set_compatibility. A subject
// without a level of its own answers 404 (wraps clients.ErrNotFound). It is
// not on clients.SchemaRegistryWriter; the actions package asserts for it.
func (c *Client) DeleteSubjectCompatibility(ctx context.Context, subject string) error {
	if subject == "" {
		return fmt.Errorf("schemareg: delete compatibility: empty subject")
	}
	return c.do(ctx, http.MethodDelete, "/config/"+url.PathEscape(subject), nil, nil)
}
