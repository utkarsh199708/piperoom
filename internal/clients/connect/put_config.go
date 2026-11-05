package connect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"piperoom/internal/clients"
)

var _ clients.ConnectConfigPutter = (*Client)(nil)

// Caps on a whole config sent by PutConnectorConfig. A real connector config
// has tens of keys; these only keep a bad caller from sending a huge body.
const (
	maxPutKeys   = 512
	maxPutValLen = 64 << 10
)

// PutConnectorConfig replaces a connector's whole config with cfg
// (PUT /connectors/{name}/config). X8 uses it only on a staging Connect, to
// give a staged connector back the config it had before the stage. PUT also
// creates a connector that does not exist, so the caller checks first that
// it exists; this method cannot tell the two apart. It refuses a connector
// outside the room's pipeline and a ctx with no scope before any I/O, like
// the other writes. cfg is never logged and never appears in an error: a
// refusal from Connect is reported by its status code alone, since Connect's
// validation messages can quote a config value.
func (c *Client) PutConnectorConfig(ctx context.Context, name string, cfg map[string]string) error {
	if name == "" {
		return fmt.Errorf("connect: put config: empty connector name")
	}
	if err := checkPutConfig(name, cfg); err != nil {
		return fmt.Errorf("connect: put config %s: %w", name, err)
	}
	if err := checkWriteScope(ctx, name); err != nil {
		return err
	}
	ctx, cancel := withWriteDeadline(ctx)
	defer cancel()
	path := "/connectors/" + url.PathEscape(name) + "/config"
	err := c.doBody(ctx, http.MethodPut, path, cfg, nil)
	var se *statusError
	if errors.As(err, &se) {
		return fmt.Errorf("connect: put config %s: status %d (Connect's message is not shown: it can quote a config value)", name, se.Code)
	}
	return err
}

// checkPutConfig refuses an empty config (Connect would answer with a
// connector without a class), a "name" key that names another connector (Connect
// refuses it too; refusing here keeps the value out of any answer), an
// oversized or non-printable key, and an oversized value. Errors name keys
// only, never values.
func checkPutConfig(name string, cfg map[string]string) error {
	if len(cfg) == 0 {
		return errors.New("empty config")
	}
	if len(cfg) > maxPutKeys {
		return fmt.Errorf("%d keys; at most %d", len(cfg), maxPutKeys)
	}
	if v, ok := cfg["name"]; ok && v != name {
		return errors.New("the config's name key names another connector")
	}
	for k, v := range cfg {
		if k == "" || len(k) > maxChangeKeyLen {
			return fmt.Errorf("config key must be 1..%d bytes", maxChangeKeyLen)
		}
		for i := 0; i < len(k); i++ {
			if k[i] <= ' ' || k[i] > '~' {
				return errors.New("a config key has a byte outside printable ASCII")
			}
		}
		if len(v) > maxPutValLen {
			return fmt.Errorf("value of %s is longer than %d bytes", k, maxPutValLen)
		}
	}
	return nil
}
