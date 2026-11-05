package openai

import (
	"fmt"
	"strings"

	"piperoom/internal/clients/netsec"
)

// maxKeyFileBytes bounds PIPEROOM_MODEL_API_KEY_FILE; a key is far smaller.
const maxKeyFileBytes = 64 << 10

// LoadKey reads the key of a role on flavor (design 9): for the main role
// PIPEROOM_MODEL_API_KEY, else the file PIPEROOM_MODEL_API_KEY_FILE names,
// else the flavor's standard variable (KeyEnv); a role on another provider
// than the main one (main false) reads only the standard variable, never
// the main provider's key. getenv is os.Getenv and readFile os.ReadFile in
// production. An empty result is not an error here: New refuses it on a
// flavor that needs a key. The error names the variable, never the value.
func LoadKey(flavorName string, main bool, getenv func(string) string, readFile func(string) ([]byte, error)) (netsec.Secret, error) {
	if main {
		if k := strings.TrimSpace(getenv("PIPEROOM_MODEL_API_KEY")); k != "" {
			return netsec.Secret(k), nil
		}
		if path := strings.TrimSpace(getenv("PIPEROOM_MODEL_API_KEY_FILE")); path != "" {
			b, err := readFile(path)
			if err != nil {
				return "", fmt.Errorf("llm/openai: PIPEROOM_MODEL_API_KEY_FILE: cannot read the file")
			}
			if len(b) > maxKeyFileBytes {
				return "", fmt.Errorf("llm/openai: PIPEROOM_MODEL_API_KEY_FILE: larger than %d bytes", maxKeyFileBytes)
			}
			k := strings.TrimSpace(string(b))
			if k == "" {
				return "", fmt.Errorf("llm/openai: PIPEROOM_MODEL_API_KEY_FILE: the file is empty")
			}
			return netsec.Secret(k), nil
		}
	}
	env := KeyEnv(flavorName)
	if env == "" || (!main && env == "PIPEROOM_MODEL_API_KEY") {
		return "", nil
	}
	return netsec.Secret(strings.TrimSpace(getenv(env))), nil
}
