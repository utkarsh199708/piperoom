package sqlserver

import (
	"errors"
	"fmt"
)

// MaxCaptureInstanceChars is sp_cdc_enable_table's limit on @capture_instance:
// the parameter is sysname, but the procedure refuses a name over 100
// characters.
const MaxCaptureInstanceChars = 100

// ErrCaptureInstance is returned (wrapped) by CaptureInstances when a derived
// capture instance name is over MaxCaptureInstanceChars.
var ErrCaptureInstance = errors.New("sqlserver: capture instance name too long")

// CaptureInstances derives the capture instance sp_cdc_enable_table creates
// for each table of a CreatePublication(name, tables) call, in the order the
// unique tables appear: name itself for a single table, name_schema_table for
// each of several. It is the one derivation the executing writer
// (SetupWriter.CreatePublication) and the setup driver's validating writer
// (internal/setup/sources/sqlserver) share, so what the driver checks before
// any I/O is exactly what the writer creates: a duplicate table counts once,
// as the writer enables it once. name and every table are checked against
// rule 6 first; a derived name over MaxCaptureInstanceChars is
// ErrCaptureInstance.
func CaptureInstances(name string, tables []string) ([]string, error) {
	if _, err := validateIdent(name); err != nil {
		return nil, err
	}
	pairs, err := splitTables(tables)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(pairs))
	for i, p := range pairs {
		inst := captureInstance(name, len(pairs), p[0], p[1])
		if len(inst) > MaxCaptureInstanceChars {
			return nil, fmt.Errorf("%w: %q is %d characters, sp_cdc_enable_table allows %d", ErrCaptureInstance, inst, len(inst), MaxCaptureInstanceChars)
		}
		out[i] = inst
	}
	return out, nil
}
