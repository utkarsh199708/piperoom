package remoteclients

import (
	"context"

	"piperoom/internal/clients"
	"piperoom/internal/setup/protocol"
)

// Registry is the Schema Registry reader over the runner:
// clients.SchemaRegistryReader. It has no write method: a setup writes a schema
// only through a registered action, never through a client.
//
// Result shapes (the runner's OpResult.Data; each is pinned by a test):
//
//	registry_subjects       {"subjects": ["name", ...]}
//	registry_versions       {"versions": [1, 2, ...]}
//	registry_schema         {"schema": clients.SchemaVersion}
//	registry_compatibility  {"level": "BACKWARD", "subject_level": true}
type Registry struct{ s *Session }

var _ clients.SchemaRegistryReader = (*Registry)(nil)

type subjectParams struct {
	Subject string `json:"subject"`
}

type schemaParams struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// Subjects lists the registry's subjects (cluster metadata; callers scope it).
func (r *Registry) Subjects(ctx context.Context) ([]string, error) {
	res, err := r.s.do(ctx, OpRegistrySubjects, protocol.TierT0, nil)
	if err != nil {
		return nil, err
	}
	var out []string
	if err := decode(res, "subjects", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Versions lists the registered versions of subject, ascending.
func (r *Registry) Versions(ctx context.Context, subject string) ([]int, error) {
	if err := requireArg(OpRegistryVersions, "subject", subject); err != nil {
		return nil, err
	}
	res, err := r.s.do(ctx, OpRegistryVersions, protocol.TierT0, subjectParams{Subject: subject})
	if err != nil {
		return nil, err
	}
	var out []int
	if err := decode(res, "versions", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Schema returns one version of subject; version -1 means latest.
func (r *Registry) Schema(ctx context.Context, subject string, version int) (clients.SchemaVersion, error) {
	if err := requireArg(OpRegistrySchema, "subject", subject); err != nil {
		return clients.SchemaVersion{}, err
	}
	res, err := r.s.do(ctx, OpRegistrySchema, protocol.TierT0, schemaParams{Subject: subject, Version: version})
	if err != nil {
		return clients.SchemaVersion{}, err
	}
	var out clients.SchemaVersion
	if err := decode(res, "schema", &out); err != nil {
		return clients.SchemaVersion{}, err
	}
	return out, nil
}

// Compatibility returns subject's compatibility level, falling back to the
// global level; subjectLevel says which one answered.
func (r *Registry) Compatibility(ctx context.Context, subject string) (string, bool, error) {
	if err := requireArg(OpRegistryCompatibility, "subject", subject); err != nil {
		return "", false, err
	}
	res, err := r.s.do(ctx, OpRegistryCompatibility, protocol.TierT0, subjectParams{Subject: subject})
	if err != nil {
		return "", false, err
	}
	var out struct {
		Level        string `json:"level"`
		SubjectLevel bool   `json:"subject_level"`
	}
	if err := decode(res, "level", &out.Level); err != nil {
		return "", false, err
	}
	if err := decode(res, "subject_level", &out.SubjectLevel); err != nil {
		return "", false, err
	}
	return out.Level, out.SubjectLevel, nil
}
