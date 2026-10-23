package setuptools

import (
	"fmt"

	"piperoom/internal/setup/catalog"
	"piperoom/internal/setup/remoteclients"
)

// clientLayers is the owners layer of a remote client op, by the target kind
// it reaches (the layers of the Q tools and Q actions that send it).
var clientLayers = map[string]string{
	"connect":  "kafka-connect",
	"postgres": "postgres",
	"kafka":    "kafka",
	"registry": "schema-registry",
}

// PlanCatalogue is every op a setup plan may name: the builtin setup ops
// (catalog.Builtin) and the remote clients' ops (remoteclients.Ops), which the
// Q tools and Q actions send in a setup room under their own names
// (connect_create_connector, pg_set_replica_identity, ...). A name in both
// keeps the setup spec.
func PlanCatalogue() (*catalog.Catalogue, error) {
	cat, err := catalog.Builtin()
	if err != nil {
		return nil, err
	}
	var more []catalog.Spec
	for _, op := range remoteclients.Ops() {
		layer, ok := clientLayers[op.Target]
		if !ok {
			return nil, fmt.Errorf("setuptools: remote client op %s reaches %q, which has no layer here", op.Op, op.Target)
		}
		more = append(more, catalog.Spec{
			Name: op.Op, Tier: op.Tier, Target: op.Target, Layer: layer,
			Describe: "remote client method " + op.Method, Via: catalog.ViaClient,
		})
	}
	return cat.With(more...)
}
