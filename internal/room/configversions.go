package room

import "slices"

// PipelineListsConnector reports whether owners.yaml lists connector under
// the pipeline's connectors: (the config-versions routes, X5c; a connector
// only the watcher recorded is checked by the route against the store). It
// checks no visibility: the caller has answered 404 for a pipeline the
// person cannot see.
func (s *Service) PipelineListsConnector(pipelineID, connector string) bool {
	if s.owners == nil || connector == "" {
		return false
	}
	p, ok := s.owners.Pipeline(pipelineID)
	return ok && slices.Contains(p.Connectors, connector)
}
