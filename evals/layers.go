package evals

import "piperoom/internal/events"

// incidentLayers are the layers a cause can sit in: events.Layers without
// `environment`, which is the layer of setup actions, never of an incident.
// Scenarios, learned examples, agreement labels and shadow labels all
// validate against it.
func incidentLayers() []string { return events.IncidentLayers() }
