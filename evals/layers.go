package evals

import (
	"fmt"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// incidentLayers are the layers a cause can sit in: events.Layers without
// `environment`, which is the layer of setup actions, never of an incident.
// Scenarios, learned examples, agreement labels and shadow labels all
// validate against it.
func incidentLayers() []string { return events.IncidentLayers() }

// rootCauseLayers are the layers a hypothesis may place the root cause in:
// RootCauseLayer first, then RootCauseLayerAlt. Empty when the scenario
// grades no root-cause layer.
func (e Expect) rootCauseLayers() []string {
	if e.RootCauseLayer == "" {
		return nil
	}
	return append([]string{e.RootCauseLayer}, e.RootCauseLayerAlt...)
}

// checkRootCauseLayerAlt validates root_cause_layer_alt: every entry is an
// incident layer, named once, distinct from root_cause_layer (which must be
// set) and never first_failing_layer. An alternative equal to the first
// failing layer would collapse the root-cause grade into the first-failing
// grade, the very distinction the field exists for.
func (e Expect) checkRootCauseLayerAlt() []error {
	if len(e.RootCauseLayerAlt) == 0 {
		return nil
	}
	var errs []error
	if e.RootCauseLayer == "" {
		errs = append(errs, fmt.Errorf("expect.root_cause_layer_alt: needs expect.root_cause_layer"))
	}
	seen := map[string]bool{}
	for i, l := range e.RootCauseLayerAlt {
		field := fmt.Sprintf("expect.root_cause_layer_alt[%d]", i)
		switch {
		case !contains(incidentLayers(), l):
			errs = append(errs, fmt.Errorf("%s: unknown layer %q (want one of %s)", field, l, strings.Join(incidentLayers(), ", ")))
		case l == e.RootCauseLayer:
			errs = append(errs, fmt.Errorf("%s: %q duplicates expect.root_cause_layer", field, l))
		case l == e.FirstFailingLayer:
			errs = append(errs, fmt.Errorf("%s: %q equals expect.first_failing_layer; an alternative root-cause layer must differ from the first failing layer", field, l))
		case seen[l]:
			errs = append(errs, fmt.Errorf("%s: %q repeated", field, l))
		}
		seen[l] = true
	}
	return errs
}

// namesAnyLayer reports whether text names any of layers (undenied), the
// root-cause-layer grade when a scenario sets root_cause_layer_alt.
func namesAnyLayer(p owners.Pipeline, layers []string, text string) bool {
	for _, l := range layers {
		if namesLayer(layerTermsFor(p, l), text) {
			return true
		}
	}
	return false
}
