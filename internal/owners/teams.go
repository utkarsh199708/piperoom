package owners

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Teams (X4, docs/api.md "owners.yaml teams:"): the application teams that own
// the services outside the pipeline. The agent names one in a hypothesis whose
// cause is upstream (agent.Alert.Teams); the room pages its Slack channel only
// when the driver asks (X4d).

// teamIDRE is the teams[].id charset of docs/api.md.
var teamIDRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// slackChannelIDRE is a Slack channel id (public C…, private G…), never a
// channel name: a name can be renamed under the page, an id cannot.
var slackChannelIDRE = regexp.MustCompile(`^[CG][A-Z0-9]{6,23}$`)

// maxServiceBytes is the cap of one owns: entry, the same as the webhook's
// service field (clients.MaxExternalChangeServiceBytes): a longer name could
// never match a change.
const maxServiceBytes = 128

// Team is one entry of the top-level teams: list.
type Team struct {
	// ID is unique across the file and matches teamIDRE.
	ID string `yaml:"id"`
	// SlackChannel is the Slack channel id the room pages (X4d).
	SlackChannel string `yaml:"slack_channel"`
	// Owns lists the service names the team owns, as the change webhook's
	// service field sends them. A service is owned by one team only.
	Owns []string `yaml:"owns"`
	// Tenant is the tenant the team serves: required with a tenants: block,
	// refused without one. Read it with TenantName.
	Tenant string `yaml:"tenant"`
}

// TenantName returns the team's tenant: its tenant: value, or DefaultTenant.
func (t Team) TenantName() string {
	if t.Tenant == "" {
		return DefaultTenant
	}
	return t.Tenant
}

// validateTeams checks the teams: list: ids unique and well formed, a Slack
// channel id, at least one owned service, each service owned by one team, and
// the tenant rule. Values that are not plain names are never quoted.
func (o *Owners) validateTeams() error {
	ids := map[string]bool{}
	owner := map[string]string{} // service -> team id
	for i, t := range o.Teams {
		if !teamIDRE.MatchString(t.ID) {
			return fmt.Errorf("teams[%d]: id %s must match %s", i, quoteName(t.ID), teamIDRE)
		}
		if ids[t.ID] {
			return fmt.Errorf("teams: id %q is listed twice", t.ID)
		}
		ids[t.ID] = true
		if !slackChannelIDRE.MatchString(t.SlackChannel) {
			return fmt.Errorf("team %q: slack_channel must be a Slack channel id such as C0123ABCD (not a #name)", t.ID)
		}
		if len(t.Owns) == 0 {
			return fmt.Errorf("team %q: owns: lists no service", t.ID)
		}
		for _, s := range t.Owns {
			if s == "" || s != strings.TrimSpace(s) || len(s) > maxServiceBytes || !utf8.ValidString(s) || strings.ContainsFunc(s, unicode.IsControl) {
				return fmt.Errorf("team %q: owns: entry %s must be a service name of 1 to %d bytes without surrounding space or control characters", t.ID, quoteName(s), maxServiceBytes)
			}
			if prev, ok := owner[s]; ok {
				if prev == t.ID {
					return fmt.Errorf("team %q: owns: lists %s twice", t.ID, quoteName(s))
				}
				return fmt.Errorf("teams: service %s is owned by both %q and %q; a service has one owning team", quoteName(s), prev, t.ID)
			}
			owner[s] = t.ID
		}
		switch {
		case o.TenancyOn() && t.Tenant == "":
			return fmt.Errorf("team %q: tenant: is required with a tenants: block (a team serves one tenant)", t.ID)
		case o.TenancyOn():
			if _, ok := o.Tenants[t.Tenant]; !ok {
				return fmt.Errorf("team %q: tenant %s is not declared in tenants: (declared: %s)", t.ID, quoteName(t.Tenant), strings.Join(o.TenantNames(), ", "))
			}
		case t.Tenant != "":
			return fmt.Errorf("team %q: tenant %s needs a tenants: block that declares it", t.ID, quoteName(t.Tenant))
		}
	}
	return o.validatePipelineTeams()
}

// validatePipelineTeams checks each pipeline's optional team: (X5-0). With a
// teams: list the id must be listed there for the pipeline's tenant, so the
// fleet's ?team= filter never names a team of another tenant; without one
// the label only has to look like a team id.
func (o *Owners) validatePipelineTeams() error {
	for _, p := range o.Pipelines {
		if p.Team == "" {
			continue
		}
		if !teamIDRE.MatchString(p.Team) {
			return fmt.Errorf("pipeline %q: team %s must match %s", p.ID, quoteName(p.Team), teamIDRE)
		}
		if len(o.Teams) == 0 {
			continue
		}
		i := slices.IndexFunc(o.Teams, func(t Team) bool { return t.ID == p.Team })
		if i < 0 {
			return fmt.Errorf("pipeline %q: team %q is not declared in teams:", p.ID, p.Team)
		}
		if t := o.Teams[i]; t.TenantName() != p.TenantName() {
			return fmt.Errorf("pipeline %q: team %q serves tenant %q, not the pipeline's tenant %q", p.ID, p.Team, t.TenantName(), p.TenantName())
		}
	}
	return nil
}

// TeamsFor returns the teams a room of the pipeline may name: the teams of
// the pipeline's tenant (every team without a tenants: block), in file order,
// as copies. nil for a pipeline owners.yaml does not define, or no teams.
func (o *Owners) TeamsFor(pipelineID string) []Team {
	if o == nil || len(o.Teams) == 0 {
		return nil
	}
	p, ok := o.Pipeline(pipelineID)
	if !ok {
		return nil
	}
	var out []Team
	for _, t := range o.Teams {
		if t.TenantName() == p.TenantName() {
			t.Owns = slices.Clone(t.Owns)
			out = append(out, t)
		}
	}
	return out
}
