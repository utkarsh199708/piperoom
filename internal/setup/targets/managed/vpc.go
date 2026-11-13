package managed

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"regexp"
	"strings"

	"piperoom/internal/clients/mskconnect"
	"piperoom/internal/setup/ops"
)

var (
	accountRE = regexp.MustCompile(`^[0-9]{12}$`)
	regionRE  = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]$`)
	// roomRE is a room id as the tag value carries it.
	roomRE = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)
)

func checkVPC(p VPCCheckParams) error {
	if len(p.Subnets) == 0 || len(p.Subnets) > 16 || len(p.SecurityGroups) == 0 || len(p.SecurityGroups) > 16 {
		return fmt.Errorf("%w: 1 to 16 subnets and 1 to 16 security groups", ErrParams)
	}
	for _, s := range p.Subnets {
		if err := mskconnect.CheckSubnet(s); err != nil {
			return paramErr(err)
		}
	}
	for _, g := range p.SecurityGroups {
		if err := mskconnect.CheckSecurityGroup(g); err != nil {
			return paramErr(err)
		}
	}
	if _, err := netip.ParseAddr(p.DatabaseIP); err != nil {
		return fmt.Errorf("%w: database_ip %q is not an IP address (resolve the name first: net_probe from the runner)", ErrParams, clipText(p.DatabaseIP, 64))
	}
	if p.DatabasePort < 1 || p.DatabasePort > 65535 {
		return fmt.Errorf("%w: database_port %d", ErrParams, p.DatabasePort)
	}
	return nil
}

// Check is one named check of a verdict.
type Check struct {
	Name string `json:"name"`
	OK   bool   `json:"ok"`
	Note string `json:"note"`
}

// Verdicts of msk_vpc_check.
const (
	VerdictOK            = "ok"
	VerdictNoRoute       = "no_route"
	VerdictEgressBlocked = "egress_blocked"
)

func (d *MSKDriver) vpcCheck(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p VPCCheckParams
	_ = decode(raw, &p)
	n, err := d.api.DescribeNetwork(ctx, p.Subnets, p.SecurityGroups)
	if err != nil {
		return d.result("", errData(err)), err
	}
	ip, _ := netip.ParseAddr(p.DatabaseIP)
	checks, verdict := VPCVerdict(n, p.Subnets, p.SecurityGroups, ip, p.DatabasePort)
	data := map[string]any{"verdict": verdict, "checks": checks, "database_ip": p.DatabaseIP, "database_port": p.DatabasePort,
		"not_checked": "the database's own security group or firewall ingress, and a network ACL, are not read"}
	if verdict != VerdictOK {
		var bad []string
		for _, c := range checks {
			if !c.OK {
				bad = append(bad, c.Note)
			}
		}
		return d.result("VPC check: "+verdict+": "+clipText(strings.Join(bad, "; "), 400), data),
			fmt.Errorf("%w: %s", ErrCheckFailed, verdict)
	}
	return d.result(fmt.Sprintf("VPC check: every subnet routes to %s and egress to port %d is allowed", p.DatabaseIP, p.DatabasePort), data), nil
}

// VPCVerdict judges whether a connector in subnets with groups can reach
// ip:port: each subnet's route table (its explicit association, else its
// VPC's main table) must hold an active route whose destination contains ip,
// and one of the groups must allow egress to ip on port. A security group
// rule that names only other groups cannot be judged from here and does not
// count as allowing.
func VPCVerdict(n mskconnect.Network, subnets, groups []string, ip netip.Addr, port int) ([]Check, string) {
	var checks []Check
	verdict := VerdictOK
	vpcOf := map[string]string{}
	for _, s := range n.Subnets {
		vpcOf[s.ID] = s.VPC
	}
	for _, s := range subnets {
		table, ok := routeTableFor(n, s, vpcOf[s])
		if !ok {
			checks = append(checks, Check{Name: "route " + s, Note: "no route table found for " + s})
			verdict = VerdictNoRoute
			continue
		}
		best, bestBits := mskconnect.Route{}, -1
		for _, r := range table.Routes {
			pfx, err := netip.ParsePrefix(r.Destination)
			if err != nil || !pfx.Contains(ip) || strings.EqualFold(r.State, "blackhole") {
				continue
			}
			if pfx.Bits() > bestBits {
				best, bestBits = r, pfx.Bits()
			}
		}
		if bestBits < 0 {
			var have []string
			for _, r := range table.Routes {
				have = append(have, r.Destination+" via "+r.Target)
			}
			checks = append(checks, Check{Name: "route " + s, Note: fmt.Sprintf("no route from %s to %s in %s (routes: %s)", s, ip, table.ID, strings.Join(have, ", "))})
			verdict = VerdictNoRoute
			continue
		}
		checks = append(checks, Check{Name: "route " + s, OK: true, Note: fmt.Sprintf("%s routes %s via %s (%s)", table.ID, ip, best.Target, best.Destination)})
	}
	allowed := ""
	for _, g := range n.SecurityGroups {
		if !contains(groups, g.ID) {
			continue
		}
		for _, r := range g.Egress {
			if !portAllowed(r, port) {
				continue
			}
			for _, c := range r.CIDRs {
				if pfx, err := netip.ParsePrefix(c); err == nil && pfx.Contains(ip) {
					allowed = g.ID + " allows egress to " + c
				}
			}
		}
	}
	if allowed == "" {
		checks = append(checks, Check{Name: "egress", Note: fmt.Sprintf("no security group of %s allows egress to %s:%d by CIDR", strings.Join(groups, ", "), ip, port)})
		if verdict == VerdictOK {
			verdict = VerdictEgressBlocked
		}
	} else {
		checks = append(checks, Check{Name: "egress", OK: true, Note: allowed})
	}
	return checks, verdict
}

func routeTableFor(n mskconnect.Network, subnet, vpc string) (mskconnect.RouteTable, bool) {
	for _, t := range n.RouteTables {
		if contains(t.Subnets, subnet) {
			return t, true
		}
	}
	for _, t := range n.RouteTables {
		if t.Main && t.VPC == vpc {
			return t, true
		}
	}
	return mskconnect.RouteTable{}, false
}

func portAllowed(r mskconnect.Rule, port int) bool {
	switch r.Protocol {
	case "-1", "all":
		return true
	case "tcp", "6":
		return int(r.FromPort) <= port && port <= int(r.ToPort)
	}
	return false
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
