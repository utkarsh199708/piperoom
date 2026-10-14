package loadtest

import (
	"context"
	"strconv"
	"strings"
	"time"
)

// CPUUse is the CPU time each part of the run used over a span. The parts
// share one machine, so it shows whether the server, the load driver or
// Postgres was the busy one. A value of -1 means not measured.
type CPUUse struct {
	WallS     float64 `json:"wall_s"`
	ServerS   float64 `json:"server_cpu_s"`
	DriverS   float64 `json:"driver_cpu_s"`
	PostgresS float64 `json:"postgres_cpu_s"`
}

// String renders the use as average cores busy.
func (c CPUUse) String() string {
	f := func(v float64) string {
		if v < 0 || c.WallS <= 0 {
			return "n/a"
		}
		return strconv.FormatFloat(v/c.WallS, 'f', 2, 64)
	}
	return "server " + f(c.ServerS) + ", driver " + f(c.DriverS) + ", postgres " + f(c.PostgresS) + " cores"
}

type cpuMark struct {
	at                  time.Time
	server, driver, pgs float64
}

// cpuMark reads the CPU counters of the servers, this process and Postgres.
func (e *Env) cpuMark(ctx context.Context, st *stack) cpuMark {
	m := cpuMark{at: time.Now(), server: -1, driver: processCPUSeconds(), pgs: e.PG.cpuSeconds(ctx)}
	if srv, err := st.serverStats(ctx); err == nil {
		m.server = 0
		for _, s := range srv {
			if s.CPUSeconds < 0 {
				m.server = -1
				break
			}
			m.server += s.CPUSeconds
		}
	}
	return m
}

func (a cpuMark) until(b cpuMark) CPUUse {
	d := func(x, y float64) float64 {
		if x < 0 || y < 0 {
			return -1
		}
		return round3(y - x)
	}
	return CPUUse{WallS: round3(b.at.Sub(a.at).Seconds()), ServerS: d(a.server, b.server), DriverS: d(a.driver, b.driver), PostgresS: d(a.pgs, b.pgs)}
}

// cpuSeconds is the Postgres container's CPU time from its cgroup (v1
// cpuacct.usage in ns, or v2 cpu.stat usage_usec); -1 when the server is not
// a container this run owns or the counter cannot be read.
func (p *Postgres) cpuSeconds(ctx context.Context) float64 {
	if p.container == "" {
		return -1
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := docker(cctx, "exec", p.container, "sh", "-c",
		"cat /sys/fs/cgroup/cpuacct/cpuacct.usage 2>/dev/null || grep usage_usec /sys/fs/cgroup/cpu.stat")
	if err != nil {
		return -1
	}
	return parseCgroupCPU(out)
}

// parseCgroupCPU reads either a bare nanosecond count (cgroup v1) or a
// "usage_usec N" line (cgroup v2).
func parseCgroupCPU(out string) float64 {
	out = strings.TrimSpace(out)
	if v, ok := strings.CutPrefix(out, "usage_usec"); ok {
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return -1
		}
		return float64(n) / 1e6
	}
	n, err := strconv.ParseInt(out, 10, 64)
	if err != nil {
		return -1
	}
	return float64(n) / 1e9
}
