package connprobe

import (
	"context"
	"strings"

	"piperoom/internal/owners"
)

var warehousePGChecks = []string{"config", "reach", "login", "read_only", "select_sink"}

var warehouseNames = pgNames{envVar: "WAREHOUSE_DB_URL", pipeRe: "connections.warehouse_db_url", what: "warehouse"}

// probeWarehouse tests the warehouse the way Piperoom reads it. The engine
// follows WAREHOUSE_KIND: "" or "postgres" is a Postgres warehouse, "snowflake"
// is Snowflake (warehouse_snowflake.go). It only reads.
func probeWarehouse(ctx context.Context, t Target, g *Guard) Result {
	return probeWarehouseWith(ctx, t, g, connectPG, connectSnowflake)
}

func probeWarehouseWith(ctx context.Context, t Target, g *Guard, pg pgConnector, sf sfConnector) Result {
	switch strings.ToLower(strings.TrimSpace(t.Config.WarehouseKind)) {
	case "", "postgres":
		return probeWarehousePG(ctx, t, g, pg)
	case "snowflake":
		return probeWarehouseSnowflake(ctx, t, g, sf)
	}
	return Result{Kind: KindWarehouse, Checks: skipRest([]Check{{
		ID: "config", Status: StatusFail,
		Problem: "WAREHOUSE_KIND is not a known warehouse engine",
		Fix:     "Set WAREHOUSE_KIND to postgres or snowflake.",
	}}, "not run: config failed", warehousePGChecks[1:]...)}
}

// probeWarehousePG checks a Postgres warehouse: config, reach, login, the
// read-only guard (a warning only) and SELECT on every sink table and its
// timestamp column (docs/pilot/permissions.md, "Warehouse").
func probeWarehousePG(ctx context.Context, t Target, g *Guard, connect pgConnector) Result {
	res := Result{Kind: KindWarehouse}
	o := openPG(ctx, t.Config.WarehouseDBURL, warehouseNames, g, connect)
	res.Checks = o.checks
	if !o.ok {
		res.Checks = skipRest(res.Checks, "not run: "+failedStep(o.checks)+" failed", warehousePGChecks[len(o.checks):]...)
		return res
	}
	defer o.sess.close()
	role := o.cfg.User

	res.Checks = append(res.Checks, readOnlyCheck(ctx, o.sess, role))
	res.Checks = append(res.Checks, selectCheck(ctx, o.sess, "select_sink", "sink", role, sinkItems(t.Pipelines)))
	return res
}

// sinkItems lists the distinct (sink table, timestamp column) pairs.
func sinkItems(ps []owners.Pipeline) []selectItem {
	seen := map[selectItem]bool{}
	var out []selectItem
	for _, p := range ps {
		for _, n := range p.SinkTables {
			it := selectItem{name: n, tsCol: p.SinkTSColumn}
			if !seen[it] {
				seen[it] = true
				out = append(out, it)
			}
		}
	}
	return out
}
