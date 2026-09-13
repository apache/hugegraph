# RAFT-001 independent oracle

The three-node smoke fixture writes a unique vertex, stops and restarts PD/store nodes, restarts API servers, then reads the same vertex through all API endpoints. The recovery assertion requires the persisted property value `committed`; the passing run is recorded in `raft-001-three-node-smoke-20260913-rerun.log`.
