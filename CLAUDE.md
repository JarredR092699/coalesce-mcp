# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

An MCP (Model Context Protocol) server that gives Claude read/write access to [Coalesce](https://coalesce.io) — a Snowflake-native data transformation platform. The primary use case is **failure investigation**: ask Claude to diagnose why a pipeline run failed without leaving the chat.

**Package name:** `coalesce-mcp` | **PyPI:** `pip install coalesce-mcp` | **Version:** 0.2.0

---

## Development Commands

```bash
# Install dev dependencies
uv sync

# Run locally
COALESCE_API_TOKEN=xxx uv run coalesce-mcp-server

# Build
uv build

# Publish
uv publish
```

Python 3.10+ required. Key deps: `mcp>=1.0.0`, `httpx>=0.27.0`.

---

## Architecture

**Two files that matter:**
- `server.py` — declares MCP tools (names, descriptions, JSON schemas) and routes `call_tool` invocations to client functions
- `client.py` — `CoalesceClient` class wraps `httpx.AsyncClient`; standalone async functions are the actual MCP tool implementations

**Two-layer tool pattern:**
1. `CoalesceClient.method()` — raw HTTP call, returns `dict`, lets exceptions propagate
2. `tool_function()` — MCP-facing, calls client, formats/filters, returns `str` (JSON); catches `httpx.HTTPStatusError` and returns JSON error objects

**Client singleton:** `get_client()` in `client.py` returns a module-level `CoalesceClient`. The `httpx.AsyncClient` inside is lazily initialized and reused.

**Server routing:** `call_tool(name, arguments)` → appropriate MCP tool function → `[TextContent(type="text", text=result)]`

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `COALESCE_API_TOKEN` | (required) | Bearer token |
| `COALESCE_BASE_URL` | `https://app.coalescesoftware.io/api` | Override for on-prem |
| `COALESCE_READONLY_MODE` | `false` | Set `true` to hide `create_workspace_node` and `set_node` tools |

`COALESCE_READONLY_MODE` is used in the Snowflake Cortex CLI integration — the agent has a `DATAENG_READ_ONLY` Snowflake role and the readonly mode prevents write tool exposure.

Claude Desktop config snippet:
```json
{
  "mcpServers": {
    "coalesce": {
      "command": "uvx",
      "args": ["--from", "coalesce-mcp", "coalesce-mcp-server"],
      "env": {
        "COALESCE_API_TOKEN": "your-token-here"
      }
    }
  }
}
```

---

## API Endpoints

All calls go to `COALESCE_BASE_URL`. Auth is `Authorization: Bearer <token>`.

### Job Runs (read-only)
| Method | Path | Purpose |
|---|---|---|
| GET | `/v1/runs` | List runs; params: `environmentID`, `runStatus`, `limit`, `startingFrom`, `orderBy` |
| GET | `/v1/runs/{runID}` | Single run details |
| GET | `/scheduler/runStatus?runID={id}` | Live status (different base path than run details) |
| GET | `/v1/runs/{runID}/results` | Node-level execution results — flat dict keyed by node ID |

### Node Management (read + write)
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/environments/{envID}/nodes` | All deployed nodes |
| GET | `/api/v1/workspaces/{wsID}/nodes` | All workspace nodes |
| GET/PUT | `/api/v1/workspaces/{wsID}/nodes/{nodeID}` | Get or full-replace workspace node |
| GET | `/api/v1/environments/{envID}/nodes/{nodeID}` | Single environment node |
| POST | `/api/v1/workspaces/{wsID}/nodes` | Create node |

---

## MCP Tools Exposed

### Failure Investigation
| Tool | Purpose |
|---|---|
| `list_job_runs` | List runs with optional filters |
| `list_failed_runs` | Shortcut: failed runs only |
| `get_run` | Full run object |
| `get_run_status` | Live status via scheduler endpoint |
| `get_run_results` | Pre-processed: failed nodes + blocked downstream + summary stats |
| `get_job_details` | Combined: run info + status + full results + extracted errors |
| `investigate_failure` | **Best for diagnosis:** run metadata + failures + downstream impact |

### Node Management
| Tool | Purpose |
|---|---|
| `list_environment_nodes` / `list_workspace_nodes` | List all nodes |
| `get_workspace_node` / `get_environment_node` | Full node config + SQL |
| `create_workspace_node` | Create with defaults |
| `set_node` | Full replacement update (read current first!) |

**Recommended investigation flow:**
1. `list_failed_runs` → find run_id
2. `investigate_failure` → root cause + downstream impact
3. `get_workspace_node` → inspect SQL of failing node

---

## Key Data Shapes

### Run results (`/v1/runs/{runID}/results`)
Flat dict keyed by node ID. Possible `status` values: `success`, `failed`, `skipped`, `canceled`, `running`.
```json
{
  "node-id-1": {
    "status": "failed",
    "nodeName": "dim_customer",
    "errorMessage": "SQL compilation error...",
    "stage": "transform",
    "sql": "CREATE TABLE ...",
    "durationSeconds": 12.5
  }
}
```

Some results include `predecessorNodeIDs`/`predecessors` fields enabling exact downstream tracing via BFS; without them, `_trace_downstream` falls back to treating `skipped`/`canceled` nodes as likely-downstream.

---

## Known Bugs (from 2026-03-18 testing)

1. ~~**`get_job_details` JSON parse error**~~ — **Fixed (2026-03-19).** Root cause: `CoalesceClient.get_run_status` and `CoalesceClient.get_run_results` called `response.json()` with no empty-body guard. A 2xx response with an empty body throws `json.JSONDecodeError`, which the MCP-level `except httpx.HTTPStatusError` clauses don't catch, crashing the tool call. Fixed by adding `if not response.content: return {}` to both methods, matching the pattern already used in `list_runs`.

2. ~~**`get_run_results` may return all nodes**~~ — **Fixed.** `_classify_nodes` already checks both `node.get("status") or node.get("runState")`, so API field name differences are handled. `_format_blocked_node` was also updated (2026-03-19) to apply the same fallback so `downstream_blocked_nodes[].status` reflects the actual state rather than `null`.

3. **`investigate_failure` field name issue** — **Fixed** (same fix as #2). The `_classify_nodes` pipeline correctly handles both `status` and `runState`. The remaining gap (`_format_blocked_node` showing `null` status for blocked nodes) was fixed 2026-03-19.

**Debugging approach:** Test with a definitively failed run, log the raw `results_data` from `client.get_run_results()` to verify the actual field names before assuming the filtering works.

---

## Other Known Issues

- `tests/` directory is empty — no automated tests
- `__init__.py` shows version `0.1.3` but `pyproject.toml` says `0.2.0` (version drift)
- No retry logic on HTTP errors
- `CoalesceClient` is never explicitly closed (no shutdown hook)
- `logging.getLogger(__name__)` is called inline inside methods rather than at module level
