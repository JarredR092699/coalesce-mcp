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
The actual API response shape from `/v1/runs/{runID}/results` is a **list wrapped in `{"data": [...]}`**, not a flat dict. Each node object looks like:

```json
{
  "nodeID": "abc123",
  "runState": "error",
  "name": "dim_customer",
  "queryResults": [
    {
      "status": "Success",
      "success": true,
      "sql": "...",
      "type": "sql"
    },
    {
      "status": "Failure",
      "success": false,
      "error": {
        "errorString": "Numeric value '3W' is not recognized",
        "errorDetail": "000627"
      },
      "sql": "INSERT INTO ...",
      "type": "sql"
    }
  ]
}
```

Key field mapping:
- `runState`: `"complete"` / `"error"` / `"skipped"` (not `"success"/"failed"`)
- Errors are nested inside `queryResults[].error.errorString` (not top-level `errorMessage`)
- SQL is in `queryResults[].sql` on the failed query step

`_classify_nodes` handles both the old and new field shapes. `_extract_query_results_error` walks `queryResults` to find the first failed step and extracts its error and SQL.

Some results include `predecessorNodeIDs`/`predecessors` fields enabling exact downstream tracing via BFS; without them, `_trace_downstream` falls back to treating `skipped`/`canceled` nodes as likely-downstream.

---

## Bug History

1. ~~**`get_job_details` JSON parse error**~~ — **Fixed (2026-03-19).** Empty body guard added to `get_run_status` and `get_run_results`.

2. ~~**Failed nodes not detected (`failed: 0`)**~~ — **Fixed (2026-03-20).** `_classify_nodes` now recognizes `runState: "error"` and `runState: "complete"`. `_extract_query_results_error` added to pull errors from `queryResults[].error.errorString`.

3. ~~**Failed nodes leaking into `downstream_blocked_nodes`**~~ — **Fixed (2026-03-20).** `all_blocked_ids` now excludes `failed_ids`.

4. ~~**Double `/api/api/` in URL**~~ — **Fixed.** `base_url` always ends with `/` and all request paths are relative (no leading slash).

---

## Other Known Issues

- No retry logic on HTTP errors
- `CoalesceClient` is never explicitly closed (no shutdown hook)
- `logging.getLogger(__name__)` is called inline inside `list_runs` rather than at module level
