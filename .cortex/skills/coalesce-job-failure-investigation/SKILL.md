---
name: coalesce-job-failure-investigation
description: "Investigate and diagnose Coalesce pipeline job failures. Use when: a job failed, pipeline is broken, need to find errors, debug run failures, check what went wrong, fix failing nodes, diagnose pipeline issues. Triggers: job failed, pipeline failure, run failed, why did the job fail, what went wrong, investigate failure, debug pipeline, failing node, error in run, broken pipeline, coalesce failure."
allowed-tools:
  - mcp__coalesce__list_failed_runs
  - mcp__coalesce__list_job_runs
  - mcp__coalesce__investigate_failure
  - mcp__coalesce__get_run_results
  - mcp__coalesce__get_job_details
  - mcp__coalesce__get_run
  - mcp__coalesce__get_run_status
  - mcp__coalesce__get_environment_node
  - mcp__coalesce__get_workspace_node
  - mcp__coalesce__set_node
---

# Coalesce Job Failure Investigation

Diagnose why a Coalesce pipeline run failed, identify the root cause, inspect the failing SQL, and guide the user toward a fix.

## Workflow

```
Step 1: Find failed runs
         ↓
Step 2: Investigate the failure (root cause + downstream impact)
         ↓
Step 3: Inspect the failing node's SQL
         ↓
Step 4: Recommend a fix
         ↓
Step 5 (optional): Apply the fix if write access is available
```

### Step 1: Find Failed Runs

**Goal:** Identify which run(s) failed.

**If the user provides a run ID:** Skip to Step 2.

**If the user asks about "the latest failure" or doesn't specify a run:**

Call `mcp__coalesce__list_failed_runs`:
- `environment_id` — pass if the user specifies an environment, otherwise omit
- `limit` — default 20; use 1-5 if only looking for the most recent

**Response shape:**
```json
{
  "runs": [
    {
      "run_id": 7,
      "run_status": "failed",
      "environment_id": "22",
      "job_name": "...",
      "start_time": "2026-03-26T15:51:21.468Z",
      "end_time": "2026-03-26T15:51:35.369Z",
      "run_type": "refresh"
    }
  ],
  "next_cursor": null,
  "count": 1
}
```

Present the failed run(s) to the user with run ID, timestamp, and environment. If multiple failures exist, ask which one to investigate.

**⚠️ STOP if multiple failures:** Ask user which run to investigate before proceeding.

### Step 2: Investigate the Failure

**Goal:** Get root cause, failing nodes, error messages, and downstream impact in one call.

Call `mcp__coalesce__investigate_failure` with the `run_id`.

This is the **best single tool** for failure diagnosis. It returns:
- **Run metadata** — run_id, status, environment, timestamps, run_type
- **Failed nodes** — node name, node ID, exact error message, the SQL that failed
- **Downstream blocked nodes** — nodes that were skipped because their upstream failed

**Response shape:**
```json
{
  "run": {
    "run_id": "7",
    "run_status": "failed",
    "environment_id": "22",
    "run_type": "refresh",
    "start_time": "...",
    "end_time": "..."
  },
  "summary": {
    "total_nodes": 3,
    "failed": 1,
    "succeeded": 1,
    "downstream_blocked": 1
  },
  "failed_nodes": [
    {
      "node_id": "abc123",
      "node_name": "STG_SENSOR_READINGS",
      "error": "Numeric value '3W' is not recognized",
      "sql": "INSERT INTO ..."
    }
  ],
  "downstream_blocked_nodes": [
    {
      "node_id": "def456",
      "node_name": "DIM_SENSOR_READINGS",
      "status": "skipped"
    }
  ]
}
```

**Present to the user:**
1. **Root cause** — which node failed and why (interpret the error message)
2. **Impact** — which downstream nodes were blocked
3. **The failing SQL snippet** if available

Ask the user if they want to inspect the full node definition/SQL.

### Step 3: Inspect the Failing Node

**Goal:** Get the full SQL and column transforms for the failing node so you can pinpoint the exact issue.

**Deciding which tool to call:**
- If you have an `environment_id` from the run → call `mcp__coalesce__get_environment_node` with `environment_id` and `node_id`
- If you have a `workspace_id` → call `mcp__coalesce__get_workspace_node` with `workspace_id` and `node_id`
- If neither is known, ask the user for the workspace ID

**What to look for in the response:**
- `config.columns[].transform` — the SQL transform expression per column (e.g., `CAST(... AS FLOAT)`)
- `overrideSQL` — if `true`, look at the full custom SQL override
- `materializationType` — table, view, transient, etc.
- `config.selectDistinct`, `config.whereClause`, `config.groupByAll` — query modifiers

**Common error patterns and fixes:**

| Error Pattern | Likely Cause | Fix |
|---|---|---|
| `Numeric value 'X' is not recognized` | Hard CAST on dirty data | Use `TRY_CAST()` instead of `CAST()` |
| `NULL result in a non-nullable column` | Missing data with NOT NULL constraint | Add `COALESCE()` wrapper or relax constraint |
| `Object 'X' does not exist` | Missing source table or schema drift | Check source exists; update reference |
| `Ambiguous column name` | Join without table qualifier | Add table alias prefix |
| `Division by zero` | Unguarded division | Add `NULLIF(denominator, 0)` |
| `Timestamp 'X' is not recognized` | Date format mismatch | Use `TRY_TO_TIMESTAMP()` with format |

Present the problematic SQL/transform and recommend a specific fix.

### Step 4: Recommend a Fix

**Goal:** Give the user a concrete, actionable fix.

Structure the recommendation as:
1. **What to change** — the exact transform or SQL line
2. **Before** — current code
3. **After** — corrected code
4. **Why** — brief explanation of the fix

If the node uses column-level transforms (not `overrideSQL`), call out the specific column name and its transform expression.

Ask the user if they want you to apply the fix.

**⚠️ STOP:** Wait for user to confirm before attempting any writes.

### Step 5 (Optional): Apply the Fix

**Goal:** Update the workspace node with the corrected SQL.

**Prerequisites:**
- The `set_node` tool must be available (not hidden by `COALESCE_READONLY_MODE=true`)
- You need the `workspace_id` — ask the user if not already known

**Workflow:**

1. Call `mcp__coalesce__get_workspace_node` with `workspace_id` and `node_id` to get the **current full node definition**
2. Modify the relevant field (e.g., update a column's `transform` value)
3. Call `mcp__coalesce__set_node` with:
   - `workspace_id`
   - `node_id`
   - `node_config_json` — the **complete** modified node object as a JSON string

**IMPORTANT:** `set_node` does a **full replacement** — you must pass the entire node object, not just the changed fields. Always fetch the current node first.

**If `set_node` is not available** (read-only mode):

Tell the user to make the change manually in the Coalesce UI:
1. Open the node in the workspace
2. Find the column/transform to change
3. Apply the fix
4. Deploy and re-run

## Tools Reference

### mcp__coalesce__list_failed_runs
**When:** First step — find which runs failed.
**Params:** `environment_id` (optional), `limit` (optional, default 20), `starting_from` (optional)

### mcp__coalesce__list_job_runs
**When:** Need broader filtering (by status, environment) or want to see all runs.
**Params:** `environment_id`, `run_status` (running|completed|failed|canceled), `limit`, `starting_from`

### mcp__coalesce__investigate_failure
**When:** You have a run_id and want the full diagnosis in one call. **Best starting point for failure investigation.**
**Params:** `run_id` (required)

### mcp__coalesce__get_run_results
**When:** You want pre-processed results (failed + blocked nodes) without run metadata. Use `investigate_failure` instead unless you specifically need just results.
**Params:** `run_id` (required)

### mcp__coalesce__get_job_details
**When:** You want raw run metadata + extracted errors together. Less concise than `investigate_failure`.
**Params:** `run_id` (required)

### mcp__coalesce__get_run
**When:** You need the run object only (no node results).
**Params:** `run_id` (required)

### mcp__coalesce__get_run_status
**When:** Checking if a run is still in progress.
**Params:** `run_id` (required)

### mcp__coalesce__get_environment_node
**When:** Inspect the deployed (production) version of a node.
**Params:** `environment_id` (required), `node_id` (required)

### mcp__coalesce__get_workspace_node
**When:** Inspect the development version of a node, or before updating it.
**Params:** `workspace_id` (required), `node_id` (required)

### mcp__coalesce__set_node (write — may be disabled)
**When:** Applying a fix to a workspace node. Requires `COALESCE_READONLY_MODE=false`.
**Params:** `workspace_id` (required), `node_id` (required), `node_config_json` (required — full node object as JSON string)

## Stopping Points

- **Step 1:** If multiple failed runs, ask which to investigate
- **Step 3:** After presenting the failing SQL, ask if user wants to see the full node
- **Step 4:** After recommending a fix, wait for user approval before writing

## Troubleshooting

| Problem | Solution |
|---|---|
| `investigate_failure` returns no failed nodes | The run may have succeeded or been canceled. Check `run_status` field. Try `get_run_results` for raw data. |
| Node ID from failure isn't found in environment | The node may have been deleted or renamed since the run. Try `list_environment_nodes` to browse. |
| `set_node` tool not available | Environment is in read-only mode (`COALESCE_READONLY_MODE=true`). Guide user to make changes in the Coalesce UI. |
| No `workspace_id` available | Ask the user. It's not included in run metadata — it must be provided separately. |
