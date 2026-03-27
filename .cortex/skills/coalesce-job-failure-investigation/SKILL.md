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
  - mcp__coalesce__patch_node_field
  - mcp__coalesce__start_run
  - mcp__coalesce__retry_run
  - mcp__coalesce__cancel_run
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
         ↓
Step 6 (optional): Verify the fix by re-running the job
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

**Ask for the workspace ID now** — before proceeding to Step 3. The workspace ID is not included in run metadata and cannot be looked up programmatically. Tell the user:
> "To inspect and fix the node I'll need your workspace ID. You can find it in the Coalesce URL when you're in the workspace: `.../workspaces/{ID}/build/...`"

Store the workspace ID for use in Steps 3 and 5.

### Step 3: Inspect the Failing Node

**Goal:** Get the full SQL and column transforms for the failing node so you can pinpoint the exact issue.

**Deciding which tool to call:**
- You already have `environment_id` from the `investigate_failure` response in Step 2 — use it directly
- Call `mcp__coalesce__get_workspace_node` with the `workspace_id` collected in Step 2 and the `node_id` from the failed node
- Do NOT call `list_environment_nodes` to try to find the node — you already have the `node_id` from `investigate_failure`

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
- The `patch_node_field` tool must be available (not hidden by `COALESCE_READONLY_MODE=true`)
- You should already have `workspace_id` from Step 2 — if somehow missing, the URL is `.../workspaces/{ID}/build/...`

**Workflow:**

1. Identify the exact `field_path` to update based on your Step 3 inspection.
   Paths use the **raw API node shape** (not the slimmed display):
   - Column transform: `"metadata.columns[N].sources[0].transform"` — find N by counting the column's position (0-based) in the `get_workspace_node` output
   - WHERE clause: `"config.whereClause"`
   - Pre/post SQL: `"config.preSQL"` / `"config.postSQL"`
   - JOIN condition: `"metadata.storageMapping[N].join"`

2. Present a clear before/after diff to the user:

```
Field:  columns[2].transforms[0]
Before: CAST(raw_value AS FLOAT)
After:  TRY_CAST(raw_value AS FLOAT)
```

3. **⚠️ STOP — wait for explicit user confirmation** ("yes", "apply it", "go ahead") before writing.

4. Once confirmed, call `mcp__coalesce__patch_node_field` with:
   - `workspace_id`
   - `node_id`
   - `field_path` — the dot/bracket path identified in step 1
   - `new_value` — the corrected SQL string

5. Report the result to the user. On success, the tool returns `old_value` and `new_value` confirming the change. Remind the user they may need to **re-run the job** to verify the fix.

**If `patch_node_field` is not available** (read-only mode):

Tell the user to make the change manually in the Coalesce UI:
1. Open the node in the workspace
2. Find the column/transform to change
3. Apply the fix
4. Deploy and re-run

### Step 6 (Optional): Verify the Fix

**Goal:** Re-run the job to confirm the fix resolves the failure.

**Prerequisites:**
- Step 5 must be complete (fix applied)
- `retry_run` tool must be available (not hidden by `COALESCE_READONLY_MODE=true`)

**⚠️ STOP:** Ask the user if they want to re-run before triggering anything.

**Workflow:**

1. Call `mcp__coalesce__retry_run` with the **original failed `run_id`** from Step 1/2.
   - This re-runs only the previously failed nodes — not a full environment refresh
   - Do NOT use `start_run` here unless the user explicitly wants a full refresh

2. Note the `new_run_id` from the response.

3. Poll `mcp__coalesce__get_run_status` with `new_run_id` until `runStatus` is `"complete"` or `"failed"`.
   - Wait a few seconds between polls — don't hammer the API
   - Tell the user what status you're seeing as you poll

4. Once complete, call `mcp__coalesce__get_run_results` with `new_run_id` to confirm results:
   - **All nodes passed** (`failed: 0`) → fix is confirmed, tell the user
   - **Still failing** → report the new error; it may be a different issue or the fix was incomplete

**If the retry itself fails to start:**
- Check that the original run_id is a valid failed run (not canceled or still running)
- If `retry_run` is unavailable, suggest `start_run` with the same `environment_id` and `job_id` as a fallback

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
**When:** Applying a full node replacement. Prefer `patch_node_field` for targeted single-field fixes.
**Params:** `workspace_id` (required), `node_id` (required), `node_config_json` (required — full node object as JSON string)

### mcp__coalesce__patch_node_field (write — may be disabled)
**When:** Applying a targeted SQL fix to a single field. Preferred over `set_node` for failure remediation — handles fetch-modify-replace internally.
**Params:** `workspace_id` (required), `node_id` (required), `field_path` (required — e.g. `"columns[0].transforms[0]"`), `new_value` (required)
**Returns:** `{success, node_name, field_path, old_value, new_value}`

### mcp__coalesce__retry_run (write — may be disabled)
**When:** After applying a fix — re-runs only the previously failed nodes from a prior run. **The primary tool for Step 6 verification.**
**Params:** `run_id` (required — the original failed run ID)
**Returns:** `{success, original_run_id, new_run_id}` — use `new_run_id` to poll status

### mcp__coalesce__start_run (write — may be disabled)
**When:** Triggering a fresh full-environment refresh (not a retry). Use this if the user wants to run the whole job from scratch rather than retry only failed nodes.
**Params:** `environment_id` (required), `job_id` (optional), `parallelism` (optional)
**Returns:** `{success, run_id}` — use `run_id` to poll status

### mcp__coalesce__cancel_run (write — may be disabled)
**When:** Aborting a run that is taking too long, was triggered by mistake, or needs to be stopped before completion.
**Params:** `run_id` (required), `environment_id` (optional)
**Returns:** `{success, run_id}`

## Stopping Points

- **Step 1:** If multiple failed runs, ask which to investigate
- **Step 3:** After presenting the failing SQL, ask if user wants to see the full node
- **Step 4:** After recommending a fix, wait for user approval before writing
- **Step 5:** Show before/after diff and require explicit user confirmation before calling `patch_node_field`
- **Step 6:** Ask the user if they want to re-run before calling `retry_run` — do not trigger a run automatically

## Troubleshooting

| Problem | Solution |
|---|---|
| `investigate_failure` returns no failed nodes | The run may have succeeded or been canceled. Check `run_status` field. Try `get_run_results` for raw data. |
| Node ID from failure isn't found in environment | The node may have been deleted or renamed since the run. Try `list_environment_nodes` to browse. |
| `set_node` or `patch_node_field` tool not available | Environment is in read-only mode (`COALESCE_READONLY_MODE=true`). Guide user to make changes in the Coalesce UI. |
| `patch_node_field` returns `Invalid field_path` | The path doesn't match the node structure. Call `get_workspace_node` to inspect the actual field names and array indices, then retry with the correct path. |
| No `workspace_id` available | Ask the user at the end of Step 2, before proceeding. It's not included in run metadata. Tell them to find it in the Coalesce URL: `.../workspaces/{ID}/build/...` |
