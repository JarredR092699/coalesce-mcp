"""Coalesce API tool implementations for job run endpoints.

Based on Coalesce API documentation:
- https://docs.coalesce.io/docs/api/coalesce/get-runs
- https://docs.coalesce.io/docs/api/runs/run-status

All endpoints are READ-ONLY. No mutation operations are included.
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Any

import httpx


class CoalesceClient:
    """HTTP client for Coalesce API."""

    def __init__(self):
        # Get config from environment (set by MCP server launcher)
        self.base_url = os.getenv("COALESCE_BASE_URL", "https://app.coalescesoftware.io/api").rstrip("/")
        self.token = os.getenv("COALESCE_API_TOKEN", "")
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=30.0,
            )
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    # =========================================================================
    # Job Run Endpoints (READ-ONLY)
    # =========================================================================

    async def list_runs(
        self,
        environment_id: str | None = None,
        run_status: str | None = None,
        limit: int = 50,
        starting_from: str | None = None,
        order_by: str = "id",
        order_by_direction: str = "desc",
    ) -> dict[str, Any]:
        """
        List job runs from Coalesce.

        Endpoint: GET /v1/runs

        Args:
            environment_id: Filter by environment ID (optional)
            run_status: Filter by runStatus: 'running', 'completed', 'failed', 'canceled' (optional)
            limit: Maximum number of runs to return (default 50)
            starting_from: Cursor for pagination (from previous response's 'next' field)
            order_by: Field to sort by (default 'id')
            order_by_direction: Sort direction 'asc' or 'desc' (default 'desc')

        Returns:
            Dict with 'data' (list of runs) and 'next' (cursor for next page)
        """
        client = await self._get_client()

        params: dict[str, Any] = {
            "limit": limit,
            "orderBy": order_by,
            "orderByDirection": order_by_direction,
        }
        if environment_id:
            params["environmentID"] = environment_id
        if run_status:
            params["runStatus"] = run_status
        if starting_from:
            params["startingFrom"] = starting_from

        import logging
        logger = logging.getLogger(__name__)

        full_url = f"{client.base_url}/v1/runs"
        logger.info(f"Calling Coalesce API: {full_url} with params: {params}")

        response = await client.get("/v1/runs", params=params)

        logger.info(f"Response status: {response.status_code}")
        logger.debug(f"Response headers: {response.headers}")

        try:
            response.raise_for_status()
        except Exception as e:
            logger.error(f"API Error. Status: {response.status_code}, Body: {response.text[:1000]}")
            raise

        # Debug: Check if response has content
        if not response.content:
            return {"data": [], "next": None}

        try:
            data = response.json()
        except Exception as e:
            # Log the raw response for debugging
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to parse JSON. Response text: {response.text[:500]}")
            raise

        # Handle both array and object responses
        # Return structure: {"data": [...], "next": "cursor" or null}
        if isinstance(data, list):
            return {"data": data, "next": None}
        return {
            "data": data.get("data", data.get("runs", [])),
            "next": data.get("next"),
        }

    async def get_run(self, run_id: str) -> dict[str, Any]:
        """
        Get details for a specific run by fetching from list endpoint.

        The Coalesce API doesn't have a dedicated single-run endpoint.
        We use list_runs filtered to find the specific run.

        Args:
            run_id: The run ID to retrieve

        Returns:
            Run object with details, or empty dict if not found
        """
        # Use list endpoint and filter - single run endpoint doesn't exist
        result = await self.list_runs(limit=1, starting_from=str(int(run_id) + 1))
        for run in result.get("data", []):
            if str(run.get("id") or run.get("runID")) == str(run_id):
                return run
        
        # Fallback: try scheduler endpoint for status
        try:
            return await self.get_run_status(run_id)
        except Exception:
            return {"id": run_id, "error": "Run not found"}

    async def get_run_status(self, run_id: str) -> dict[str, Any]:
        """
        Get status for a specific run.

        Endpoint: GET /scheduler/runStatus?runID={runID}

        Args:
            run_id: The run ID to check

        Returns:
            Run status object
        """
        client = await self._get_client()

        response = await client.get("/scheduler/runStatus", params={"runID": run_id})
        response.raise_for_status()

        return response.json()

    async def get_run_results(self, run_id: str) -> dict[str, Any]:
        """
        Get detailed results for a run, including node-level status and errors.

        Endpoint: GET /v1/runs/{runID}/results

        Args:
            run_id: The run ID to get results for

        Returns:
            Run results organized by nodeID with status, errors, SQL executed, etc.
        """
        client = await self._get_client()

        response = await client.get(f"/v1/runs/{run_id}/results")
        response.raise_for_status()

        return response.json()

    # =========================================================================
    # Node Management Endpoints
    # =========================================================================

    async def list_environment_nodes(self, environment_id: str) -> dict[str, Any]:
        """
        List all deployed nodes in an environment.

        Endpoint: GET /api/v1/environments/{environmentID}/nodes

        Args:
            environment_id: The environment ID to list nodes from

        Returns:
            Dict with array of node objects
        """
        client = await self._get_client()
        response = await client.get(f"/api/v1/environments/{environment_id}/nodes")
        response.raise_for_status()
        return response.json()

    async def list_workspace_nodes(self, workspace_id: str) -> dict[str, Any]:
        """
        List all development nodes in a workspace.

        Endpoint: GET /api/v1/workspaces/{workspaceID}/nodes

        Args:
            workspace_id: The workspace ID to list nodes from

        Returns:
            Dict with array of node objects
        """
        client = await self._get_client()
        response = await client.get(f"/api/v1/workspaces/{workspace_id}/nodes")
        response.raise_for_status()
        return response.json()

    async def get_workspace_node(self, workspace_id: str, node_id: str) -> dict[str, Any]:
        """
        Get complete details for a specific workspace node.

        Endpoint: GET /api/v1/workspaces/{workspaceID}/nodes/{nodeID}

        Args:
            workspace_id: The workspace ID containing the node
            node_id: The node ID to retrieve

        Returns:
            Complete node object with metadata
        """
        client = await self._get_client()
        response = await client.get(f"/api/v1/workspaces/{workspace_id}/nodes/{node_id}")
        response.raise_for_status()
        return response.json()

    async def get_environment_node(self, environment_id: str, node_id: str) -> dict[str, Any]:
        """
        Get complete details for a specific environment node.

        Endpoint: GET /api/v1/environments/{environmentID}/nodes/{nodeID}

        Args:
            environment_id: The environment ID containing the node
            node_id: The node ID to retrieve

        Returns:
            Complete node object with metadata
        """
        client = await self._get_client()
        response = await client.get(f"/api/v1/environments/{environment_id}/nodes/{node_id}")
        response.raise_for_status()
        return response.json()

    async def create_workspace_node(
        self,
        workspace_id: str,
        node_type: str,
        predecessor_node_ids: list[str]
    ) -> dict[str, Any]:
        """
        Create a new node in a workspace with default settings.

        Endpoint: POST /api/v1/workspaces/{workspaceID}/nodes

        Args:
            workspace_id: The workspace ID to create the node in
            node_type: Node type (e.g., 'Stage', 'Fact', 'Dimension', or custom nodeTypeID)
            predecessor_node_ids: Array of predecessor node IDs (use [] for source nodes)

        Returns:
            Created node object with generated ID
        """
        client = await self._get_client()
        response = await client.post(
            f"/api/v1/workspaces/{workspace_id}/nodes",
            json={
                "nodeType": node_type,
                "predecessorNodeIDs": predecessor_node_ids,
            }
        )
        response.raise_for_status()
        return response.json()

    async def set_node(
        self,
        workspace_id: str,
        node_id: str,
        node_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Update (full replacement) an existing node.

        Endpoint: PUT /api/v1/workspaces/{workspaceID}/nodes/{nodeID}

        Args:
            workspace_id: The workspace ID containing the node
            node_id: The node ID to update
            node_data: Complete node object with all required fields

        Returns:
            Updated node object
        """
        client = await self._get_client()
        response = await client.put(
            f"/api/v1/workspaces/{workspace_id}/nodes/{node_id}",
            json=node_data
        )
        response.raise_for_status()
        return response.json()


# Global client instance
_client: CoalesceClient | None = None


def get_client() -> CoalesceClient:
    global _client
    if _client is None:
        _client = CoalesceClient()
    return _client


# =============================================================================
# MCP Tool Functions
# =============================================================================

async def list_job_runs(
    environment_id: str | None = None,
    run_status: str | None = None,
    limit: int = 50,
    starting_from: str | None = None,
) -> str:
    """
    List recent job runs from Coalesce.

    Use this tool to:
    - Check the status of recent pipeline jobs
    - Identify failed jobs that need investigation
    - Monitor job execution history
    - Find runs by environment or runStatus

    Args:
        environment_id: Filter by environment ID (optional)
        run_status: Filter by runStatus - 'running', 'completed', 'failed', 'canceled' (optional)
        limit: Maximum number of runs to return (default 50)
        starting_from: Cursor for next page from previous response (optional)

    Returns:
        JSON object with 'runs' array and 'next_cursor' for pagination
    """
    client = get_client()
    result = await client.list_runs(
        environment_id=environment_id,
        run_status=run_status,
        limit=limit,
        starting_from=starting_from,
    )

    # Format for readability
    formatted_runs = []
    for run in result.get("data", []):
        formatted_runs.append({
            "run_id": run.get("id") or run.get("runID"),
            "run_status": run.get("runStatus") or run.get("status"),  # Try runStatus first, fallback to status
            "environment_id": run.get("environmentID") or run.get("environment"),
            "job_name": run.get("jobName") or run.get("name"),
            "start_time": run.get("runStartTime") or run.get("startTime"),
            "end_time": run.get("runEndTime") or run.get("endTime"),
            "run_type": run.get("runType"),
            "triggered_by": run.get("triggeredBy"),
        })

    return json.dumps({
        "runs": formatted_runs,
        "next_cursor": result.get("next"),
        "count": len(formatted_runs),
    }, indent=2, default=str)


async def get_run(run_id: str) -> str:
    """
    Get details for a specific job run.

    Use this tool to:
    - Get full details about a specific run
    - See run configuration and parameters
    - Check when a run started and ended

    Args:
        run_id: The ID of the run to retrieve

    Returns:
        JSON object with full run details
    """
    client = get_client()

    try:
        run = await client.get_run(run_id)
        return json.dumps(run, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"Failed to get run: {e.response.status_code}",
            "run_id": run_id,
        }, indent=2)


async def get_run_status(run_id: str) -> str:
    """
    Get the current status of a job run.

    Use this tool to:
    - Check if a run is still in progress
    - See the current execution status
    - Monitor long-running jobs

    Args:
        run_id: The ID of the run to check

    Returns:
        JSON object with run status information
    """
    client = get_client()

    try:
        status = await client.get_run_status(run_id)
        return json.dumps(status, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"Failed to get run status: {e.response.status_code}",
            "run_id": run_id,
        }, indent=2)


def _parse_results_to_node_map(results_data: Any) -> dict[str, dict]:
    """
    Normalize the run results API response into a flat {node_id: node_dict} map.

    The Coalesce API can return results as:
    - A flat dict keyed by node ID (most common)
    - A wrapped object like {"nodes": {...}} or {"data": [...]}
    - A list of node objects with an "id"/"nodeID" field
    """
    if isinstance(results_data, dict):
        # Check for wrapper keys
        if "nodes" in results_data and isinstance(results_data["nodes"], dict):
            return results_data["nodes"]
        if "data" in results_data and isinstance(results_data["data"], list):
            return {
                (n.get("nodeID") or n.get("id")): n
                for n in results_data["data"]
                if isinstance(n, dict) and (n.get("nodeID") or n.get("id"))
            }
        # Assume flat dict keyed by node ID
        return {k: v for k, v in results_data.items() if isinstance(v, dict)}
    if isinstance(results_data, list):
        return {
            (n.get("nodeID") or n.get("id")): n
            for n in results_data
            if isinstance(n, dict) and (n.get("nodeID") or n.get("id"))
        }
    return {}


def _classify_nodes(node_map: dict[str, dict]) -> tuple[list, list, list]:
    """
    Partition nodes into (failed, skipped_or_canceled, succeeded).
    Each list contains (node_id, node_dict) tuples.
    """
    failed, blocked, succeeded = [], [], []
    for node_id, node in node_map.items():
        status = (node.get("status") or node.get("runState") or "").lower()
        has_error = bool(node.get("errorMessage") or node.get("error"))
        if status == "failed" or (has_error and status != "success"):
            failed.append((node_id, node))
        elif status in ("skipped", "canceled", "cancelled"):
            blocked.append((node_id, node))
        elif status in ("success", "succeeded", "completed"):
            succeeded.append((node_id, node))
    return failed, blocked, succeeded


def _trace_downstream(failed_ids: set[str], node_map: dict[str, dict]) -> set[str]:
    """
    Return all node IDs that are downstream of any failed node.

    Uses predecessor info embedded in results when available (BFS).
    Falls back to returning all skipped/canceled nodes as a heuristic.
    """
    # Build successor map from predecessor fields
    successors: dict[str, set[str]] = {}
    has_predecessor_info = False
    for node_id, node in node_map.items():
        preds = node.get("predecessorNodeIDs") or node.get("predecessors") or []
        if preds:
            has_predecessor_info = True
        for pred_id in preds:
            successors.setdefault(str(pred_id), set()).add(node_id)

    if not has_predecessor_info:
        # Heuristic: skipped/canceled nodes are likely downstream
        return {
            nid for nid, n in node_map.items()
            if (n.get("status") or n.get("runState") or "").lower() in ("skipped", "canceled", "cancelled")
        }

    # BFS from each failed node through the successor graph
    downstream: set[str] = set()
    queue = list(failed_ids)
    while queue:
        current = queue.pop()
        for succ in successors.get(current, set()):
            if succ not in failed_ids and succ not in downstream:
                downstream.add(succ)
                queue.append(succ)
    return downstream


def _format_failed_node(node_id: str, node: dict) -> dict:
    SQL_LIMIT = 500
    raw_sql = node.get("sql")
    sql_field: dict[str, Any] = {}
    if raw_sql:
        if len(raw_sql) > SQL_LIMIT:
            sql_field = {"sql": raw_sql[:SQL_LIMIT], "sql_truncated": True}
        else:
            sql_field = {"sql": raw_sql}
    return {
        "node_id": node_id,
        "node_name": node.get("nodeName") or node.get("name"),
        "error": node.get("errorMessage") or node.get("error"),
        "stage": node.get("stage"),
        "duration_seconds": node.get("durationSeconds") or node.get("duration"),
        **sql_field,
    }


def _format_blocked_node(node_id: str, node: dict) -> dict:
    return {
        "node_id": node_id,
        "node_name": node.get("nodeName") or node.get("name"),
        "status": node.get("status"),
    }


async def get_run_results(run_id: str) -> str:
    """
    Get pre-processed results for a job run — only failures and blocked downstream nodes.

    Returns a concise summary instead of a raw dump of every node. For full
    diagnostic context including run metadata, prefer investigate_failure.

    Use this tool to:
    - See which nodes failed and what errors they produced
    - Identify downstream nodes that were blocked by failures
    - Get execution stats without noise from successful nodes

    Args:
        run_id: The ID of the run to get results for

    Returns:
        JSON with summary stats, failed_nodes (with errors + SQL), and
        downstream_blocked_nodes. Successful nodes are omitted.
    """
    client = get_client()

    try:
        raw = await client.get_run_results(run_id)
    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"Failed to get run results: {e.response.status_code}",
            "run_id": run_id,
        }, indent=2)

    node_map = _parse_results_to_node_map(raw)
    if not node_map:
        return json.dumps({"run_id": run_id, "message": "No node results found.", "raw": raw}, indent=2, default=str)

    failed, blocked, succeeded = _classify_nodes(node_map)
    other_count = len(node_map) - len(failed) - len(blocked) - len(succeeded)

    failed_ids = {nid for nid, _ in failed}
    downstream_ids = _trace_downstream(failed_ids, node_map)
    # Merge heuristic-detected blocked nodes with traced downstream nodes
    all_blocked = {nid for nid, _ in blocked} | downstream_ids

    has_pred_info = any(
        n.get("predecessorNodeIDs") or n.get("predecessors")
        for n in node_map.values()
    )

    return json.dumps({
        "run_id": run_id,
        "summary": {
            "total_nodes": len(node_map),
            "failed": len(failed),
            "succeeded": len(succeeded),
            "downstream_blocked": len(all_blocked),
            "other": other_count,
        },
        "failed_nodes": [_format_failed_node(nid, n) for nid, n in failed],
        "downstream_blocked_nodes": [
            _format_blocked_node(nid, node_map[nid])
            for nid in all_blocked
            if nid in node_map
        ],
        "downstream_tracing": "dependency_graph" if has_pred_info else "heuristic_skipped_nodes",
    }, indent=2, default=str)


async def get_job_details(run_id: str) -> str:
    """
    Get comprehensive details about a job run, combining status and results.

    This is a convenience function that fetches both run status and results
    in one call, providing a complete picture of the job execution.

    Use this tool to:
    - Investigate why a specific job failed
    - Get all error messages and node-level status in one call
    - Understand the full execution history of a run

    Args:
        run_id: The ID of the run to get details for

    Returns:
        JSON object with:
        - run_id: The run identifier
        - status: Current run status
        - results: Node-level execution results
        - errors: Extracted error information (if any failures)
    """
    client = get_client()

    run_data, results_data = await asyncio.gather(
        client.get_run(run_id),
        client.get_run_results(run_id),
        return_exceptions=True,
    )
    if isinstance(run_data, Exception):
        run_data = None
    if isinstance(results_data, Exception):
        results_data = None

    details: dict[str, Any] = {
        "run_id": run_id,
        "run": run_data,
    }

    if results_data:
        node_map = _parse_results_to_node_map(results_data)
        failed, _, _ = _classify_nodes(node_map)
        if failed:
            details["errors"] = [_format_failed_node(nid, n) for nid, n in failed]
            details["error_count"] = len(failed)

    return json.dumps(details, indent=2, default=str)


async def investigate_failure(run_id: str) -> str:
    """
    Investigate a failed job run end-to-end in a single call.

    Combines get_run + get_run_results to produce a concise, actionable
    failure report without raw data noise.

    Use this tool to:
    - Get a complete picture of why a run failed
    - See all failing nodes, their errors, and affected SQL
    - Understand downstream impact (which nodes were blocked)
    - Triage failures quickly without multiple tool calls

    Args:
        run_id: The ID of the run to investigate

    Returns:
        JSON with run metadata, failure summary, failed node details (with
        errors and SQL), and downstream blocked nodes.
    """
    client = get_client()

    # Fetch run metadata and results concurrently
    run_data, results_raw = await asyncio.gather(
        client.get_run(run_id),
        client.get_run_results(run_id),
        return_exceptions=True,
    )

    # Build run summary (gracefully handle fetch failure)
    run_summary: dict[str, Any] = {"run_id": run_id}
    if isinstance(run_data, dict):
        run_summary.update({
            "job_name": run_data.get("jobName") or run_data.get("name"),
            "run_status": run_data.get("runStatus") or run_data.get("status"),
            "environment_id": run_data.get("environmentID") or run_data.get("environment"),
            "run_type": run_data.get("runType"),
            "triggered_by": run_data.get("triggeredBy"),
            "start_time": run_data.get("runStartTime") or run_data.get("startTime"),
            "end_time": run_data.get("runEndTime") or run_data.get("endTime"),
        })
    elif isinstance(run_data, Exception):
        run_summary["run_metadata_error"] = str(run_data)

    # Handle results fetch failure
    if isinstance(results_raw, Exception):
        return json.dumps({
            **run_summary,
            "error": f"Failed to fetch run results: {results_raw}",
        }, indent=2, default=str)

    node_map = _parse_results_to_node_map(results_raw)
    if not node_map:
        return json.dumps({
            **run_summary,
            "message": "Run completed with no node-level results available.",
        }, indent=2, default=str)

    failed, blocked, succeeded = _classify_nodes(node_map)
    other_count = len(node_map) - len(failed) - len(blocked) - len(succeeded)

    failed_ids = {nid for nid, _ in failed}
    downstream_ids = _trace_downstream(failed_ids, node_map)
    all_blocked_ids = {nid for nid, _ in blocked} | downstream_ids

    has_pred_info = any(
        n.get("predecessorNodeIDs") or n.get("predecessors")
        for n in node_map.values()
    )

    if not failed:
        return json.dumps({
            **run_summary,
            "message": "No failed nodes found in results. Run may have succeeded or been canceled.",
            "summary": {
                "total_nodes": len(node_map),
                "succeeded": len(succeeded),
                "skipped_or_canceled": len(blocked),
                "other": other_count,
            },
        }, indent=2, default=str)

    return json.dumps({
        "run": run_summary,
        "summary": {
            "total_nodes": len(node_map),
            "failed": len(failed),
            "succeeded": len(succeeded),
            "downstream_blocked": len(all_blocked_ids),
            "other": other_count,
        },
        "failed_nodes": [_format_failed_node(nid, n) for nid, n in failed],
        "downstream_blocked_nodes": [
            _format_blocked_node(nid, node_map[nid])
            for nid in all_blocked_ids
            if nid in node_map
        ],
        "downstream_tracing": "dependency_graph" if has_pred_info else "heuristic_skipped_nodes",
    }, indent=2, default=str)


async def list_failed_runs(
    environment_id: str | None = None,
    limit: int = 20,
    starting_from: str | None = None,
) -> str:
    """
    List recent failed job runs from Coalesce.

    This is a convenience function that filters for failed runs only.

    Use this tool to:
    - Quickly find jobs that need attention
    - Get a list of recent failures for investigation
    - Monitor pipeline health

    Args:
        environment_id: Filter by environment ID (optional)
        limit: Maximum number of failed runs to return (default 20)
        starting_from: Cursor for next page from previous response (optional)

    Returns:
        JSON object with 'runs' array of failed runs and 'next_cursor' for pagination
    """
    return await list_job_runs(
        environment_id=environment_id,
        run_status="failed",
        limit=limit,
        starting_from=starting_from,
    )


# =============================================================================
# Node Management MCP Tool Functions
# =============================================================================

async def list_environment_nodes_tool(environment_id: str) -> str:
    """
    List all deployed nodes in an environment.

    Use this tool to:
    - View all nodes deployed to a production environment
    - Audit deployed transformations
    - Compare environment configurations
    - Check what's currently in production

    Args:
        environment_id: The environment ID to list nodes from

    Returns:
        JSON array of node objects with IDs, names, types, and metadata
    """
    client = get_client()
    try:
        result = await client.list_environment_nodes(environment_id)
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"Failed to list environment nodes: {e.response.status_code}",
            "environment_id": environment_id,
            "details": e.response.text if e.response.text else None,
        }, indent=2)


async def list_workspace_nodes_tool(workspace_id: str) -> str:
    """
    List all development nodes in a workspace.

    Use this tool to:
    - View all transformations in development
    - Audit workspace structure
    - Inventory data pipeline nodes
    - Find specific nodes by browsing

    Args:
        workspace_id: The workspace ID to list nodes from

    Returns:
        JSON array of node objects with IDs, names, types, and metadata
    """
    client = get_client()
    try:
        result = await client.list_workspace_nodes(workspace_id)
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"Failed to list workspace nodes: {e.response.status_code}",
            "workspace_id": workspace_id,
            "details": e.response.text if e.response.text else None,
        }, indent=2)


async def get_workspace_node_tool(workspace_id: str, node_id: str) -> str:
    """
    Get complete details for a specific workspace node.

    Use this tool to:
    - View full node configuration and SQL
    - Understand a transformation's logic
    - Get metadata for a specific node
    - Inspect node properties before updating

    Args:
        workspace_id: The workspace ID containing the node
        node_id: The node ID to retrieve

    Returns:
        JSON object with complete node details including SQL, metadata, and configuration
    """
    client = get_client()
    try:
        result = await client.get_workspace_node(workspace_id, node_id)
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"Failed to get workspace node: {e.response.status_code}",
            "workspace_id": workspace_id,
            "node_id": node_id,
            "details": e.response.text if e.response.text else None,
        }, indent=2)


async def get_environment_node_tool(environment_id: str, node_id: str) -> str:
    """
    Get complete details for a specific environment node.

    Use this tool to:
    - View deployed node configuration
    - Check production transformation logic
    - Compare environment vs workspace versions
    - Audit deployed nodes

    Args:
        environment_id: The environment ID containing the node
        node_id: The node ID to retrieve

    Returns:
        JSON object with complete node details including SQL, metadata, and configuration
    """
    client = get_client()
    try:
        result = await client.get_environment_node(environment_id, node_id)
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"Failed to get environment node: {e.response.status_code}",
            "environment_id": environment_id,
            "node_id": node_id,
            "details": e.response.text if e.response.text else None,
        }, indent=2)


async def create_workspace_node_tool(
    workspace_id: str,
    node_type: str,
    predecessor_node_ids: list[str] | None = None
) -> str:
    """
    Create a new node in a workspace with default settings.

    Use this tool to:
    - Programmatically create new transformations
    - Build pipeline nodes via code
    - Initialize nodes with workspace defaults

    NOTE: Creates node with defaults. Use set_node to configure further.

    Args:
        workspace_id: The workspace ID to create the node in
        node_type: Node type: 'Stage', 'Fact', 'Dimension', or custom nodeTypeID
        predecessor_node_ids: Array of predecessor node IDs. Use [] for source nodes.

    Returns:
        JSON object with created node including generated ID
    """
    client = get_client()

    # Default to empty array if not provided
    if predecessor_node_ids is None:
        predecessor_node_ids = []

    try:
        result = await client.create_workspace_node(workspace_id, node_type, predecessor_node_ids)
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"Failed to create workspace node: {e.response.status_code}",
            "workspace_id": workspace_id,
            "node_type": node_type,
            "predecessor_node_ids": predecessor_node_ids,
            "details": e.response.text if e.response.text else None,
        }, indent=2)


async def set_node_tool(
    workspace_id: str,
    node_id: str,
    node_config_json: str
) -> str:
    """
    Update (full replacement) an existing node.

    IMPORTANT: This performs a COMPLETE REPLACEMENT of the node.
    Recommended workflow:
    1. Call get_workspace_node to fetch current config
    2. Modify the returned JSON
    3. Pass the complete modified config to this tool

    Use this tool to:
    - Update node configuration
    - Modify SQL or transformation logic
    - Change node properties
    - Update metadata

    Args:
        workspace_id: The workspace ID containing the node
        node_id: The node ID to update
        node_config_json: Complete node object as JSON string (must include all required fields)

    Returns:
        JSON object with updated node
    """
    client = get_client()

    # Parse and validate JSON
    try:
        node_data = json.loads(node_config_json)
    except json.JSONDecodeError as e:
        return json.dumps({
            "error": "Invalid JSON in node_config_json parameter",
            "details": str(e),
        }, indent=2)

    # Validate required fields
    required_fields = ["id", "name", "locationName", "nodeType", "metadata"]
    missing_fields = [field for field in required_fields if field not in node_data]
    if missing_fields:
        return json.dumps({
            "error": "Missing required fields in node configuration",
            "missing_fields": missing_fields,
            "required_fields": required_fields,
        }, indent=2)

    # Validate ID consistency
    if node_data.get("id") != node_id:
        return json.dumps({
            "error": "Node ID mismatch",
            "node_id_parameter": node_id,
            "node_id_in_config": node_data.get("id"),
            "details": "The 'id' field in the node config must match the node_id parameter",
        }, indent=2)

    # Call API
    try:
        result = await client.set_node(workspace_id, node_id, node_data)
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"Failed to update node: {e.response.status_code}",
            "workspace_id": workspace_id,
            "node_id": node_id,
            "details": e.response.text if e.response.text else None,
        }, indent=2)
