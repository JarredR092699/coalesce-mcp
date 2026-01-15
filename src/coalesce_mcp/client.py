"""Coalesce API tool implementations for job run endpoints.

Based on Coalesce API documentation:
- https://docs.coalesce.io/docs/api/coalesce/get-runs
- https://docs.coalesce.io/docs/api/runs/run-status

All endpoints are READ-ONLY. No mutation operations are included.
"""

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
        Get details for a specific run.

        Endpoint: GET /v1/runs/{runID}

        Args:
            run_id: The run ID to retrieve

        Returns:
            Run object with details
        """
        client = await self._get_client()

        response = await client.get(f"/v1/runs/{run_id}")
        response.raise_for_status()

        return response.json()

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


async def get_run_results(run_id: str) -> str:
    """
    Get detailed results for a job run, including node-level execution details.

    Use this tool to:
    - See which nodes succeeded or failed
    - Get error messages for failed nodes
    - Understand what SQL was executed
    - Identify the specific transformation that caused a failure

    Args:
        run_id: The ID of the run to get results for

    Returns:
        JSON object with results organized by node, including:
        - Node status (success/failed)
        - Error messages
        - SQL executed
        - Execution time
    """
    client = get_client()

    try:
        results = await client.get_run_results(run_id)
        return json.dumps(results, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return json.dumps({
            "error": f"Failed to get run results: {e.response.status_code}",
            "run_id": run_id,
        }, indent=2)


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

    # Fetch all available information
    status_data = None
    results_data = None
    run_data = None

    try:
        run_data = await client.get_run(run_id)
    except httpx.HTTPStatusError:
        pass

    try:
        status_data = await client.get_run_status(run_id)
    except httpx.HTTPStatusError:
        pass

    try:
        results_data = await client.get_run_results(run_id)
    except httpx.HTTPStatusError:
        pass

    # Combine into comprehensive response
    details = {
        "run_id": run_id,
        "run": run_data,
        "status": status_data,
        "results": results_data,
    }

    # Extract errors from results for easy access
    if results_data and isinstance(results_data, dict):
        errors = []
        for node_id, node_result in results_data.items():
            if isinstance(node_result, dict):
                node_status = node_result.get("status", "").lower()
                error_msg = node_result.get("errorMessage") or node_result.get("error")

                if node_status == "failed" or error_msg:
                    errors.append({
                        "node_id": node_id,
                        "node_name": node_result.get("nodeName") or node_result.get("name"),
                        "stage": node_result.get("stage"),
                        "status": node_status,
                        "error_message": error_msg,
                        "sql": node_result.get("sql"),
                    })

        if errors:
            details["errors"] = errors
            details["error_count"] = len(errors)

    return json.dumps(details, indent=2, default=str)


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
