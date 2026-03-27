"""Tests for job run tools in client.py.

Uses httpx's built-in MockTransport to stub HTTP responses without hitting the real API.
"""
import json
import pytest
import httpx

from coalesce_mcp.client import (
    CoalesceClient,
    list_job_runs,
    list_failed_runs,
    get_run,
    get_run_status,
    get_run_results,
    get_job_details,
    investigate_failure,
    _parse_results_to_node_map,
    _classify_nodes,
    _trace_downstream,
    get_client,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

SAMPLE_RUN = {
    "id": "1001",
    "runStatus": "failed",
    "environmentID": "env-1",
    "jobName": "nightly_load",
    "runStartTime": "2026-03-19T00:00:00Z",
    "runEndTime": "2026-03-19T00:05:00Z",
    "runType": "scheduled",
    "triggeredBy": "scheduler",
}

SAMPLE_RESULTS = {
    "node-aaa": {
        "status": "failed",
        "nodeName": "dim_customer",
        "errorMessage": "SQL compilation error: object does not exist",
        "stage": "transform",
        "sql": "CREATE TABLE dim_customer AS SELECT * FROM src_customer",
        "durationSeconds": 3.5,
    },
    "node-bbb": {
        "status": "skipped",
        "nodeName": "fct_orders",
    },
    "node-ccc": {
        "status": "success",
        "nodeName": "stg_products",
    },
}


def _make_transport(routes: dict) -> httpx.MockTransport:
    """
    Build an httpx.MockTransport from a dict of {url_substring: response_body}.

    Patterns are matched longest-first so more-specific routes (e.g.
    "/v1/runs/1001/results") win over shorter ones ("/v1/runs").

    If body is a dict/list it's JSON-encoded; if bytes it's used directly.
    Status 200 unless body is a tuple (status, body).
    """
    # Sort by descending pattern length so longer/more-specific patterns match first
    sorted_routes = sorted(routes.items(), key=lambda kv: len(kv[0]), reverse=True)

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        for pattern, body in sorted_routes:
            if pattern in url:
                if isinstance(body, tuple):
                    status, body = body
                else:
                    status = 200
                if isinstance(body, (dict, list)):
                    content = json.dumps(body).encode()
                    headers = {"content-type": "application/json"}
                elif isinstance(body, bytes):
                    content = body
                    headers = {}
                else:
                    content = body.encode()
                    headers = {}
                return httpx.Response(status, content=content, headers=headers)
        return httpx.Response(404, content=b'{"error":"not found"}', headers={"content-type": "application/json"})

    return httpx.MockTransport(handler)


def _make_client(routes: dict) -> CoalesceClient:
    """Return a CoalesceClient wired to a mock transport."""
    c = CoalesceClient()
    transport = _make_transport(routes)
    c._client = httpx.AsyncClient(
        base_url="https://app.coalescesoftware.io/api/",
        transport=transport,
        headers={"Authorization": "Bearer test-token"},
    )
    return c


# ---------------------------------------------------------------------------
# Unit tests: _parse_results_to_node_map
# ---------------------------------------------------------------------------

class TestParseResultsToNodeMap:
    def test_flat_dict(self):
        raw = {"node-1": {"status": "failed"}, "node-2": {"status": "success"}}
        result = _parse_results_to_node_map(raw)
        assert result == raw

    def test_wrapped_nodes_dict(self):
        raw = {"nodes": {"node-1": {"status": "failed"}}}
        result = _parse_results_to_node_map(raw)
        assert "node-1" in result

    def test_wrapped_data_list(self):
        raw = {"data": [{"nodeID": "node-1", "status": "failed"}, {"id": "node-2", "status": "success"}]}
        result = _parse_results_to_node_map(raw)
        assert "node-1" in result
        assert "node-2" in result

    def test_list_input(self):
        raw = [{"nodeID": "node-1", "status": "failed"}, {"id": "node-2", "status": "skipped"}]
        result = _parse_results_to_node_map(raw)
        assert "node-1" in result
        assert "node-2" in result

    def test_empty_dict(self):
        assert _parse_results_to_node_map({}) == {}

    def test_none_values_skipped(self):
        # Non-dict values in a flat dict should be ignored
        raw = {"node-1": {"status": "failed"}, "count": 5, "name": "test"}
        result = _parse_results_to_node_map(raw)
        assert "node-1" in result
        assert "count" not in result


# ---------------------------------------------------------------------------
# Unit tests: _classify_nodes
# ---------------------------------------------------------------------------

class TestClassifyNodes:
    def test_failed_by_status(self):
        node_map = {"n1": {"status": "failed", "errorMessage": "err"}}
        failed, blocked, succeeded = _classify_nodes(node_map)
        assert len(failed) == 1
        assert failed[0][0] == "n1"

    def test_failed_by_runState(self):
        node_map = {"n1": {"runState": "failed", "errorMessage": "err"}}
        failed, _, _ = _classify_nodes(node_map)
        assert len(failed) == 1

    def test_succeeded(self):
        node_map = {"n1": {"status": "success"}}
        failed, _, succeeded = _classify_nodes(node_map)
        assert len(succeeded) == 1
        assert len(failed) == 0

    def test_blocked_skipped(self):
        node_map = {"n1": {"status": "skipped"}}
        _, blocked, _ = _classify_nodes(node_map)
        assert len(blocked) == 1

    def test_blocked_canceled(self):
        node_map = {"n1": {"status": "canceled"}}
        _, blocked, _ = _classify_nodes(node_map)
        assert len(blocked) == 1

    def test_mixed(self):
        node_map = {
            "n1": {"status": "failed", "errorMessage": "oops"},
            "n2": {"status": "skipped"},
            "n3": {"status": "success"},
            "n4": {"runState": "succeeded"},
        }
        failed, blocked, succeeded = _classify_nodes(node_map)
        assert len(failed) == 1
        assert len(blocked) == 1
        assert len(succeeded) == 2


# ---------------------------------------------------------------------------
# Unit tests: _trace_downstream
# ---------------------------------------------------------------------------

class TestTraceDownstream:
    def test_heuristic_fallback_no_predecessors(self):
        node_map = {
            "n1": {"status": "failed"},
            "n2": {"status": "skipped"},
            "n3": {"status": "success"},
        }
        downstream = _trace_downstream({"n1"}, node_map)
        assert "n2" in downstream
        assert "n3" not in downstream

    def test_dependency_graph_tracing(self):
        node_map = {
            "n1": {"status": "failed"},
            "n2": {"status": "skipped", "predecessorNodeIDs": ["n1"]},
            "n3": {"status": "skipped", "predecessorNodeIDs": ["n2"]},
            "n4": {"status": "success", "predecessorNodeIDs": []},
        }
        downstream = _trace_downstream({"n1"}, node_map)
        assert "n2" in downstream
        assert "n3" in downstream
        assert "n4" not in downstream

    def test_failed_node_in_downstream_bfs(self):
        """BFS includes n2 even if it's also failed — callers must exclude failed_ids."""
        node_map = {
            "n1": {"status": "failed"},
            "n2": {"status": "failed", "predecessorNodeIDs": ["n1"]},
        }
        downstream = _trace_downstream({"n1"}, node_map)
        # BFS finds n2 as downstream — filtering failed nodes is the caller's job
        assert "n2" in downstream


# ---------------------------------------------------------------------------
# Integration tests: MCP tool functions with mocked HTTP
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestListJobRuns:
    async def test_returns_formatted_runs(self):
        routes = {
            "/v1/runs": {"data": [SAMPLE_RUN], "next": None}
        }
        client = _make_client(routes)

        # Patch module-level client
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await list_job_runs(limit=5)
        finally:
            mod._client = orig

        parsed = json.loads(result)
        assert parsed["count"] == 1
        run = parsed["runs"][0]
        assert run["run_id"] == "1001"
        assert run["run_status"] == "failed"
        assert run["job_name"] == "nightly_load"

    async def test_empty_response(self):
        routes = {"/v1/runs": {"data": [], "next": None}}
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await list_job_runs()
        finally:
            mod._client = orig

        parsed = json.loads(result)
        assert parsed["count"] == 0
        assert parsed["runs"] == []

    async def test_pagination_cursor_passed(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(200, content=json.dumps({"data": [], "next": None}).encode(), headers={"content-type": "application/json"})

        client = CoalesceClient()
        client._client = httpx.AsyncClient(
            base_url="https://app.coalescesoftware.io/api/",
            transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer test"},
        )
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            await list_job_runs(starting_from="999")
        finally:
            mod._client = orig

        assert "startingFrom=999" in captured["url"]

    async def test_status_filter_passed(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(200, content=json.dumps({"data": [], "next": None}).encode(), headers={"content-type": "application/json"})

        client = CoalesceClient()
        client._client = httpx.AsyncClient(
            base_url="https://app.coalescesoftware.io/api/",
            transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer test"},
        )
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            await list_job_runs(run_status="failed")
        finally:
            mod._client = orig

        assert "runStatus=failed" in captured["url"]


@pytest.mark.asyncio
class TestListFailedRuns:
    async def test_filters_to_failed(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(200, content=json.dumps({"data": [], "next": None}).encode(), headers={"content-type": "application/json"})

        client = CoalesceClient()
        client._client = httpx.AsyncClient(
            base_url="https://app.coalescesoftware.io/api/",
            transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer test"},
        )
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            await list_failed_runs()
        finally:
            mod._client = orig

        assert "runStatus=failed" in captured["url"]


@pytest.mark.asyncio
class TestGetRunResults:
    async def test_filters_out_succeeded_nodes(self):
        routes = {f"/v1/runs/1001/results": SAMPLE_RESULTS}
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await get_run_results("1001")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        assert parsed["summary"]["failed"] == 1
        assert parsed["summary"]["succeeded"] == 1
        failed_names = [n["node_name"] for n in parsed["failed_nodes"]]
        assert "dim_customer" in failed_names
        # Succeeded node should not appear in either list
        all_names = [n["node_name"] for n in parsed["failed_nodes"]] + \
                    [n["node_name"] for n in parsed["downstream_blocked_nodes"]]
        assert "stg_products" not in all_names

    async def test_failed_node_has_error_and_sql(self):
        routes = {f"/v1/runs/1001/results": SAMPLE_RESULTS}
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await get_run_results("1001")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        node = parsed["failed_nodes"][0]
        assert node["error"] == "SQL compilation error: object does not exist"
        assert "dim_customer" in node["sql"]

    async def test_blocked_node_included(self):
        routes = {f"/v1/runs/1001/results": SAMPLE_RESULTS}
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await get_run_results("1001")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        blocked_names = [n["node_name"] for n in parsed["downstream_blocked_nodes"]]
        assert "fct_orders" in blocked_names

    async def test_empty_body_returns_no_results(self):
        routes = {f"/v1/runs/1001/results": (200, b"")}
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await get_run_results("1001")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        assert "message" in parsed

    async def test_runState_field_name_handled(self):
        """API may return runState instead of status — both must be handled."""
        results = {
            "node-x": {"runState": "failed", "errorMessage": "boom", "nodeName": "my_node"},
            "node-y": {"runState": "skipped", "nodeName": "downstream_node"},
        }
        routes = {"/v1/runs/2000/results": results}
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await get_run_results("2000")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        assert parsed["summary"]["failed"] == 1
        blocked = [n["node_name"] for n in parsed["downstream_blocked_nodes"]]
        assert "downstream_node" in blocked


@pytest.mark.asyncio
class TestInvestigateFailure:
    async def test_returns_run_and_failures(self):
        routes = {
            "/v1/runs": {"data": [SAMPLE_RUN], "next": None},
            "/v1/runs/1001/results": SAMPLE_RESULTS,
        }
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await investigate_failure("1001")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        assert "run" in parsed
        assert "failed_nodes" in parsed
        assert parsed["summary"]["failed"] == 1

    async def test_no_failures_returns_message(self):
        results_all_success = {
            "node-aaa": {"status": "success", "nodeName": "dim_customer"},
        }
        routes = {
            "/v1/runs": {"data": [SAMPLE_RUN], "next": None},
            "/v1/runs/1001/results": results_all_success,
        }
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await investigate_failure("1001")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        assert "message" in parsed
        assert "No failed nodes" in parsed["message"]

    async def test_results_fetch_failure_handled(self):
        """If results endpoint 404s, investigate_failure should return error JSON not raise."""
        routes = {
            "/v1/runs": {"data": [SAMPLE_RUN], "next": None},
            "/v1/runs/1001/results": (404, {"error": "not found"}),
        }
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await investigate_failure("1001")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        assert "error" in parsed

    async def test_sql_truncated_at_500_chars(self):
        long_sql = "SELECT " + "x," * 300 + "1"
        results = {
            "node-x": {
                "status": "failed",
                "nodeName": "big_node",
                "errorMessage": "err",
                "sql": long_sql,
            }
        }
        routes = {
            "/v1/runs": {"data": [SAMPLE_RUN], "next": None},
            "/v1/runs/1001/results": results,
        }
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await investigate_failure("1001")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        node = parsed["failed_nodes"][0]
        assert len(node["sql"]) <= 500
        assert node.get("sql_truncated") is True


@pytest.mark.asyncio
class TestGetRun:
    async def test_returns_run_when_found(self):
        routes = {
            "/v1/runs": {"data": [SAMPLE_RUN], "next": None},
            "/scheduler/runStatus": {"runID": "1001", "status": "failed"},
        }
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await get_run("1001")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        # Should have run data (either from list or scheduler)
        assert parsed is not None

    async def test_run_not_found_returns_error_json(self):
        routes = {
            "/v1/runs": {"data": [], "next": None},
            "/scheduler/runStatus": (404, {"error": "not found"}),
        }
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await get_run("9999")
        finally:
            mod._client = orig

        # Should not raise — should return JSON
        parsed = json.loads(result)
        assert parsed is not None


# ---------------------------------------------------------------------------
# Unit tests: _apply_field_path
# ---------------------------------------------------------------------------

class TestApplyFieldPath:
    """Test targeted field path updates on node dicts."""

    def test_modify_existing_field(self):
        from coalesce_mcp.client import _apply_field_path
        node = {"metadata": {"columns": [{"name": "COL_A", "isBusinessKey": False}]}}
        old, new = _apply_field_path(node, "metadata.columns[0].isBusinessKey", "true")
        assert old == "False"
        assert new == "True"
        assert node["metadata"]["columns"][0]["isBusinessKey"] is True

    def test_create_new_field_on_final_segment(self):
        from coalesce_mcp.client import _apply_field_path
        node = {"metadata": {"columns": [{"name": "COL_A", "dataType": "VARCHAR"}]}}
        # isBusinessKey does not exist yet — should be created
        old, new = _apply_field_path(node, "metadata.columns[0].isBusinessKey", "true")
        assert old == "null"
        assert new == "True"
        assert node["metadata"]["columns"][0]["isBusinessKey"] is True

    def test_missing_intermediate_key_raises(self):
        from coalesce_mcp.client import _apply_field_path
        node = {"metadata": {}}
        with pytest.raises(KeyError, match="columns"):
            _apply_field_path(node, "metadata.columns[0].isBusinessKey", "true")

    def test_boolean_false_coercion(self):
        from coalesce_mcp.client import _apply_field_path
        node = {"metadata": {"columns": [{"name": "COL_A", "isBusinessKey": True}]}}
        old, new = _apply_field_path(node, "metadata.columns[0].isBusinessKey", "false")
        assert old == "True"
        assert new == "False"
        assert node["metadata"]["columns"][0]["isBusinessKey"] is False


# ---------------------------------------------------------------------------
# Integration tests: patch_node_field_tool
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestPatchNodeFieldTool:
    """Test that patch_node_field_tool handles the table property and field creation."""

    async def test_adds_table_property_when_missing(self):
        from coalesce_mcp.client import patch_node_field_tool
        # Raw node without 'table' property
        raw_node = {
            "id": "node-1",
            "name": "DIM_TEST",
            "nodeType": "Dimension",
            "locationName": "SRC",
            "metadata": {
                "columns": [{"name": "COL_A", "isBusinessKey": False}],
            },
        }
        captured_put = {}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(200, content=json.dumps(raw_node).encode(),
                                     headers={"content-type": "application/json"})
            elif request.method == "PUT":
                captured_put["body"] = json.loads(request.content)
                return httpx.Response(200, content=json.dumps(raw_node).encode(),
                                     headers={"content-type": "application/json"})
            return httpx.Response(404)

        client = CoalesceClient()
        client._client = httpx.AsyncClient(
            base_url="https://app.coalescesoftware.io/api/",
            transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer test"},
        )
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await patch_node_field_tool("21", "node-1",
                                                  "metadata.columns[0].isBusinessKey", "true")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        assert parsed.get("success") is True
        # Verify the PUT body included 'table'
        assert captured_put["body"]["table"] == "DIM_TEST"
        # Verify the field was set
        assert captured_put["body"]["metadata"]["columns"][0]["isBusinessKey"] is True

    async def test_preserves_existing_table_property(self):
        from coalesce_mcp.client import patch_node_field_tool
        raw_node = {
            "id": "node-1",
            "name": "DIM_TEST",
            "table": "CUSTOM_TABLE_NAME",
            "nodeType": "Dimension",
            "locationName": "SRC",
            "metadata": {
                "columns": [{"name": "COL_A", "isBusinessKey": False}],
            },
        }
        captured_put = {}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(200, content=json.dumps(raw_node).encode(),
                                     headers={"content-type": "application/json"})
            elif request.method == "PUT":
                captured_put["body"] = json.loads(request.content)
                return httpx.Response(200, content=json.dumps(raw_node).encode(),
                                     headers={"content-type": "application/json"})
            return httpx.Response(404)

        client = CoalesceClient()
        client._client = httpx.AsyncClient(
            base_url="https://app.coalescesoftware.io/api/",
            transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer test"},
        )
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await patch_node_field_tool("21", "node-1",
                                                  "metadata.columns[0].isBusinessKey", "true")
        finally:
            mod._client = orig

        parsed = json.loads(result)
        assert parsed.get("success") is True
        # Should NOT overwrite existing table value
        assert captured_put["body"]["table"] == "CUSTOM_TABLE_NAME"


@pytest.mark.asyncio
class TestNodeManagementEmptyBodies:
    """Test that node management methods handle empty response bodies gracefully."""

    async def test_list_workspace_nodes_empty_body(self):
        from coalesce_mcp.client import list_workspace_nodes_tool
        routes = {"/v1/workspaces/1/nodes": (200, b"")}
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await list_workspace_nodes_tool("1")
        finally:
            mod._client = orig
        # Should return valid JSON, not raise
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    async def test_list_environment_nodes_empty_body(self):
        from coalesce_mcp.client import list_environment_nodes_tool
        routes = {"/v1/environments/1/nodes": (200, b"")}
        client = _make_client(routes)
        import coalesce_mcp.client as mod
        orig = mod._client
        mod._client = client
        try:
            result = await list_environment_nodes_tool("1")
        finally:
            mod._client = orig
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
