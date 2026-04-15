"""Coalesce MCP Server implementation.

Provides tools for interacting with Coalesce API:
- Job runs (read-only)
- Node management (read and write, unless COALESCE_READONLY_MODE=true)
"""

import contextlib
import os
from collections.abc import AsyncIterator

from dotenv import load_dotenv
load_dotenv()

from mcp.server import Server
from mcp.types import Tool, TextContent
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.applications import Starlette
from starlette.routing import Mount
import uvicorn

READONLY_MODE = os.getenv("COALESCE_READONLY_MODE", "false").lower() == "true"
WRITE_TOOLS = {"create_workspace_node", "set_node", "patch_node_field", "start_run", "retry_run", "cancel_run"}

from coalesce_mcp.client import (
    list_job_runs,
    get_run_results,
    list_failed_runs,
    investigate_failure,
    list_environment_nodes_tool,
    get_workspace_node_tool,
    get_environment_node_tool,
    create_workspace_node_tool,
    set_node_tool,
    patch_node_field_tool,
    start_run_tool,
    retry_run_tool,
    cancel_run_tool,
)

# Create MCP server instance
server = Server("coalesce-mcp")

session_manager = StreamableHTTPSessionManager(
    app=server,
    json_response=False,
    stateless=True,
)


@contextlib.asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[None]:
    async with session_manager.run():
        yield


starlette_app = Starlette(
    lifespan=lifespan,
    routes=[
        Mount("/mcp", app=session_manager.handle_request),
    ],
)


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools for the Coalesce MCP server."""
    tools = [
        Tool(
            name="list_job_runs",
            description="""List recent job runs from Coalesce.

Use this tool to:
- Check the status of recent pipeline jobs
- Identify failed jobs that need investigation
- Monitor job execution history
- Find runs by environment or status

Returns a list of runs with status, timestamps, and job names.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "environment_id": {
                        "type": "string",
                        "description": "Filter by environment ID (optional)",
                    },
                    "run_status": {
                        "type": "string",
                        "description": "Filter by runStatus: 'running', 'completed', 'failed', 'canceled'",
                        "enum": ["running", "completed", "failed", "canceled"],
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of runs to return (default 10)",
                        "default": 10,
                    },
                    "starting_from": {
                        "type": "string",
                        "description": "Cursor for pagination (from previous response's next_cursor field)",
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="list_failed_runs",
            description="""List recent FAILED job runs from Coalesce.

Convenience tool that filters for failed runs only.

Use this tool to:
- Quickly find jobs that need attention
- Get a list of recent failures for investigation
- Monitor pipeline health""",
            inputSchema={
                "type": "object",
                "properties": {
                    "environment_id": {
                        "type": "string",
                        "description": "Filter by environment ID (optional)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of failed runs to return (default 20)",
                        "default": 20,
                    },
                    "starting_from": {
                        "type": "string",
                        "description": "Cursor for pagination (from previous response's next_cursor field)",
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="get_run_results",
            description="""Get pre-processed results for a job run — only failures and blocked downstream nodes.

Returns a concise summary instead of a raw dump of every node. Successful
nodes are omitted to reduce noise.

Use this tool to:
- See which nodes failed and what errors they produced
- Identify downstream nodes that were blocked by failures
- Get execution stats without noise from successful nodes

For full diagnostic context including run metadata, prefer investigate_failure.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The ID of the run to get results for",
                    },
                },
                "required": ["run_id"],
            },
        ),
        Tool(
            name="investigate_failure",
            description="""Investigate a failed job run end-to-end in a single call.

This is the BEST STARTING POINT for failure diagnosis. Combines run metadata,
node-level failure details, and downstream impact analysis into one concise response.

Use this tool to:
- Get a complete picture of why a run failed
- See all failing nodes, their exact error messages, and the SQL that failed
- Understand downstream impact (which nodes were blocked by upstream failures)
- Triage pipeline failures quickly without chaining multiple tool calls

Automatically traces downstream blocked nodes using the dependency graph when
available, or falls back to identifying skipped/canceled nodes as a heuristic.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The ID of the failed run to investigate",
                    },
                },
                "required": ["run_id"],
            },
        ),
        Tool(
            name="list_environment_nodes",
            description="""List all deployed nodes in an environment.

Use this tool to:
- View all nodes deployed to a production environment
- Audit deployed transformations
- Compare environment configurations
- Check what's currently in production

Returns array of node objects with IDs, names, types, and metadata.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "environment_id": {
                        "type": "string",
                        "description": "The environment ID to list nodes from",
                    },
                },
                "required": ["environment_id"],
            },
        ),
        Tool(
            name="get_workspace_node",
            description="""Get full configuration for a workspace node — by node ID or by name search.

Provide node_id for a direct fetch, or node_name to search across all workspace nodes.
If both are provided, node_id takes precedence. Partial name matching is used by default.

Use this tool to:
- Inspect SQL, column transforms, and config of a workspace node
- Look up a failing node by name after investigate_failure identifies it
- Get the full node config before patching with patch_node_field or set_node

Returns slimmed node config: name, type, columns (with transforms), source mappings, config.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "workspace_id": {
                        "type": "string",
                        "description": "The workspace ID to search in or fetch from",
                    },
                    "node_id": {
                        "type": "string",
                        "description": "Node UUID for direct fetch (fastest path — use if you have it)",
                    },
                    "node_name": {
                        "type": "string",
                        "description": (
                            "Partial or full node name to search for (case-insensitive). "
                            "Example: 'GAME_DAY_BURST' matches 'STG_SBL_GAME_DAY_BURST_10_FILTER'. "
                            "Use exact_match=true if the partial name is ambiguous."
                        ),
                    },
                    "exact_match": {
                        "type": "boolean",
                        "description": "If true, requires exact name match (case-insensitive). Default false.",
                        "default": False,
                    },
                },
                "required": ["workspace_id"],
            },
        ),
        Tool(
            name="get_environment_node",
            description="""Get complete details for a specific environment node.

Use this tool to:
- View deployed node configuration
- Check production transformation logic
- Compare environment vs workspace versions
- Audit deployed nodes

Returns complete node object including SQL, metadata, and configuration.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "environment_id": {
                        "type": "string",
                        "description": "The environment ID containing the node",
                    },
                    "node_id": {
                        "type": "string",
                        "description": "The node ID to retrieve",
                    },
                },
                "required": ["environment_id", "node_id"],
            },
        ),
        Tool(
            name="create_workspace_node",
            description="""Create a new node in a workspace with default settings.

Use this tool to:
- Programmatically create new transformations
- Build pipeline nodes via code
- Initialize nodes with workspace defaults

NOTE: Creates node with defaults. Use set_node to configure further.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "workspace_id": {
                        "type": "string",
                        "description": "The workspace ID to create the node in",
                    },
                    "node_type": {
                        "type": "string",
                        "description": "Node type: 'Stage', 'Fact', 'Dimension', or custom nodeTypeID",
                    },
                    "predecessor_node_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Array of predecessor node IDs. Use [] for source nodes.",
                        "default": [],
                    },
                },
                "required": ["workspace_id", "node_type"],
            },
        ),
        Tool(
            name="set_node",
            description="""Update (full replacement) an existing node.

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

Returns updated node object.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "workspace_id": {
                        "type": "string",
                        "description": "The workspace ID containing the node",
                    },
                    "node_id": {
                        "type": "string",
                        "description": "The node ID to update",
                    },
                    "node_config_json": {
                        "type": "string",
                        "description": "Complete node object as JSON string (must include all required fields: id, name, locationName, nodeType, metadata)",
                    },
                },
                "required": ["workspace_id", "node_id", "node_config_json"],
            },
        ),
        Tool(
            name="patch_node_field",
            description="""Apply a targeted, surgical update to a single field on a workspace node.

Handles the full fetch-modify-replace cycle internally — you only need to
provide the path to the field you want to change and its new value.

Supported field_path expressions (raw API node shape):
- "metadata.columns[N].sources[N].transform"  — column-level SQL transform (most common fix)
- "metadata.columns[N].isBusinessKey"          — set/unset business key ("true" or "false")
- "metadata.columns[N].isSurrogateKey"         — set/unset surrogate key ("true" or "false")
- "config.whereClause"                         — WHERE clause modifier
- "config.preSQL" / "config.postSQL"           — pre/post SQL hooks
- "metadata.storageMapping[N].join"            — JOIN condition on a source mapping
- "metadata.storageMapping[N].customSQL"       — custom SQL on a source mapping

Boolean values ("true"/"false") are automatically coerced to JSON booleans.

Use this tool to:
- Apply a targeted SQL fix (e.g. CAST → TRY_CAST) to a single column transform
- Update a WHERE clause or JOIN condition without touching the rest of the node
- Patch the overrideSQL body

IMPORTANT: Always show the user a before/after diff and get explicit confirmation
before calling this tool.

Returns a diff summary: node_name, field_path, old_value, new_value.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "workspace_id": {
                        "type": "string",
                        "description": "The workspace ID containing the node",
                    },
                    "node_id": {
                        "type": "string",
                        "description": "The node ID to update",
                    },
                    "field_path": {
                        "type": "string",
                        "description": "Dot/bracket path to the field to update using raw API node shape (e.g. 'metadata.columns[4].sources[0].transform', 'config.whereClause')",
                    },
                    "new_value": {
                        "type": "string",
                        "description": "The new value to set at that path",
                    },
                },
                "required": ["workspace_id", "node_id", "field_path", "new_value"],
            },
        ),
        Tool(
            name="start_run",
            description="""Start a new job run in Coalesce.

Use this tool to:
- Kick off a fresh pipeline run for an environment or specific job
- Trigger a full environment refresh
- Run a specific job by job ID

After starting, use investigate_failure with the returned run_id to check results.
Returns run_id and initial status.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "environment_id": {
                        "type": "string",
                        "description": "The environment ID to run",
                    },
                    "job_id": {
                        "type": "string",
                        "description": "Specific job ID to run (optional). Omit to refresh the entire environment.",
                    },
                    "parallelism": {
                        "type": "integer",
                        "description": "Optional parallel execution level",
                    },
                },
                "required": ["environment_id"],
            },
        ),
        Tool(
            name="retry_run",
            description="""Retry a failed job run from the point of failure.

Only the failed nodes are re-executed — nodes that already succeeded are skipped.
This is the RECOMMENDED tool to verify a fix after patching a node.

Recommended workflow:
1. investigate_failure → identify root cause
2. patch_node_field → fix the node
3. retry_run → verify the fix
4. investigate_failure → confirm all nodes passed

Returns a new run_id for the retry run.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The run ID of the failed run to retry",
                    },
                },
                "required": ["run_id"],
            },
        ),
        Tool(
            name="cancel_run",
            description="""Cancel an in-progress job run.

Use this tool to:
- Stop a run that is taking too long
- Abort a run triggered by mistake
- Free up resources from a stuck run

Returns confirmation of the cancellation request.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The run ID to cancel",
                    },
                    "environment_id": {
                        "type": "string",
                        "description": "The environment ID the run belongs to",
                    },
                },
                "required": ["run_id"],
            },
        ),
    ]

    if READONLY_MODE:
        tools = [t for t in tools if t.name not in WRITE_TOOLS]

    return tools


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls."""

    if READONLY_MODE and name in WRITE_TOOLS:
        return [TextContent(type="text", text='{"error": "Write operations are disabled in readonly mode"}')]

    if name == "list_job_runs":
        result = await list_job_runs(
            environment_id=arguments.get("environment_id"),
            run_status=arguments.get("run_status"),
            limit=arguments.get("limit", 50),
            starting_from=arguments.get("starting_from"),
        )
        return [TextContent(type="text", text=result)]

    elif name == "list_failed_runs":
        result = await list_failed_runs(
            environment_id=arguments.get("environment_id"),
            limit=arguments.get("limit", 20),
            starting_from=arguments.get("starting_from"),
        )
        return [TextContent(type="text", text=result)]

    elif name == "get_run_results":
        run_id = arguments.get("run_id")
        if not run_id:
            return [TextContent(type="text", text='{"error": "run_id is required"}')]
        result = await get_run_results(run_id)
        return [TextContent(type="text", text=result)]

    elif name == "investigate_failure":
        run_id = arguments.get("run_id")
        if not run_id:
            return [TextContent(type="text", text='{"error": "run_id is required"}')]
        result = await investigate_failure(run_id)
        return [TextContent(type="text", text=result)]

    elif name == "list_environment_nodes":
        environment_id = arguments.get("environment_id")
        if not environment_id:
            return [TextContent(type="text", text='{"error": "environment_id is required"}')]
        result = await list_environment_nodes_tool(environment_id)
        return [TextContent(type="text", text=result)]

    elif name == "get_workspace_node":
        workspace_id = arguments.get("workspace_id")
        node_id = arguments.get("node_id")
        node_name = arguments.get("node_name")
        if not workspace_id:
            return [TextContent(type="text", text='{"error": "workspace_id is required"}')]
        if not node_id and not node_name:
            return [TextContent(type="text", text='{"error": "Either node_id or node_name is required"}')]
        result = await get_workspace_node_tool(
            workspace_id,
            node_id=node_id,
            node_name=node_name,
            exact_match=arguments.get("exact_match", False),
        )
        return [TextContent(type="text", text=result)]

    elif name == "get_environment_node":
        environment_id = arguments.get("environment_id")
        node_id = arguments.get("node_id")
        if not environment_id or not node_id:
            return [TextContent(type="text", text='{"error": "environment_id and node_id are required"}')]
        result = await get_environment_node_tool(environment_id, node_id)
        return [TextContent(type="text", text=result)]

    elif name == "create_workspace_node":
        workspace_id = arguments.get("workspace_id")
        node_type = arguments.get("node_type")
        if not workspace_id or not node_type:
            return [TextContent(type="text", text='{"error": "workspace_id and node_type are required"}')]
        predecessor_node_ids = arguments.get("predecessor_node_ids")
        result = await create_workspace_node_tool(workspace_id, node_type, predecessor_node_ids)
        return [TextContent(type="text", text=result)]

    elif name == "set_node":
        workspace_id = arguments.get("workspace_id")
        node_id = arguments.get("node_id")
        node_config_json = arguments.get("node_config_json")
        if not workspace_id or not node_id or not node_config_json:
            return [TextContent(type="text", text='{"error": "workspace_id, node_id, and node_config_json are required"}')]
        result = await set_node_tool(workspace_id, node_id, node_config_json)
        return [TextContent(type="text", text=result)]

    elif name == "patch_node_field":
        workspace_id = arguments.get("workspace_id")
        node_id = arguments.get("node_id")
        field_path = arguments.get("field_path")
        new_value = arguments.get("new_value")
        if not workspace_id or not node_id or not field_path or new_value is None:
            return [TextContent(type="text", text='{"error": "workspace_id, node_id, field_path, and new_value are required"}')]
        result = await patch_node_field_tool(workspace_id, node_id, field_path, new_value)
        return [TextContent(type="text", text=result)]

    elif name == "start_run":
        environment_id = arguments.get("environment_id")
        if not environment_id:
            return [TextContent(type="text", text='{"error": "environment_id is required"}')]
        result = await start_run_tool(
            environment_id,
            job_id=arguments.get("job_id"),
            parallelism=arguments.get("parallelism"),
        )
        return [TextContent(type="text", text=result)]

    elif name == "retry_run":
        run_id = arguments.get("run_id")
        if not run_id:
            return [TextContent(type="text", text='{"error": "run_id is required"}')]
        result = await retry_run_tool(run_id)
        return [TextContent(type="text", text=result)]

    elif name == "cancel_run":
        run_id = arguments.get("run_id")
        if not run_id:
            return [TextContent(type="text", text='{"error": "run_id is required"}')]
        result = await cancel_run_tool(run_id, environment_id=arguments.get("environment_id"))
        return [TextContent(type="text", text=result)]

    else:
        return [TextContent(type="text", text=f'{{"error": "Unknown tool: {name}"}}')]


def main():
    """Entry point for console script"""
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8001"))
    uvicorn.run(starlette_app, host=host, port=port)


if __name__ == "__main__":
    main()
