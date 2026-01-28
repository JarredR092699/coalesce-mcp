"""Coalesce MCP Server implementation.

Provides tools for interacting with Coalesce API:
- Job runs (read-only)
- Node management (read and write)
"""

from mcp.server import Server
from mcp.types import Tool, TextContent
from mcp.server.stdio import stdio_server

from coalesce_mcp.client import (
    list_job_runs,
    get_run,
    get_run_status,
    get_run_results,
    get_job_details,
    list_failed_runs,
    list_environment_nodes_tool,
    list_workspace_nodes_tool,
    get_workspace_node_tool,
    get_environment_node_tool,
    create_workspace_node_tool,
    set_node_tool,
)

# Create MCP server instance
server = Server("coalesce-mcp")


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools for the Coalesce MCP server."""
    return [
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
                        "description": "Maximum number of runs to return (default 50)",
                        "default": 50,
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
            name="get_run",
            description="""Get details for a specific job run.

Use this tool to:
- Get full details about a specific run
- See run configuration and parameters
- Check when a run started and ended""",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The ID of the run to retrieve",
                    },
                },
                "required": ["run_id"],
            },
        ),
        Tool(
            name="get_run_status",
            description="""Get the current status of a job run.

Use this tool to:
- Check if a run is still in progress
- See the current execution status
- Monitor long-running jobs""",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The ID of the run to check",
                    },
                },
                "required": ["run_id"],
            },
        ),
        Tool(
            name="get_run_results",
            description="""Get detailed results for a job run, including node-level execution details.

Use this tool to:
- See which nodes succeeded or failed
- Get error messages for failed nodes
- Understand what SQL was executed
- Identify the specific transformation that caused a failure""",
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
            name="get_job_details",
            description="""Get comprehensive details about a job run (combines status + results).

This is the RECOMMENDED tool for investigating failures. It fetches all available
information about a run in one call.

Use this tool to:
- Investigate why a specific job failed
- Get all error messages and node-level status in one call
- Understand the full execution history of a run

Returns run info, status, results, and extracted error details.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The ID of the run to get details for",
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
            name="list_workspace_nodes",
            description="""List all development nodes in a workspace.

Use this tool to:
- View all transformations in development
- Audit workspace structure
- Inventory data pipeline nodes
- Find specific nodes by browsing

Returns array of node objects with IDs, names, types, and metadata.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "workspace_id": {
                        "type": "string",
                        "description": "The workspace ID to list nodes from",
                    },
                },
                "required": ["workspace_id"],
            },
        ),
        Tool(
            name="get_workspace_node",
            description="""Get complete details for a specific workspace node.

Use this tool to:
- View full node configuration and SQL
- Understand a transformation's logic
- Get metadata for a specific node
- Inspect node properties before updating

Returns complete node object including SQL, metadata, and configuration.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "workspace_id": {
                        "type": "string",
                        "description": "The workspace ID containing the node",
                    },
                    "node_id": {
                        "type": "string",
                        "description": "The node ID to retrieve",
                    },
                },
                "required": ["workspace_id", "node_id"],
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
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls."""

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

    elif name == "get_run":
        run_id = arguments.get("run_id")
        if not run_id:
            return [TextContent(type="text", text='{"error": "run_id is required"}')]
        result = await get_run(run_id)
        return [TextContent(type="text", text=result)]

    elif name == "get_run_status":
        run_id = arguments.get("run_id")
        if not run_id:
            return [TextContent(type="text", text='{"error": "run_id is required"}')]
        result = await get_run_status(run_id)
        return [TextContent(type="text", text=result)]

    elif name == "get_run_results":
        run_id = arguments.get("run_id")
        if not run_id:
            return [TextContent(type="text", text='{"error": "run_id is required"}')]
        result = await get_run_results(run_id)
        return [TextContent(type="text", text=result)]

    elif name == "get_job_details":
        run_id = arguments.get("run_id")
        if not run_id:
            return [TextContent(type="text", text='{"error": "run_id is required"}')]
        result = await get_job_details(run_id)
        return [TextContent(type="text", text=result)]

    elif name == "list_environment_nodes":
        environment_id = arguments.get("environment_id")
        if not environment_id:
            return [TextContent(type="text", text='{"error": "environment_id is required"}')]
        result = await list_environment_nodes_tool(environment_id)
        return [TextContent(type="text", text=result)]

    elif name == "list_workspace_nodes":
        workspace_id = arguments.get("workspace_id")
        if not workspace_id:
            return [TextContent(type="text", text='{"error": "workspace_id is required"}')]
        result = await list_workspace_nodes_tool(workspace_id)
        return [TextContent(type="text", text=result)]

    elif name == "get_workspace_node":
        workspace_id = arguments.get("workspace_id")
        node_id = arguments.get("node_id")
        if not workspace_id or not node_id:
            return [TextContent(type="text", text='{"error": "workspace_id and node_id are required"}')]
        result = await get_workspace_node_tool(workspace_id, node_id)
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

    else:
        return [TextContent(type="text", text=f'{{"error": "Unknown tool: {name}"}}')]


async def async_main():
    """Run the MCP server (async)."""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )

def main(): 
    """Entry point for console script"""
    import asyncio
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
