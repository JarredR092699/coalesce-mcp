# Coalesce MCP Server

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) server that gives Claude direct access to your [Coalesce](https://coalesce.io) data pipelines. Ask Claude to investigate failures, inspect nodes, and understand your pipeline in plain English — without leaving your chat.

## What it does

**Failure investigation** — the primary use case. Ask Claude:
- *"Why did run 95011 fail?"*
- *"What failed in the last hour?"*
- *"Show me the error and SQL from the failing node"*

Claude will call `investigate_failure`, surface the exact error message, the SQL that failed, and which downstream nodes were blocked — all in one response.

**Node inspection** — browse and read your workspace:
- *"Show me the columns and transforms for node X"*
- *"What does this staging table look like?"*

**Node editing** — modify workspace nodes programmatically (disable with `COALESCE_READONLY_MODE=true`):
- *"Update the SQL on this node"*
- *"Create a new stage node"*

## Quick start

### 1. Install

```bash
# Recommended
uvx --from coalesce-mcp coalesce-mcp-server

# Or install globally
uv tool install coalesce-mcp
```

### 2. Get your API token

1. Log into Coalesce
2. Go to **Settings** → **API Tokens**
3. Create a token and copy it

### 3. Configure Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

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

Restart Claude Desktop. You're done.

## Tools

### Failure investigation

| Tool | Description |
|---|---|
| `investigate_failure` | **Best starting point.** Run metadata + failed nodes (with error messages and SQL) + downstream blocked nodes in one call. |
| `list_failed_runs` | List recent failed runs. Use this to find a `run_id` to investigate. |
| `list_job_runs` | List all recent runs with optional filters (status, environment, limit). |
| `get_run_results` | Node-level results for a run — failures and blocked nodes only, succeeded nodes omitted. |
| `get_run` | Full metadata for a specific run. |
| `get_run_status` | Live status via the scheduler endpoint. |
| `get_job_details` | Run metadata + extracted errors combined. |

### Node management

| Tool | Description |
|---|---|
| `list_workspace_nodes` | List all nodes in a workspace. |
| `list_environment_nodes` | List all deployed nodes in an environment. |
| `get_workspace_node` | Full node config — columns, transforms, source mappings, SQL. |
| `get_environment_node` | Same as above for deployed (production) nodes. |
| `create_workspace_node` | Create a new node with defaults. |
| `set_node` | Full replacement update of an existing node. **Read first, then write.** |

## Recommended investigation workflow

```
1. list_failed_runs          → find a run_id
2. investigate_failure       → root cause + downstream impact
3. get_workspace_node        → inspect the failing node's SQL and transforms
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `COALESCE_API_TOKEN` | required | Bearer token from Coalesce Settings |
| `COALESCE_BASE_URL` | `https://app.coalescesoftware.io/api/` | Override for on-prem deployments |
| `COALESCE_READONLY_MODE` | `false` | Set `true` to hide `create_workspace_node` and `set_node` |

## Snowflake Cortex CLI

If you're using the Snowflake Cortex CLI, add this to your `mcp.json`:

```json
{
  "mcpServers": {
    "coalesce": {
      "command": "/path/to/coalesce-mcp-server",
      "env": {
        "COALESCE_API_TOKEN": "your-token-here",
        "COALESCE_READONLY_MODE": "true"
      }
    }
  }
}
```

Set `COALESCE_READONLY_MODE=true` if your agent role is read-only.

## Development

```bash
git clone https://github.com/JarredR092699/coalesce-mcp.git
cd coalesce-mcp

# Install with dev dependencies
uv sync

# Run tests
uv run pytest

# Run against real API (requires .env with COALESCE_API_TOKEN)
uv run python test_debug.py

# Install locally
uv tool install . --force
```

## Requirements

- Python 3.10+
- A Coalesce account with API access

## License

MIT — see [LICENSE](LICENSE)

## Links

- [Issues](https://github.com/JarredR092699/coalesce-mcp/issues)
- [Coalesce API docs](https://docs.coalesce.io/docs/api)
- [Model Context Protocol](https://modelcontextprotocol.io/)
