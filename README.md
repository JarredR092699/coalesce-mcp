# Coalesce MCP Server

A Model Context Protocol (MCP) server for investigating Coalesce data pipeline failures interactively with Claude.

## Overview

This MCP server provides tools to investigate Coalesce job runs, making it easy for data engineers to diagnose pipeline failures directly in Claude Desktop or Claude.ai. Ask Claude about your Coalesce pipelines in natural language and get detailed error analysis, node-level results, and actionable recommendations.

**What you can do:**
- List recent job runs and failures
- Get detailed error messages for failed runs
- Investigate node-level execution results
- Understand what SQL was executed
- Identify root causes of pipeline failures

**All operations are read-only** - no jobs are triggered or modified.

## Installation

### Using pip

```bash
pip install coalesce-mcp
```

### Using uv (recommended)

```bash
uv tool install coalesce-mcp
```

## Configuration

### Get Your Coalesce API Token

1. Log into Coalesce
2. Go to **Settings** → **API Tokens**
3. Create a new token with read access
4. Copy the token (you'll need it for configuration)

### Configure Claude Desktop

#### macOS

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "coalesce": {
      "command": "coalesce-mcp",
      "env": {
        "COALESCE_API_TOKEN": "your-api-token-here",
        "COALESCE_BASE_URL": "https://app.coalescesoftware.io/api"
      }
    }
  }
}
```

#### Windows

Edit `%APPDATA%\Claude\claude_desktop_config.json` with the same configuration as above.

#### If using uv

```json
{
  "mcpServers": {
    "coalesce": {
      "command": "uvx",
      "args": ["coalesce-mcp"],
      "env": {
        "COALESCE_API_TOKEN": "your-api-token-here",
        "COALESCE_BASE_URL": "https://app.coalescesoftware.io/api"
      }
    }
  }
}
```

### Restart Claude Desktop

After updating the configuration, restart Claude Desktop to load the MCP server.

## Available Tools

The server provides 6 tools for investigating Coalesce job runs:

### 1. `list_job_runs`

Lists recent job runs with optional filters.

**Parameters:**
- `environment_id` (optional): Filter by environment ID
- `run_status` (optional): Filter by status - `running`, `completed`, `failed`, `canceled`
- `limit` (optional): Maximum number of runs to return (default 50)
- `starting_from` (optional): Cursor for pagination

**Example prompts:**
- "Show me recent Coalesce runs"
- "List all completed runs from the last week"

### 2. `list_failed_runs`

Convenience tool that filters for failed runs only.

**Parameters:**
- `environment_id` (optional): Filter by environment ID
- `limit` (optional): Maximum number of failed runs (default 20)
- `starting_from` (optional): Cursor for pagination

**Example prompts:**
- "What jobs have failed recently?"
- "Show me all failed runs"

### 3. `get_run`

Gets full details for a specific run.

**Parameters:**
- `run_id` (required): The ID of the run to retrieve

**Example prompts:**
- "Get details for run 79458"
- "Show me information about run 12345"

### 4. `get_run_status`

Gets the current status of a run.

**Parameters:**
- `run_id` (required): The ID of the run to check

**Example prompts:**
- "Is run 79458 still running?"
- "What's the status of run 12345?"

### 5. `get_run_results`

Gets node-level execution results.

**Parameters:**
- `run_id` (required): The ID of the run to get results for

**Example prompts:**
- "Show me which nodes failed in run 79458"
- "What are the execution results for run 12345?"

### 6. `get_job_details` (RECOMMENDED)

Combines all information about a run in one call - status, results, and extracted error details.

**Parameters:**
- `run_id` (required): The ID of the run to investigate

**Example prompts:**
- "Investigate failure in run 79458"
- "What went wrong with run 12345?"
- "Analyze run 79458 and explain the error"

## Usage Examples

Once installed and configured, you can ask Claude questions like:

```
"Can you list recent failed Coalesce runs?"

"What went wrong with run 79458?"

"Show me the error messages from the latest failed run"

"Which nodes failed in run 79458 and what were the error messages?"

"Investigate the most recent failure and explain what happened"

"List all runs from the production environment"
```

Claude will use the appropriate tools to answer your questions and provide detailed analysis.

## Requirements

- **Python:** 3.10 or higher
- **Coalesce account:** With API access
- **API token:** Read access to job runs

## Security

- **API tokens are environment variables** - never hardcoded
- **Each user provides their own token** - decentralized authentication
- **Read-only operations** - no jobs are triggered or modified
- **No logging of credentials** - tokens are kept secure

## Troubleshooting

### MCP Server Not Detected

1. Verify the configuration path is correct for your OS
2. Check that the JSON is valid (no trailing commas)
3. Restart Claude Desktop after configuration changes
4. Check Claude Desktop logs for error messages

### Authentication Errors

1. Verify your API token is correct
2. Check that the token has read access to job runs
3. Ensure `COALESCE_BASE_URL` is set correctly

### Command Not Found

If `coalesce-mcp` command is not found:

```bash
# Using pip
pip install --force-reinstall coalesce-mcp

# Using uv
uv tool install --force coalesce-mcp
```

## Development

To contribute or modify the server:

```bash
# Clone the repository
git clone https://github.com/yourusername/coalesce-mcp.git
cd coalesce-mcp

# Install dependencies
pip install -e .

# Run tests
pytest
```

## License

MIT License - see LICENSE file for details

## Support

- **Issues:** https://github.com/yourusername/coalesce-mcp/issues
- **Coalesce API Docs:** https://docs.coalesce.io/docs/api

## Credits

Built with the [Model Context Protocol](https://modelcontextprotocol.io/) for Claude AI integration.
