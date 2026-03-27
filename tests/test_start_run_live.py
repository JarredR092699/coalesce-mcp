"""Live integration test for start_run, retry_run, and cancel_run.

Hits the real Coalesce API. Requires environment variables to be set.

Usage (PowerShell):
    $env:COALESCE_API_TOKEN = "your-token-here"
    $env:COALESCE_ENVIRONMENT_ID = "your-environment-id"
    $env:COALESCE_JOB_ID = "your-job-id"   # optional
    uv run python tests/test_start_run_live.py

Or with inline env vars:
    $env:COALESCE_API_TOKEN="tok"; $env:COALESCE_ENVIRONMENT_ID="env-1"; uv run python tests/test_start_run_live.py
"""

import asyncio
import json
import os
import sys


async def main():
    token = os.getenv("COALESCE_API_TOKEN")
    environment_id = os.getenv("COALESCE_ENVIRONMENT_ID")
    job_id = os.getenv("COALESCE_JOB_ID")  # optional

    if not token or not environment_id:
        print("ERROR: Set COALESCE_API_TOKEN and COALESCE_ENVIRONMENT_ID before running.")
        print()
        print("PowerShell:")
        print('  $env:COALESCE_API_TOKEN = "your-token-here"')
        print('  $env:COALESCE_ENVIRONMENT_ID = "your-environment-id"')
        print('  uv run python tests/test_start_run_live.py')
        sys.exit(1)

    # Import here so env vars are set first
    from coalesce_mcp.client import start_run_tool, retry_run_tool, cancel_run_tool, get_run_status

    print("=" * 60)
    print("Coalesce Live API Test — start_run / retry_run / cancel_run")
    print("=" * 60)
    print(f"Environment ID : {environment_id}")
    print(f"Job ID         : {job_id or '(none — full env refresh)'}")
    print()

    # -------------------------------------------------------------------------
    # Test 1: start_run
    # -------------------------------------------------------------------------
    print("--- Test 1: start_run ---")
    result_json = await start_run_tool(environment_id, job_id=job_id)
    result = json.loads(result_json)
    print(json.dumps(result, indent=2))

    if "error" in result:
        print("\nstart_run failed — check token/environment_id and try again.")
        sys.exit(1)

    run_id = result.get("run_id")
    print(f"\nRun started. run_id = {run_id}")

    # -------------------------------------------------------------------------
    # Test 2: get_run_status (verify the run is actually running)
    # -------------------------------------------------------------------------
    print("\n--- Test 2: get_run_status ---")
    status_json = await get_run_status(str(run_id))
    status = json.loads(status_json)
    print(json.dumps(status, indent=2))

    # -------------------------------------------------------------------------
    # Test 3: cancel_run (stop it immediately so we don't burn credits)
    # -------------------------------------------------------------------------
    print(f"\n--- Test 3: cancel_run (run_id={run_id}) ---")
    cancel_json = await cancel_run_tool(str(run_id))
    cancel = json.loads(cancel_json)
    print(json.dumps(cancel, indent=2))

    if cancel.get("success"):
        print("\nAll 3 tests passed.")
        print("retry_run not tested here (requires an existing failed run_id).")
        print("To test retry_run, run:")
        print(f'  uv run python -c "')
        print(f'  import asyncio, json, os')
        print(f'  os.environ[\'COALESCE_API_TOKEN\'] = \'your-token\'')
        print(f'  from coalesce_mcp.client import retry_run_tool')
        print(f'  r = asyncio.run(retry_run_tool(\'<failed-run-id>\'))')
        print(f'  print(r)"')
    else:
        print("\ncancel_run may have failed — check the response above.")


if __name__ == "__main__":
    asyncio.run(main())
