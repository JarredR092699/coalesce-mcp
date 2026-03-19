"""Quick test script — run with: uv run python test_tools.py"""
import asyncio
import os
import sys

# Set token from env or pass as arg: python test_tools.py <token> <run_id>
token = sys.argv[1] if len(sys.argv) > 1 else os.getenv("COALESCE_API_TOKEN", "")
run_id = sys.argv[2] if len(sys.argv) > 2 else None

os.environ["COALESCE_API_TOKEN"] = token

from coalesce_mcp.client import list_failed_runs, investigate_failure, get_run_results, get_job_details


async def main():
    print("=" * 60)
    print("1. list_failed_runs (limit=3)")
    print("=" * 60)
    result = await list_failed_runs(limit=3)
    print(result)

    if run_id:
        print("\n" + "=" * 60)
        print(f"2. investigate_failure (run_id={run_id})")
        print("=" * 60)
        result = await investigate_failure(run_id)
        print(result)

        print("\n" + "=" * 60)
        print(f"3. get_job_details (run_id={run_id})")
        print("=" * 60)
        result = await get_job_details(run_id)
        print(result)
    else:
        print("\n(Pass a run_id as second arg to test investigate_failure and get_job_details)")
        print("Usage: uv run python test_tools.py <token> <run_id>")


asyncio.run(main())
