import asyncio
from prefect.client import get_client
from prefect.deployments import run_deployment
import os
import uuid
from concurrent.futures import ThreadPoolExecutor


async def trigger_job_flow(job_id: str):
    os.environ["PREFECT_API_URL"] = "http://prefect-server:4200/api"
    print(f"[{job_id}] Setting Prefect API URL and triggering deployment...")
    
    try:
        async with get_client() as client:
            print(f"[{job_id}] Connected to Prefect API")
            flow_run = await run_deployment(
                name="lunit_cxr_402/algo.402",
                parameters={"job_id": job_id}
            )
            print(f"[{job_id}] Flow run triggered: {flow_run}")
            print(f"[{job_id}] Flow run ID: {flow_run.id}")
    except Exception as e:
        print(f"[{job_id}] Error during flow trigger: {e}")


def run_worker(worker_id: int):
    job_id = str(uuid.uuid4())  # Generate unique job ID for each worker
    print(f"[Worker {worker_id}] Starting with job_id: {job_id}")
    
    try:
        asyncio.run(trigger_job_flow(job_id))
        print(f"[Worker {worker_id}] Finished successfully")
    except Exception as e:
        print(f"[Worker {worker_id}] Exception in event loop: {e}")


if __name__ == "__main__":
    print("[Main] Starting ThreadPoolExecutor")
    with ThreadPoolExecutor(max_workers=8) as executor:
        for i in range(8):
            executor.submit(run_worker, i)
    print("[Main] All tasks submitted")

