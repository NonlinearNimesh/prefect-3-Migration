import asyncio
from prefect.client import get_client
from prefect.deployments import run_deployment
import os
import uuid
from concurrent.futures import ThreadPoolExecutor


async def trigger_job_flow(job_id: str):
    os.environ["PREFECT_API_URL"] = "http://prefect-server:4200/api"

    async with get_client() as client:
        flow_run = await run_deployment(
            name="chexpert_cxr_131/algo.131",
            parameters={"jobid": job_id}
        )
        print(f"Triggered flow run with ID: {flow_run.id} for job ID: {job_id}")

def run_worker(worker_id: int):
    job_id = str(uuid.uuid4())  # Generate unique job ID for each worker
    print(f"Worker {worker_id} starting with job_id: {job_id}")
    
    # Run the async function in the asyncio event loop
    asyncio.run(trigger_job_flow(job_id))
    print(f"Worker {worker_id} finished")

if __name__ == "__main__":
    # Create a ThreadPoolExecutor to run 8 workers concurrently
    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit 8 worker tasks to the executor
        for i in range(8):
            executor.submit(run_worker, i)


