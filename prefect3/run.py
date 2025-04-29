import asyncio
from prefect.client import get_client
from prefect.deployments import run_deployment
import os
async def trigger_job_flow(job_id: str):
    os.environ["PREFECT_API_URL"] = "http://prefect-server:4200/api"

    async with get_client() as client:
        flow_run = await run_deployment(
            name="job-flow/job_flow-deployment",
            parameters={"jobid": job_id}
        )
        print(f"Triggered flow run with ID: {flow_run.id}")

# Example usage
if __name__ == "__main__":
    asyncio.run(trigger_job_flow("1234567890qwertyuiopasdfghjklmnbzxcvbnm"))

