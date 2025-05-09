import os
import asyncio
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate

# Load config from environment variables
work_pool_name = os.getenv("WORK_POOL_NAME", "Kubernetes")
namespace = os.getenv("K8S_NAMESPACE", "namespace-prefect")
image = os.getenv("WORKER_IMAGE", "caringdockers/prefect3_flow-worker:v1.0.0")
image_pull_policy = os.getenv("K8S_IMAGE_PULL_POLICY", "IfNotPresent")
service_account_name = os.getenv("K8S_SERVICE_ACCOUNT", "my-service-account-prefect")
finished_job_ttl = int(os.getenv("K8S_FINISHED_JOB_TTL", 3600))
job_watch_timeout = int(os.getenv("K8S_JOB_WATCH_TIMEOUT", 7200))
pod_watch_timeout = int(os.getenv("K8S_POD_WATCH_TIMEOUT", 60))
stream_output = os.getenv("K8S_STREAM_OUTPUT", "true").lower() == "true"

# Define base_job_template with image visible in UI
base_job_template = {
    "job_configuration": {
        "namespace": namespace,
        "image": image,
        "image_pull_policy": image_pull_policy,
        "service_account_name": service_account_name,
        "finished_job_ttl": finished_job_ttl,
        "job_watch_timeout_seconds": job_watch_timeout,
        "pod_watch_timeout_seconds": pod_watch_timeout,
        "stream_output": stream_output,
        "env": {},
        "labels": {},
    },
    "variables": {
        "type": "object",
        "properties": {
            "image": {
                "type": "string",
                "title": "Image",
                "default": image,
                "description": "The container image to use for flow runs."
            },
            "namespace": {
                "type": "string",
                "title": "Namespace",
                "default": namespace
            },
            "image_pull_policy": {
                "type": "string",
                "title": "Image Pull Policy",
                "default": image_pull_policy
            },
            "service_account_name": {
                "type": "string",
                "title": "Service Account Name",
                "default": service_account_name
            },
            "env": {
                "type": "object",
                "title": "Environment Variables",
                "default": {}
            },
            "labels": {
                "type": "object",
                "title": "Labels",
                "default": {}
            },
            "finished_job_ttl": {
                "type": "integer",
                "title": "Finished Job TTL",
                "default": finished_job_ttl
            },
            "job_watch_timeout_seconds": {
                "type": "integer",
                "title": "Job Watch Timeout Seconds",
                "default": job_watch_timeout
            },
            "pod_watch_timeout_seconds": {
                "type": "integer",
                "title": "Pod Watch Timeout Seconds",
                "default": pod_watch_timeout
            },
            "stream_output": {
                "type": "boolean",
                "title": "Stream Output",
                "default": stream_output
            },
        },
        "required": ["image", "namespace"]
    }
}

async def delete_and_create_k8s_work_pool():
    async with get_client() as client:
        existing_pools = await client.read_work_pools()
        if any(p.name == work_pool_name for p in existing_pools):
            print(f"🗑️ Deleting existing work pool: {work_pool_name}")
            await client.delete_work_pool(work_pool_name)
        else:
            print(f"✅ No existing work pool found. Creating new: {work_pool_name}")

        await client.create_work_pool(
            work_pool=WorkPoolCreate(
                name=work_pool_name,
                type="kubernetes",
                base_job_template=base_job_template
            )
        )
        print(f"🚀 Created new Kubernetes work pool: {work_pool_name}")

# Run the async function
if __name__ == "__main__":
    asyncio.run(delete_and_create_k8s_work_pool())
