import os
import asyncio
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate

os.environ["PREFECT_API_URL"] = "http://prefect-server.carpl.svc.cluster.local:4200/api"

# Load config from environment variables
work_pool_name = "Kubernetes" #os.getenv("WORK_POOL_NAME", "Kubernetes")
namespace = "carpl" #os.getenv("K8S_NAMESPACE", "namespace-prefect")
image = "caringdockers/prefect3:v1.0.45" #os.getenv("WORKER_IMAGE", "caringdockers/prefect3_flow-worker:v1.0.0")
image_pull_policy = "IfNotPresent" #os.getenv("K8S_IMAGE_PULL_POLICY", "IfNotPresent")
service_account_name = "my-service-account-prefect" #os.getenv("K8S_SERVICE_ACCOUNT", "my-service-account-prefect")
finished_job_ttl = 3600 #int(os.getenv("K8S_FINISHED_JOB_TTL", 3600))
job_watch_timeout = 7200 #int(os.getenv("K8S_JOB_WATCH_TIMEOUT", 7200))
pod_watch_timeout = 60 #int(os.getenv("K8S_POD_WATCH_TIMEOUT", 60))
stream_output = True #os.getenv("K8S_STREAM_OUTPUT", "true").lower() == "true"

# Define the job template without cluster_config
base_job_template = {
    "job_configuration": {
        "namespace": "{{ namespace }}",
        "image": "{{ image }}",
        "image_pull_policy": "{{ image_pull_policy }}",
        "service_account_name": "{{ service_account_name }}",
        "finished_job_ttl": "{{ finished_job_ttl }}",
        "job_watch_timeout_seconds": "{{ job_watch_timeout_seconds }}",
        "pod_watch_timeout_seconds": "{{ pod_watch_timeout_seconds }}",
        "stream_output": "{{ stream_output }}",
        "env": "{{ env }}",
        "labels": "{{ labels }}",
        "command": "{{ command }}",
        "name": "{{ name }}",
        "job_manifest": {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "generateName": "{{ name }}-",
                "namespace": "{{ namespace }}",
                "labels": "{{ labels }}"
            },
            "spec": {
                "ttlSecondsAfterFinished": "{{ finished_job_ttl }}",
                "backoffLimit": 0,
                "template": {
                    "spec": {
                        "serviceAccountName": "{{ service_account_name }}",
                        "restartPolicy": "Never",
                        "completions": 1,
                        "parallelism": 1,
                        "containers": [
                            {
                                "name": "prefect-job",
                                "image": "{{ image }}",
                                "imagePullPolicy": "{{ image_pull_policy }}",
                                "args": "{{ command }}",
                                "env": "{{ env }}"
                            }
                        ]
                    }
                }
            }
        }
    },
    "variables": {
        "type": "object",
        "properties": {
            "image": {"type": "string", "title": "Image", "default": image},
            "namespace": {"type": "string", "title": "Namespace", "default": namespace},
            "image_pull_policy": {"type": "string", "title": "Image Pull Policy", "default": image_pull_policy},
            "service_account_name": {"type": "string", "title": "Service Account Name", "default": service_account_name},
            "env": {"type": "object", "title": "Environment Variables", "default": {}},
            "labels": {"type": "object", "title": "Labels", "default": {}},
            "finished_job_ttl": {"type": "integer", "title": "Finished Job TTL", "default": finished_job_ttl},
            "job_watch_timeout_seconds": {"type": "integer", "title": "Job Watch Timeout Seconds", "default": job_watch_timeout},
            "pod_watch_timeout_seconds": {"type": "integer", "title": "Pod Watch Timeout Seconds", "default": pod_watch_timeout},
            "stream_output": {"type": "boolean", "title": "Stream Output", "default": stream_output},
            "command": {
                "type": "string",
                "title": "Command",
                "default": "python -m prefect.engine"
            },
            "name": {
                "type": "string",
                "title": "Job Name",
                "default": "prefect-job"
            }
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

if __name__ == "__main__":
    asyncio.run(delete_and_create_k8s_work_pool())