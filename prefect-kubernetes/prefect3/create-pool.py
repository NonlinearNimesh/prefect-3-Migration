import asyncio
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect_kubernetes.jobs import KubernetesJob
from prefect_kubernetes.credentials import KubernetesCredentials, KubernetesClusterConfig
from kubernetes import client as k8s_client


import subprocess

def start_prefect_worker(pool_name):
    try:
        # Start the Prefect worker using the subprocess
        subprocess.run(
            ["prefect", "worker", "start", "--pool", pool_name],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        print(f"✅ Prefect worker started for pool '{pool_name}'")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error starting Prefect worker: {e}")
        print(f"stderr: {e.stderr}")
        print(f"stdout: {e.stdout}")

# Example usage
start_prefect_worker("my-k8s-pool-auto")




async def create_k8s_workpool_and_block():
    work_pool_name = "my-k8s-pool-auto"
    block_name = "prefect-k8s-block"

    # Load the Kubernetes cluster config
    kube_config = await KubernetesClusterConfig.load("prefect-k8s-block")  # Use await here
    
    # Create Kubernetes Credentials instance
    credentials = KubernetesCredentials(kube_config=kube_config)

    # Define Kubernetes Job spec (v1_job)
    job_spec = k8s_client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=k8s_client.V1ObjectMeta(name="prefect-job"),
        spec=k8s_client.V1JobSpec(
            template=k8s_client.V1PodTemplateSpec(
                spec=k8s_client.V1PodSpec(
                    containers=[
                        k8s_client.V1Container(
                            name="prefect-container",
                            image="caringdockers/prefect3_flow-worker:v1.0.0",
                            image_pull_policy="IfNotPresent",
                        )
                    ],
                    restart_policy="Never",
                )
            )
        )
    )

    # Convert the V1Job object to a dictionary
    job_spec_dict = job_spec.to_dict()

    # Create KubernetesJob infrastructure block with the proper settings
    infra_block = KubernetesJob(
        image="caringdockers/prefect3_flow-worker:v1.0.0",
        namespace="namespace-prefect",
        service_account_name="my-service-account-prefect",
        image_pull_policy="IfNotPresent",
        finished_job_ttl=18400,
        job_watch_timeout_seconds=7200,
        pod_watch_timeout_seconds=18400,
        stream_output=True,
        cluster_config=kube_config,
        credentials=credentials,
        v1_job=job_spec_dict,  # Pass the job spec as a dictionary
    )
    
    # Save the Kubernetes block (properly awaiting async operation)
    await infra_block.save(name=block_name, overwrite=True)
    print(f"✅ Saved Kubernetes block: {block_name}")

    # Create Kubernetes Work Pool asynchronously
    async with get_client() as client:
        existing_pools = await client.read_work_pools()
        if any(pool.name == work_pool_name for pool in existing_pools):
            print(f"⚠️ Work pool '{work_pool_name}' already exists.")
        else:
            await client.create_work_pool(
                work_pool=WorkPoolCreate(
                    name=work_pool_name,
                    type="kubernetes",
                    base_job_template={},
                )
            )
            print(f"✅ Created Kubernetes work pool: {work_pool_name}")
            start_prefect_worker(work_pool_name)

# Run the function asynchronously
asyncio.run(create_k8s_workpool_and_block())

