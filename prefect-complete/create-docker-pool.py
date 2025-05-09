# import asyncio
# import os
# from prefect.client.orchestration import get_client
# from prefect.client.schemas.actions import WorkPoolCreate

# async def create_docker_pool():
#     # Get values from environment variables
#     work_pool_name = os.getenv('WORK_POOL_INFRA', 'Docker')  # Default to 'Docker' if env var is not set

#     image_value = os.getenv('WORKER_IMAGE', 'caringdockers/prefect3:v1.0.7')
#     env_value = os.getenv('DOCKER_ENV', '{"MY_ENV_VAR": "example"}')  # Default as JSON string
#     labels_value = os.getenv('DOCKER_LABELS', '{"env": "test"}')  # Default as JSON string
#     volumes_value = os.getenv('DOCKER_VOLUMES', '["/var/run/docker.sock:/var/run/docker.sock"]')  # Default as JSON string
#     auto_remove_value = os.getenv('DOCKER_AUTO_REMOVE', 'True') == 'True'  # Convert to boolean
#     network_mode_value = os.getenv('DOCKER_NETWORK_MODE', 'bridge')
#     stream_output_value = os.getenv('DOCKER_STREAM_OUTPUT', 'True') == 'True'  # Convert to boolean
#     image_pull_policy_value = os.getenv('DOCKER_IMAGE_PULL_POLICY', 'IfNotPresent')
#     privileged_value = os.getenv('DOCKER_PRIVILEGED', 'False') == 'True'  # Privileged flag

#     # Parse the environment and labels from JSON strings
#     import json
#     env_value = json.loads(env_value)
#     labels_value = json.loads(labels_value)
#     volumes_value = json.loads(volumes_value)

#     base_job_template = {
#         "job_configuration": {
#             "image": image_value,
#             "env": env_value,
#             "labels": labels_value,
#             "volumes": volumes_value,
#             "auto_remove": auto_remove_value,
#             "network_mode": network_mode_value,
#             "stream_output": stream_output_value,
#             "image_pull_policy": image_pull_policy_value,
#             "privileged": privileged_value  # Add privileged flag
#         },
#         "variables": {
#             "type": "object",
#             "properties": {
#                 "image": {
#                     "type": "string",
#                     "title": "Image",
#                     "default": image_value
#                 },
#                 "env": {
#                     "type": "object",
#                     "additionalProperties": {"type": "string"},
#                     "title": "Environment Variables",
#                     "default": env_value
#                 },
#                 "labels": {
#                     "type": "object",
#                     "additionalProperties": {"type": "string"},
#                     "title": "Labels",
#                     "default": labels_value
#                 },
#                 "volumes": {
#                     "type": "array",
#                     "items": {"type": "string"},
#                     "title": "Volumes",
#                     "default": volumes_value
#                 },
#                 "auto_remove": {
#                     "type": "boolean",
#                     "title": "Auto Remove",
#                     "default": auto_remove_value
#                 },
#                 "network_mode": {
#                     "type": "string",
#                     "title": "Network Mode",
#                     "default": network_mode_value
#                 },
#                 "stream_output": {
#                     "type": "boolean",
#                     "title": "Stream Output",
#                     "default": stream_output_value
#                 },
#                 "image_pull_policy": {
#                     "type": "string",
#                     "title": "Image Pull Policy",
#                     "default": image_pull_policy_value
#                 },
#                 "privileged": {
#                     "type": "boolean",
#                     "title": "Privileged",
#                     "default": privileged_value
#                 }
#             },
#             "required": ["image"],
#             "additionalProperties": True
#         }
#     }

#     # Create work pool asynchronously
#     async with get_client() as client:
#         existing_pools = await client.read_work_pools()
#         if any(pool.name == work_pool_name for pool in existing_pools):
#             print(f"⚠️ Docker work pool '{work_pool_name}' already exists.")
#         else:
#             await client.create_work_pool(
#                 work_pool=WorkPoolCreate(
#                     name=work_pool_name,
#                     type="docker",
#                     base_job_template=base_job_template
#                 )
#             )
#             print(f"✅ Docker work pool '{work_pool_name}' created with UI-visible defaults.")

# if __name__ == "__main__":
#     asyncio.run(create_docker_pool())




import asyncio
import os
import json
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate

async def create_or_overwrite_docker_pool():
    # Get values from environment variables
    work_pool_name = os.getenv('WORK_POOL_INFRA', 'Docker')  # Default to 'Docker' if not set

    image_value = os.getenv('WORKER_IMAGE', 'caringdockers/prefect3:v1.0.7')
    env_value = os.getenv('DOCKER_ENV', '{"MY_ENV_VAR": "example"}')
    labels_value = os.getenv('DOCKER_LABELS', '{"env": "test"}')
    volumes_value = os.getenv('DOCKER_VOLUMES', '["/var/run/docker.sock:/var/run/docker.sock"]')
    auto_remove_value = os.getenv('DOCKER_AUTO_REMOVE', 'True') == 'True'
    network_mode_value = os.getenv('DOCKER_NETWORK_MODE', 'bridge')
    stream_output_value = os.getenv('DOCKER_STREAM_OUTPUT', 'True') == 'True'
    image_pull_policy_value = os.getenv('DOCKER_IMAGE_PULL_POLICY', 'IfNotPresent')
    privileged_value = os.getenv('DOCKER_PRIVILEGED', 'False') == 'True'

    # Parse JSON string values
    env_value = json.loads(env_value)
    labels_value = json.loads(labels_value)
    volumes_value = json.loads(volumes_value)

    base_job_template = {
        "job_configuration": {
            "image": image_value,
            "env": env_value,
            "labels": labels_value,
            "volumes": volumes_value,
            "auto_remove": auto_remove_value,
            "network_mode": network_mode_value,
            "stream_output": stream_output_value,
            "image_pull_policy": image_pull_policy_value,
            "privileged": privileged_value
        },
        "variables": {
            "type": "object",
            "properties": {
                "image": {"type": "string", "title": "Image", "default": image_value},
                "env": {"type": "object", "additionalProperties": {"type": "string"}, "title": "Environment Variables", "default": env_value},
                "labels": {"type": "object", "additionalProperties": {"type": "string"}, "title": "Labels", "default": labels_value},
                "volumes": {"type": "array", "items": {"type": "string"}, "title": "Volumes", "default": volumes_value},
                "auto_remove": {"type": "boolean", "title": "Auto Remove", "default": auto_remove_value},
                "network_mode": {"type": "string", "title": "Network Mode", "default": network_mode_value},
                "stream_output": {"type": "boolean", "title": "Stream Output", "default": stream_output_value},
                "image_pull_policy": {"type": "string", "title": "Image Pull Policy", "default": image_pull_policy_value},
                "privileged": {"type": "boolean", "title": "Privileged", "default": privileged_value}
            },
            "required": ["image"],
            "additionalProperties": True
        }
    }

    async with get_client() as client:
        try:
            existing_pool = await client.read_work_pool(work_pool_name)
            print(f"⚠️ Work pool '{work_pool_name}' exists. Deleting it...")
            await client.delete_work_pool(work_pool_name)
        except Exception:
            print(f"ℹ️ Work pool '{work_pool_name}' does not exist. Creating new one.")

        await client.create_work_pool(
            work_pool=WorkPoolCreate(
                name=work_pool_name,
                type="docker",
                base_job_template=base_job_template
            )
        )
        print(f"✅ Docker work pool '{work_pool_name}' created or overwritten.")

if __name__ == "__main__":
    asyncio.run(create_or_overwrite_docker_pool())
