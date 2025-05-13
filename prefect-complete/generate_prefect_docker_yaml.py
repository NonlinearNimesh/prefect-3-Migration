import os
import re
import yaml
from datetime import datetime, timezone

# Configuration from environment variables
flows_dir = "src/flows"
prefect_yaml_path = "prefect.yaml"
work_pool_name = os.getenv("WORK_POOL_NAME", "Docker")
docker_image = os.getenv("DOCKER_IMAGE", "caringdockers/prefect3:v2.0.13")

def generate_entries():
    flow_entries = []
    deployment_entries = []

    for root, dirs, files in os.walk(flows_dir):
        for filename in files:
            if filename.endswith(".py"):
                flow_name = filename.replace(".py", "")

                match = re.search(r"_(\d+)$", flow_name)
                if not match:
                    print(f"Skipping {filename}: does not end in '_<number>'")
                    continue

                flow_number = match.group(1)
                entrypoint_path = os.path.join(root, filename).replace("\\", "/")

                deployment_name = f"algo.{flow_number}"
                queue_name = f"algo-{flow_number}"

                # Flow entry
                flow_entries.append({
                    "entrypoint": entrypoint_path,
                    "name": flow_name,
                    "version": "1.0",
                    "work_pool": {
                        "name": work_pool_name,
                        "work_queue_name": queue_name,
                        "job_variables": {}
                    }
                })

                # Deployment entry
                deployment_entries.append({
                    "name": deployment_name,
                    "entrypoint": f"{entrypoint_path}:{flow_name}",
                    "work_pool": {
                        "name": work_pool_name,
                        "work_queue_name": queue_name,
                        "job_variables": {
                            "image": docker_image
                        }
                    },
                    "pull": [
                        {
                            "prefect.deployments.steps.set_working_directory": {
                                "directory": "/app"
                            }
                        }
                    ],
                    "schedules": [
                        {
                            "interval": 10.0,
                            "anchor_date": datetime.now(timezone.utc).isoformat(),
                            "timezone": "UTC",
                            "active": False
                        }
                    ],
                    "version": "1.0",
                    "tags": [],
                    "concurrency_limit": 10,
                    "description": f"Deployment of {flow_name.replace('_', ' ').title()} model",
                    "parameters": {}
                })

    return flow_entries, deployment_entries

def generate_prefect_yaml():
    flows, deployments = generate_entries()

    prefect_config = {
        "prefect-version": "3.0.0",
        "name": "my-prefect-project",
        "dependencies": ["requirements.txt"],
        "flows": flows,
        "deployments": deployments
    }

    with open(prefect_yaml_path, "w") as f:
        yaml.dump(prefect_config, f, sort_keys=False)

    print(f"✅ Generated {prefect_yaml_path} with {len(flows)} flows and {len(deployments)} deployments")

if __name__ == "__main__":
    generate_prefect_yaml()

