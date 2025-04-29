import os
import yaml

# Configuration
flows_dir = "src/flows"
prefect_yaml_path = "prefect.yaml"
work_pool_name = "my-docker-pool"

# Helper to generate flow and deployment entries
def generate_entries():
    flow_entries = []
    deployment_entries = []

    for filename in os.listdir(flows_dir):
        if filename.endswith(".py"):
            flow_name = filename.replace(".py", "")
            entrypoint = f"{flows_dir}/{filename}"
            queue_name = f"{flow_name}-queue"
            deployment_name = f"{flow_name}-deployment"

            # Flow entry
            flow_entries.append({
                "entrypoint": entrypoint,
                "name": flow_name,
                "version": "1.0",
                "work_pool": {
                    "name": work_pool_name,
                    "work_queue_name": queue_name,
                    "job_variables": {}
                }
            })

            # Deployment entry
            deployment = {
                "name": deployment_name,
                "entrypoint": f"{entrypoint}:{flow_name}",
                "work_pool": {
                    "name": work_pool_name,
                    "work_queue_name": queue_name
                },
                "pull": [
                {
                    "prefect.deployments.steps.set_working_directory": {
                        "directory": "/app/src"
                    }
                }
            ]
            }

            # Skip scheduling if the flow is job_flow
            if flow_name != "job_flow":
                deployment["schedule"] = {
                    "interval": 10,
                    "active": True
                }

            deployment_entries.append(deployment)

    return flow_entries, deployment_entries

# Generate and write prefect.yaml
def generate_prefect_yaml():
    flows, deployments = generate_entries()

    prefect_config = {
        "prefect-version": "3.0.0",
        "name": "my-prefect-project",
        "dependencies": ["requirements.txt"],
        "flows": flows,
        "deployments": deployments,
        "infrastructure": {
            "type": "process",
            "env": {},
            "labels": [],
            "name": "process-infra"
        }
    }

    with open(prefect_yaml_path, "w") as f:
        yaml.dump(prefect_config, f, sort_keys=False)

generate_prefect_yaml()

