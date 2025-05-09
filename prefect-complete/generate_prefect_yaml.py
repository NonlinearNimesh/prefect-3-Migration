import os
import re
import yaml

# Configuration
flows_dir = "src/flows"
prefect_yaml_path = "prefect.yaml"
work_pool_name = "Process"

def generate_entries():
    flow_entries = []
    deployment_entries = []

    for root, dirs, files in os.walk(flows_dir):
        for filename in files:
            if filename.endswith(".py"):
                flow_name = filename.replace(".py", "")

                # Extract trailing number from file name (e.g., chexpert_cxr_131)
                match = re.search(r"_(\d+)$", flow_name)
                if not match:
                    print(f"Skipping {filename}: does not end in '_<number>'")
                    continue

                flow_number = match.group(1)
                entrypoint_path = os.path.join(root, filename)

                deployment_name = f"algo.{flow_number}"
                queue_name = f"algo-{flow_number}"

                # Flow entry
                flow_entries.append({
                    "entrypoint": entrypoint_path,
                    "name": flow_name,
                    "work_pool": {
                        "name": work_pool_name,
                        "work_queue_name": queue_name,
                        "job_variables": {}
                    }
                })

                # Deployment entry
                deployment = {
                    "name": deployment_name,
                    "entrypoint": f"{entrypoint_path}:{flow_name}",
                    "work_pool": {
                        "name": work_pool_name,
                        "work_queue_name": queue_name
                    },
                    "pull": [
                        {
                            "prefect.deployments.steps.set_working_directory": {
                                "directory": "/app"
                            }
                        }
                    ],
                    "schedule": {
                        "interval": 10,
                        "active": False
                    }
                }

                deployment_entries.append(deployment)

    return flow_entries, deployment_entries

def generate_prefect_yaml():
    flows, deployments = generate_entries()

    prefect_config = {
        "prefect-version": "3.0.0",
        "name": "my-prefect-project",
        "dependencies": ["requirements.txt"],
        "flows": flows,
        "deployments": deployments,
        "infrastructure": {
            "type": "docker",
            "env": {},
            "labels": [],
            "name": "docker-infra"
        }
    }

    with open(prefect_yaml_path, "w") as f:
        yaml.dump(prefect_config, f, sort_keys=False)

generate_prefect_yaml()
