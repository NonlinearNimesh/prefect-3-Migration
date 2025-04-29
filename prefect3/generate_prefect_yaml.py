import os
import yaml
from pathlib import Path

def get_flow_files(flows_dir):
    return [f for f in Path(flows_dir).glob("*.py") if f.is_file()]

def snake_to_kebab(s):
    return s.replace("_", "-")

def build_prefect_yaml(flows_dir="src/flows", schedule_interval=10):
    flow_files = get_flow_files(flows_dir)

    flows = []
    deployments = []

    for file in flow_files:
        flow_name = file.stem
        queue_name = f"{flow_name}-queue"
        deployment_name = f"{flow_name}-deployment"

        flows.append({
            "entrypoint": f"{flows_dir}/{file.name}",
            "name": flow_name,
            "version": "1.0",
            "work_pool": {
                "name": "my-docker-pool",
                "work_queue_name": queue_name,
                "job_variables": {}
            }
        })

        deployments.append({
            "name": deployment_name,
            "entrypoint": f"{flows_dir}/{file.name}:{flow_name}",
            "work_pool": {
                "name": "my-docker-pool",
                "work_queue_name": queue_name
            },
            "pull": False,
            "schedule": {
                "interval": schedule_interval,
                "active": True
            }
        })

    prefect_yaml = {
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

    with open("prefect.yaml", "w") as f:
        yaml.dump(prefect_yaml, f, sort_keys=False)

    print("✅ Generated prefect.yaml")

if __name__ == "__main__":
    build_prefect_yaml()

