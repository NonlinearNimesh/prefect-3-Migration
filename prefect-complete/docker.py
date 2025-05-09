import os
import re
import yaml

# Configuration
flows_dir = "src/flows"
prefect_yaml_path = "prefect.yaml"
work_pool_name = "Docker"

# Docker Work Pool Config
docker_job_variables = {
    "image": "caringdockers/prefect3:v1.0.3",
    "env": {},
    "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
    "network_mode": "bridge",
    "auto_remove": True,
    "stream_output": True
}

def generate_entries():
    flow_entries = []
    deployment_entries = []

    for root, _, files in os.walk(flows_dir):
        for filename in files:
            if filename.endswith(".py"):
                flow_name = filename.replace(".py", "")

                # Match trailing number (e.g., chexpert_cxr_131)
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
                    "work_pool": {
                        "name": work_pool_name,
                        "work_queue_name": queue_name,
                        "job_variables": docker_job_variables  # Docker config here
                    }
                })

                # Deployment entry
                deployment_entries.append({
                    "name": deployment_name,
                    "entrypoint": f"{entrypoint_path}:{flow_name}",
                    "work_pool": {
                        "name": work_pool_name,
                        "work_queue_name": queue_name,
                        "job_variables": docker_job_variables
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

if __name__ == "__main__":
    generate_prefect_yaml()

