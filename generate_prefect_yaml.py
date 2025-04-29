import os
import yaml
import ast

PROJECT_NAME = "my-prefect-project"
FLOWS_DIR = "src/flows"
DEPENDENCIES = ["requirements.txt"]
BUILD_PATH = "src"

def find_flow_functions(file_path):
    with open(file_path, "r") as f:
        node = ast.parse(f.read(), filename=file_path)

    flows = []
    for n in ast.walk(node):
        if isinstance(n, ast.FunctionDef):
            for decorator in n.decorator_list:
                if isinstance(decorator, ast.Name) and decorator.id == "flow":
                    flows.append(n.name)
                elif isinstance(decorator, ast.Call) and getattr(decorator.func, 'id', '') == "flow":
                    flows.append(n.name)
    return flows

def main():
    flows = []

    # Iterate over all files in the FLOWS_DIR to find the flow functions
    for root, _, files in os.walk(FLOWS_DIR):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                flow_functions = find_flow_functions(file_path)
                rel_path = os.path.relpath(file_path, start=".")
                for func in flow_functions:
                    flows.append({
                        "entrypoint": f"{rel_path}:{func}"
                    })

    prefect_yaml = {
        "prefect-version": "3.0.0",
        "name": PROJECT_NAME,
        "flows": flows,
        "dependencies": DEPENDENCIES,
        "build": {
            "path": BUILD_PATH
        }
    }

    # Save to the prefect.yaml file
    with open("prefect.yaml", "w") as f:
        yaml.dump(prefect_yaml, f, sort_keys=False)

    print("✅ prefect.yaml generated successfully!")

if __name__ == "__main__":
    main()

