from prefect import flow, task
from prefect.client import get_client
from prefect.settings import PREFECT_API_URL
import asyncio

@task
async def read_prefect_variable(var_name: str, default=None):
    try:
        async with get_client() as client:
            variable = await client.read_project_variable(name=var_name)
            print(f"✅ Variable '{var_name}' fetched from Prefect: {variable}")
            return variable
    except Exception as e:
        print(f"⚠️ Failed to fetch variable '{var_name}', using default: {default}")
        return default

@task
def use_variable(var_value):
    print(f"📦 Using variable: {var_value}")
    return f"Used variable value: {var_value}"

@flow(name="variableFlow")
def variable_flow():
    # Use a variable defined in Prefect Cloud/UI or via CLI/API
    var_name = "TEST_VAR"
    default_val = "default_value"

    var_value = read_prefect_variable(var_name, default_val)
    result = use_variable(var_value)

    return result

if __name__ == "__main__":
    variable_flow()

