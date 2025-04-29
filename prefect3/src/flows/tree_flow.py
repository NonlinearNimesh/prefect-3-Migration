from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import time

@task
def root_task():
    print("Starting root task")
    time.sleep(1)
    return "root"

@task
def task_A1(data):
    print(f"Running task A1 with data: {data}")
    time.sleep(2)
    return "A1 done"

@task
def task_A2(data):
    print(f"Running task A2 with data: {data}")
    time.sleep(2)
    return "A2 done"

@task
def task_B1(data):
    print(f"Running task B1 with data: {data}")
    time.sleep(1)
    return "B1 result"

@task
def task_B2(data):
    print(f"Running task B2 with data: {data}")
    time.sleep(1)
    return "B2 result"

@flow(task_runner=ConcurrentTaskRunner())
def tree_flow():
    root_output = root_task()

    # Parallel execution
    a1_future = task_A1.submit(root_output)
    a2_future = task_A2.submit(root_output)

    # Sequential execution
    b1_output = task_B1(root_output)
    b2_output = task_B2(b1_output)

    # Optional: wait for all tasks to complete
    a1_result = a1_future.result()
    a2_result = a2_future.result()

    print("Final outputs:")
    print(a1_result, a2_result, b2_output)

if __name__ == "__main__":
    tree_flow()