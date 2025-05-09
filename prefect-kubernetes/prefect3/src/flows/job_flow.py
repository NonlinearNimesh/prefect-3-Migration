from prefect import flow, task

@task
def task_1(jobid: str):
    print(f"Task 1 received jobid: {jobid}")

@task
def task_2(jobid: str):
    print(f"Task 2 received jobid: {jobid}")

@task
def task_3(jobid: str):
    print(f"Task 3 received jobid: {jobid}")

@task
def task_4(jobid: str):
    print(f"Task 4 received jobid: {jobid}")

@task
def task_5(jobid: str):
    print(f"Task 5 received jobid: {jobid}")

@task
def task_6(jobid: str):
    print(f"Task 6 received jobid: {jobid}")

@flow
def job_flow(jobid: str):
    task_1(jobid)
    task_2(jobid)
    task_3(jobid)
    task_4(jobid)
    task_5(jobid)
    task_6(jobid)

if __name__ == "__main__":
    job_flow(jobid="manual-test-id")

