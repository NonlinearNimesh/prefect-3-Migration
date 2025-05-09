from prefect import flow, task

@task
def task_1(state: dict) -> dict:
    state["jobid"] += "-task_1"
    print(f"Task 1 received jobid: {state['jobid']}")
    return state

@task
def task_2(state: dict) -> dict:
    state["jobid"] += "-task_2"
    print(f"Task 2 received jobid: {state['jobid']}")
    return state

@task
def task_3(state: dict) -> dict:
    state["jobid"] += "-task_3"
    print(f"Task 3 received jobid: {state['jobid']}")
    return state

@task
def task_4(state: dict) -> dict:
    state["jobid"] += "-task_4"
    print(f"Task 4 received jobid: {state['jobid']}")
    return state

@task
def task_5(state: dict) -> dict:
    state["jobid"] += "-task_5"
    print(f"Task 5 received jobid: {state['jobid']}")
    return state

@task
def task_6(state: dict) -> dict:
    state["jobid"] += "-task_6"
    print(f"Task 6 received jobid: {state['jobid']}")
    return state

@flow(name="chexpert_cxr_131")
def chexpert_cxr_131(jobid: str) -> dict:
    state = {"jobid": jobid}
    state = task_1(state)
    state = task_2(state)
    state = task_3(state)
    state = task_4(state)
    state = task_5(state)
    state = task_6(state)
    return state
