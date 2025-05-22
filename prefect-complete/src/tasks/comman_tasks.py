import traceback
from src.utils import utils
from prefect import task
import traceback, os
from src.utils import save_logs

SEND_IP = os.environ.get("SEND_LOG_IP")  # "platform-django:5000"
TEMP_PATH = os.environ.get("TEMP_FOLDER_PATH")  # "/tmp"
HGW_URL = os.environ.get("SEND_HGW_URL")  # "http://hgw-in-api:8080/response"
TMP_FOLDER_PATH = TEMP_PATH
INPUT_FOLDER_PATH = os.environ.get("INPUT_FOLDER_PATH", "/root/prefect_input_folder")
INFERENCE_AUTHENTICATION_TOKEN=os.environ.get("INFERENCE_AUTHENTICATION_TOKEN")
ADD_WATERMARK = os.environ.get("ADD_WATERMARK", "False")
FHIR_OUTPUT=os.environ.get("FHIR_OUTPUT", "False")
HTTPS_ENABLED = os.environ.get("HTTPS_ENABLED","False")
CREATE_HL7_PROBABILITY_REPORT = int(os.environ.get("CREATE_HL7_PROBABILITY_REPORT", 1))
CREATE_HL7_ABNORMALITY_REPORT = int(os.environ.get("CREATE_HL7_ABNORMALITY_REPORT", 1))
CREATE_HL7_AI_REPORT = int(os.environ.get("CREATE_HL7_AI_REPORT", 1))
HL7_VERSION = os.environ.get("HL7_VERSION", "2.5")
SC_STORE_PATH = os.environ.get("SC_STORE_PATH", "")


def common_logger(state, template, *args):
    formatted_string = template % args
    logger = save_logs.setup_logger(state["job_id"])
    request_logger = utils.setup_request_logger(state["algo_id"],state["job_id"])
    logger.debug(formatted_string)
    request_logger.debug(formatted_string)

@task(retries=1, retry_delay_seconds=30)
def recieve_inference_request(algo_id, jid):
    print("this is the algo id  jid ", algo_id, jid)
    state = {"task_1_message": "task_1_request_response"}
    return state
    logger = save_logs.setup_logger(jid)
    state["job_id"] = jid
    state["algo_id"] = algo_id

    try:
        print("In Get Job Id Task -> ", state)

        utils.send_log(
            job_id=jid,
            header="AI Gateway",
            subheader="File received at AI gateway",
            status="success",
            level=5,
            direction=False,
            send_ip=SEND_IP,
            msg_id=5
        )

        common_logger(state, "Task 1 Generate Job_id ===> %s", state)

        log_status = utils.credit_management_logs(
            job_id=jid,
            send_ip=SEND_IP,
            false_param=None,
            failure_reason="",
            error_log=""
        )

        state["task_1"] = True
        return state

    except Exception as e:
        error_trace = traceback.format_exc()
        print("Generate Jobid Exception\n\n", error_trace, "\n\nGenerate Jobid Exception")

        common_logger(state, "Task 1 Generate Job_id Exception ===> %s", e)

        utils.send_log(
            job_id=jid,
            header="AI Gateway",
            subheader="Failed at AI Gateway",
            status="success",
            level=5,
            direction=False,
            send_ip=SEND_IP,
            msg_id=5
        )

        log_status = utils.credit_management_logs(
            job_id=jid,
            send_ip=SEND_IP,
            false_param='recieve_at_aigw',
            failure_reason=f"Failed while receiving at AIGW - {e}",
            error_log=error_trace
        )

        state["task_1"] = False
        return state

@task(retries=1, retry_delay_seconds=30)
def get_file(state):
    try:
        print("This is the state we get from first state ", state)
        state = {"task_2_message": "task_2_request_response"}
        return state
        state, status = utils.fetching_input_dicoms(state, algo_type = "lunit")
        utils.print_logs("Task 2 Get FIle block ===> %s ", state)
        state["task_2"] = True
        return state
    except Exception as e:
        utils.print_logs("Task 2 Get FIle Exception block ===> %s ", traceback.format_exc())
        state["task_2"] = False
        return False
