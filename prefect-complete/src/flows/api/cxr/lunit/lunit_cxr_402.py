#Insight-CXR-By-Lunit

import warnings
warnings.filterwarnings('ignore')
from dotenv import load_dotenv
load_dotenv()
from prefect import flow, task
import requests
from lunit_helper import *
import time
import os, sys
import boto3
from botocore.client import Config
from prefect.futures import PrefectFuture
import traceback
from src.tasks import comman_tasks
from src.utils import save_logs
from src.utils import utils


INFERENCING_SERVICE_NAME = "s3"
AWS_BUCKET_NAME = os.environ.get("BUCKET_NAME")
REGION_NAME = os.environ.get("AWS_REGION_NAME")  # "ap-south-1"
SEND_IP = os.environ.get("SEND_LOG_IP")  #
TEMP_PATH = os.environ.get("TEMP_FOLDER_PATH")  # "/tmp"
HGW_URL = os.environ.get("SEND_HGW_URL")  #
TMP_FOLDER_PATH = TEMP_PATH
ALGO_402_BASE_URL = os.environ.get("ALGO_402_BASE_URL","")
ALGO_402_BEARER_TOKEN = os.environ.get("ALGO_402_BEARER_TOKEN","")
ALGO_402_POST_URL = ALGO_402_BASE_URL + "/dcm/"
ALGO_402_PREDICT_URL = ALGO_402_BASE_URL + "/models/latest/predict/"
request_logger = utils.setup_request_logger("Lunit-CXR-Inferencing")



@flow(name="lunit_cxr_402")
def lunit_cxr_402(job_id: str) -> dict:
    global request_logger
    global logger
    logger = save_logs.setup_logger(job_id)
    algo_id = "402"
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>")
    print(algo_id, job_id)
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>")

    future1 = comman_tasks.recieve_inference_request.submit(algo_id, job_id)
    state = future1.result()
    print("This is the state status recieve inference requrest ", state)
    # if not state.get("task_1"):
    #     return {"status": "failed", "job_id": job_id}

    future2 = comman_tasks.get_file.submit(state)
    state = future2.result()
    print("This is the state status after get file ", state)
    # if not state.get("task_1"):
    #     return {"status": "failed", "job_id": job_id}






































    return {"status": "success", "job_id": job_id}
