from prefect import flow, task
import tqdm
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image, ImageDraw, ImageFont
import io
from pydicom.pixel_data_handlers.util import convert_color_space
from pydicom.dataset import Dataset
import zipfile
import os, json, requests
import traceback
import botocore
from botocore.exceptions import ClientError
from botocore.client import Config
from dotenv import load_dotenv

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
    logger = setup_logger(state["job_id"])
    request_logger = setup_request_logger(state["algo_id"],state["job_id"])
    logger.debug(formatted_string)
    request_logger.debug(formatted_string)

def print_logs(template, *args):
    formatted_string = template % args
    logger.debug(formatted_string)
    request_logger.debug(formatted_string)


CLOUD_ENVIRONMENT = os.environ.get('CLOUD_ENVIRONMENT', "LOCAL")

if CLOUD_ENVIRONMENT == "AWS":
    import boto3
    INFERENCING_SERVICE_NAME=os.environ.get('INFERENCING_SERVICE_NAME')
    AWS_BUCKET_NAME=os.environ.get('AWS_BUCKET_NAME')
    AWS_REGION_NAME=os.environ.get('AWS_REGION_NAME')
    session = boto3.session.Session()
    s3_client = session.client(
                    service_name=INFERENCING_SERVICE_NAME,
                    config=Config(connect_timeout=10, signature_version='s3v4', s3={'addressing_style': 'path'}),
                    region_name=AWS_REGION_NAME
            )
    BUCKET_NAME = AWS_BUCKET_NAME
    LICENSE_BUCKET_NAME=os.environ.get("LICENSE_BUCKET_NAME")
    LICENSE_BUCKET_REGION=os.environ.get("LICENSE_BUCKET_REGION")
    session = boto3.session.Session()
    s3_license_client = session.client(
                        service_name=INFERENCING_SERVICE_NAME,
                        config=Config(connect_timeout=10, signature_version='s3v4', s3={'addressing_style': 'path'}),
                        region_name=LICENSE_BUCKET_REGION
                )

elif CLOUD_ENVIRONMENT == "GCS" or CLOUD_ENVIRONMENT == "HEALTHCARE_API":
    from google.cloud import storage
    storage_client = storage.Client()
    GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS","/opt/prefect/flows/temp_gcp.json")
    GCP_BUCKET_NAME_JSON = os.environ.get("GCP_BUCKET_NAME_JSON", "duploservices-hiae-prod-ai-store-carpl-platform")
    GCP_BUCKET_NAME_DCM = os.environ.get("GCP_BUCKET_NAME_DCM", "duploservices-hiae-prod-dicomstore-carpl-platform")
    GCP_BUCKET_NAME_LICENSE = os.environ.get("GCP_BUCKET_NAME_LICENSE", "duploservices-hiae-prod-license-carpl-platform")
    BUCKET_NAME = GCP_BUCKET_NAME_JSON
    LICENSE_BUCKET_NAME = GCP_BUCKET_NAME_LICENSE


else:
    from minio import Minio
    CARPL_MINI_BUCKET_NAME = os.environ.get("MINI_BUCKET_NAME")
    minio_client = Minio(
            endpoint="minio:9000",
            access_key="c@rpl@c@ring",
            secret_key="c@rpl@c@ring",
            secure=False,
        )
    BUCKET_NAME = CARPL_MINI_BUCKET_NAME

print(f'CLOUD_ENVIRONMENT : {CLOUD_ENVIRONMENT}')
print(f'BUCKET_NAME : {BUCKET_NAME}')

def send_log(job_id, header, subheader, status, level, direction, send_ip, msg_id=-1):
    try:
        dict_log = {"header": header,
                    "subheader": subheader,
                    "status": status,
                    "level": level,
                    "direction": direction,
                    "msg_id": msg_id}
        print(dict_log)
        payload = json.dumps(dict_log)
        headers = {
                'Content-Type': 'application/json',
                'Token': INFERENCE_AUTHENTICATION_TOKEN
            }
        response = requests.request(
            "post",
            f"http://{send_ip}/api/v2/inference/log/{job_id}",
            headers=headers,
            data=payload,
        )
    except Exception as e:
        print(f'There is some error at {traceback.format_exc()}')


def fetching_input_dicoms(state, algo_type="normal"):
    logger = setup_logger(state["job_id"])
    try:
        pid = ""
        print("Getting File ................ ")
        if CLOUD_ENVIRONMENT == "HEALTHCARE_API" and "study_path" in state:
            if algo_type == "kubernetes":
                status = unzip_file_healthcare(state["study_path"], state['job_id'], INPUT_FOLDER_PATH)
            else:
                status = unzip_file_healthcare(state['study_path'], state['job_id'], TEMP_PATH)
        else:
            if algo_type == "kubernetes":
                status = unzip_file(TEMP_PATH + state["job_id"] + ".json", state['job_id'], INPUT_FOLDER_PATH)
            elif algo_type == "lunit":
                status, pid = unzip_file_lunit(TEMP_PATH + state["job_id"] + ".json", state['job_id'], TEMP_PATH)
            # elif CLOUD_ENVIRONMENT == "GCS":
            #     status = unzip_file_gcs(TEMP_PATH + state["job_id"] + ".json", state['job_id'], TEMP_PATH)
            else:
                status = unzip_file(TEMP_PATH + state["job_id"] + ".json", state['job_id'], TEMP_PATH)

        if status != "failed":
            state["unzip_path"] = status
            state["original_patient_id"] = pid
            send_log(job_id=state["job_id"], header="AI Gateway", subheader="Started Inferencing at AI gateway",
                     status="success", level=5, direction=False, send_ip=SEND_IP, msg_id=6)
            common_logger(state, "Task 2 Get FIle ===> %s ", state)
            state["task_2"] = True
            return state, True
        print("In Get File Task -> ", state)
        send_log(job_id=state["job_id"], header="AI Gateway", subheader="Failed to get Inferecing Data",
                 status="failed", level=5, direction=False, send_ip=SEND_IP, msg_id=6)
        common_logger(state, "Task 2 Get FIle Failed block ===> %s ", state)
        state["task_2"] = False
        return state, False
    except Exception as e:
        common_logger(state, "Task 2 Get FIle Exception block ===> %s ", traceback.format_exc())
        send_log(job_id=state["job_id"], header="AI Gateway", subheader="Failed to get Inferecing Data",
                 status="failed", level=5, direction=False, send_ip=SEND_IP, msg_id=6)
        state["task_2"] = False
        # log_status = credit_management_logs(job_id = state["job_id"], inference_initiated = True, uploading_s3 = True, recieve_at_hgw = True, sent_to_aigw = True, recieve_at_aigw = True, preprocessing = False, sent_to_aiserver = False, inference_result_recieved = False, postprocessing = False, result_send_to_hgw = False, result_recieve_to_hgw = False, result_sending_to_hospital = False, result_recieve_at_hospital = False, inferencing_completed = False, failure_reason = "Failed to Get the Inferencing Data " + str(e), error_log = str(traceback.format_exc()), send_ip=SEND_IP)
        log_status = credit_management_logs(
            job_id=state["job_id"],
            send_ip=SEND_IP,
            false_param='preprocessing',
            failure_reason=f"Failed while getting the Inferencing Data at AIGW - {e}",
            error_log=str(traceback.format_exc()),
        )
        return state, False


def unzip_file_healthcare(study_path, job_id, file_path):
    try:
        print("Goolge application creds", GOOGLE_APPLICATION_CREDENTIALS)
        credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS,
                                                                            scopes=SCOPES)
        print("credentialssssssssss", credentials)
        study_url = f"https://healthcare.googleapis.com/v1/{study_path}/instances"
        credentials.refresh(Request())
        access_token = credentials.token
        print("access token", access_token)
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        response = requests.get(study_url, headers=headers)
        response.raise_for_status()

        metrics = response.json()
        instances_list = []
        for ind_metrics in metrics:
            sop_uid = ind_metrics["00080018"]["Value"][0]
            series_uid = ind_metrics["0020000E"]["Value"][0]
            modality = ind_metrics["00080060"]["Value"][0]
            sopclassuid = ind_metrics["00080016"]["Value"][0]
            if modality in ["SC", "SR", "OT", "PR"]:
                continue
            if sopclassuid == "1.2.840.10008.5.1.4.1.1.7":
                continue
            instance = [series_uid, sop_uid]
            instances_list.append(instance)
        failed_downloads = []
        func = partial(download_instance, credentials, study_path, file_path, job_id)
        with tqdm.tqdm(desc="Downloading images from Google Healthcare API", total=len(instances_list)) as pbar:
            with ThreadPoolExecutor(max_workers=32) as executor:
                futures = {executor.submit(func, instance): instance for instance in instances_list}
                for future in as_completed(futures):
                    if future.exception():
                        failed_downloads.append(futures[future])
                    pbar.update(1)
        print("Download File Completed")
        return str(os.path.join(file_path, job_id))
    except Exception:
        print(traceback.format_exc())
        return "failed"


def unzip_file(filepath, jobID , output):
    print("In UNZIP FILE >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n\n")
    #if not os.path.exists(filepath):
    print(output, "This is Output in unzip file")
    print("job ID: ", jobID)
    print("Filepath: ", filepath)
    result, type_ = initiate_download(jobID, filepath)
    print(type_)
    print(type(jobID))
    if result:
        if type_ == "JSON":
            fl = open(filepath.split(".")[0] + ".json", "r")
            data = json.loads(fl.read())
            #print("This is the JSON ..................", data)
            fl.close()
            try:
                os.makedirs(os.path.join(output, jobID))
            except Exception as e:
                print("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", e)
                pass
            fetch_scans(data, output, jobID)
            pass
        else:
            print(os.getcwd())
            print(">>>>>>>>",filepath)
            try:
                os.makedirs(output)
            except:
                pass
            with ZipFile(filepath, 'r') as zip:
                zip.printdir()
                zip.extractall(path = os.path.join(output, jobID))
                print("Extracted at",output, end =("/"+jobID))
            os.remove(filepath)
    return output+"/"+jobID


def download_file(object_name, bucket, file_name):
    if object_name is None:
        object_name = os.path.basename(file_name)

    if CLOUD_ENVIRONMENT == "AWS":
        try:
            response = s3_client.download_file(bucket, object_name, file_name)
        except ClientError as e:
            print(e,
                  "In download files >******************************************************************************************************")
            return False
        return True

    elif CLOUD_ENVIRONMENT == "GCS":
        try:
            storage_client = storage.Client()
            print("In Download File ")
            if object_name is None:
                object_name = os.path.basename(file_name)
                print("In download file gcs This is the object name - ", object_name)
            try:
                bucket = storage_client.bucket(bucket)
                blob = bucket.blob(object_name)
                blob.download_to_filename(file_name)
                print("The bucket is ", bucket, "and blob is ", blob)
            except Exception as e:
                print(str(traceback.format_exc()), "In download files Exception")
                return False
            return True
        except Exception as e:
            print("download_file_gcs Exception\n\n ", traceback.format_exc())

    else:
        try:
            file_path = os.path.join('tmp/', file_name)
            minio_client.fget_object(bucket, object_name, file_path)
            print("File downloaded successfully!")
            return True
        except Exception as err:
            print(err)
            return False


def key_exists(mykey, mybucket):
    if CLOUD_ENVIRONMENT == "AWS":
        response = s3_client.list_objects_v2(Bucket=mybucket, Prefix=mykey)
        try:
            if response:
                for obj in response['Contents']:
                    if mykey == obj['Key']:
                        return True
        except:
            pass
        return False

    elif CLOUD_ENVIRONMENT == "GCS":
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(mybucket)
            print("In Key Exist")
            blobs = storage_client.list_blobs(mybucket, prefix=mykey)
            print("In Key Exist -  blobs", blobs)
            print("In Key Exist -  mykey", mykey)
            print("In Key Exist -  bucket", mybucket)
            for blob in blobs:
                if blob.name == mykey:
                    print("In Key Exist -  inside If blob.name snd mykey", blob.name, mykey)
                    return True, blob
            return False, ""
        except Exception as e:
            print("key_exists_gcs traceback\n\n", traceback.format_exc())

    else:
        if True:
            response = minio_client.list_objects(mybucket, prefix=mykey, recursive=True)
            print("MINIO ", response)
            try:
                if response:
                    for obj in response:
                        if mykey == obj.object_name.strip():
                            print("..............Minio Key Matched ...................")
                            return True
                        else:
                            print("\n\n NOT MATCHED \n\n", mykey, obj.object_name)
                            return False
            except Exception as e:
                print(e)
                print("Minio Key Matched traceback....................", traceback.format_exc())
                return False
        else:
            pass


def initiate_download(job_id, final_path):
    if CLOUD_ENVIRONMENT == "AWS":
        print(len(job_id), ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>THIS IS THE LEN OF JOB ID>>>>>>>>>>>>>>>>>>>>>",
              "BUCKET-NAME", BUCKET_NAME)
        print(key_exists("middlelayer/" + job_id + ".json", BUCKET_NAME))
        try:
            if key_exists("middlelayer/" + job_id + ".json", BUCKET_NAME):
                download_file("middlelayer/" + job_id + ".json", BUCKET_NAME, final_path.split(".")[0] + ".json")
                return True, "JSON"
            elif key_exists("middlelayer/" + job_id + ".zip", BUCKET_NAME):
                download_file("middlelayer/" + job_id + ".zip", BUCKET_NAME, final_path)
                return True, "ZIP"
        except Exception as e:
            print(e)
        return False, ""

    elif CLOUD_ENVIRONMENT == "GCS":
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            print("This is the storage client ", storage_client, "and bucket is ", bucket)
            st, key_exist_json = key_exists('middlelayer/' + job_id + ".json", BUCKET_NAME)
            print("This is the status response from key exists gcs ", st, "and json is ", key_exist_json)
            print(key_exist_json)
            if st:
                print("This is the Json Path", final_path.split(".")[0] + ".json", "Json Path")
                download_file('middlelayer/' + job_id + ".json", BUCKET_NAME,
                              final_path.split(".")[0] + ".json")  # 'middlelayer/'+job_id + ".json"
                return True, "JSON"
            elif key_exists('middlelayer/' + job_id + ".zip", BUCKET_NAME):
                # download_file_gcs(storage_client, 'middlelayer/'+job_id + ".zip", GCP_BUCKET_NAME, final_path)
                return True, "ZIP"
            return False, ""
        except Exception as e:
            print("initiate_download_gcs Exception \n\n", traceback.format_exc())

    else:
        print(len(job_id), ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>THIS IS THE LEN OF JOB ID>>>>>>>>>>>>>>>>>>>>>",
              "BUCKET-NAME", BUCKET_NAME)
        print(key_exists("input_" + job_id + ".json", BUCKET_NAME))
        try:
            if key_exists("input_" + job_id + ".json", BUCKET_NAME):
                download_file("input_" + job_id + ".json", BUCKET_NAME, final_path.split(".")[0] + ".json")
                return True, "JSON"
            elif key_exists("input_" + job_id + ".zip", BUCKET_NAME):
                download_file("input_" + job_id + ".zip", BUCKET_NAME, final_path)
                return True, "ZIP"
        except Exception as e:
            print(e)
        return False, ""


def download_from_minio(filename, jobid, output):
    print("This is filename", filename)
    print("This is JobID", jobid)
    try:
        minio_client.fget_object(
            "micaring",
            filename,
            output + "/" + jobid + "/" + filename,  # Set desired file path and name here
        )
        print("File downloaded successfully!")
    except Exception as err:
        import traceback
        print(err)
        print(traceback.format_exc())


def fetch_scans(data, output, jobID):
    try:
        if CLOUD_ENVIRONMENT == "AWS":
            file_names = []
            if data["DEPLOYMENT_TYPE"] == "INFER":
                for key in data["instances"]:
                    f = f"{key[0]}_{key[1]}_{key[2]}.dcm"
                    print(f"File Name >>>>>>>>> {f}")
                    file_names.append(f)
                session = boto3.session.Session()
                s3_client = session.client(
                    service_name='s3',
                    config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
                    region_name=data['credentials']['Region']
                )
                failed_downloads = []
                func = partial(download_mulit_file, data['credentials']['BucketName'], output, jobID)
                with tqdm.tqdm(desc="Downloading images from S3 Infer", total=len(file_names)) as pbar:
                    with ThreadPoolExecutor(max_workers=32) as executor:
                        # Using a dict for preserving the downloaded file for each future, to store it as a failure if we need that
                        futures = {
                            executor.submit(func, file_to_download): file_to_download for file_to_download in file_names
                        }
                        for future in as_completed(futures):
                            if future.exception():
                                failed_downloads.append(futures[future])
                            pbar.update(1)
                print("Download File Completed")
                # for fl in file_names:
                #     try:
                #         print(fl, ">>>>>>>>>>>>>>>>>>>>", output, ">>>>>>>>>>>>>>>>>>>>", jobID, ">>>>>>>>>>>>>>>>", fl)
                #         download_file(fl, data['credentials']['BucketName'], output, jobID, fl)
                #     except Exception as e:
                #         print("Object not found ", fl)
            if data["DEPLOYMENT_TYPE"] in ["Standalone", "Standard"]:
                for key_ in data["uuid_mapping"].keys():
                    file_names.append(data["uuid_mapping"][key_] + ".dcm")
                func = partial(download_mulit_file, data['credentials']['BucketName'], output, jobID)
                with tqdm.tqdm(desc="Downloading images from S3 Standard/Standalone", total=len(file_names)) as pbar:
                    with ThreadPoolExecutor(max_workers=32) as executor:
                        # Using a dict for preserving the downloaded file for each future, to store it as a failure if we need that
                        futures = {
                            executor.submit(func, file_to_download): file_to_download for file_to_download in file_names
                        }
                        for future in as_completed(futures):
                            if future.exception():
                                failed_downloads.append(futures[future])
                            pbar.update(1)
                            # for fl in file_names:
                #     try:
                #         download_file(fl, data['credentials']['BucketName'], os.path.join(output, jobID, fl))
                #     except Exception as e:
                #         print("Object not found ", fl)

        elif CLOUD_ENVIRONMENT == "GCS":
            try:
                file_names = []
                if data["DEPLOYMENT_TYPE"] in ["Standalone", "Standard"]:
                    for key_ in data["uuid_mapping"].keys():
                        # print("This is the Key in uuid Mapping for loop", key_, "and mapping is ", data["uuid_mapping"].keys())
                        file_names.append(data["uuid_mapping"][key_] + ".dcm")
                    func = partial(download_mulit_file, GCP_BUCKET_NAME_DCM, output, jobID)
                    with tqdm.tqdm(desc="Downloading images from GCS Infer", total=len(file_names)) as pbar:
                        with ThreadPoolExecutor(max_workers=32) as executor:
                            # Using a dict for preserving the downloaded file for each future, to store it as a failure if we need that
                            futures = {
                                executor.submit(func, file_to_download): file_to_download for file_to_download in
                                file_names
                            }
                            for future in as_completed(futures):
                                if future.exception():
                                    failed_downloads.append(futures[future])
                                pbar.update(1)
                    print("Download File Completed")
                    # bucket = storage_client.bucket(GCP_BUCKET_NAME_DCM)
                    # for fl in file_names:
                    #     try:
                    #         blob = bucket.blob(fl)
                    #         blob.download_to_filename(os.path.join(output, jobID, fl))
                    #     except Exception as e:
                    #         print("Object not found ", fl)
                    #         print(" fetch_scans_gcs Object not found  \n\n", traceback.format_exc())
                    #         print("fetch_scans_gcs Object not found", e)
                if data["DEPLOYMENT_TYPE"] == "INFER":
                    for key in data["instances"]:
                        f = f"{key[0]}_{key[1]}_{key[2]}.dcm"
                        print(f"File Name >>>>>>>>> {f}")
                        file_names.append(f)
                    func = partial(download_mulit_file, GCP_BUCKET_NAME_DCM, output, jobID)
                    with tqdm.tqdm(desc="Downloading images from GCS Infer", total=len(file_names)) as pbar:
                        with ThreadPoolExecutor(max_workers=32) as executor:
                            # Using a dict for preserving the downloaded file for each future, to store it as a failure if we need that
                            futures = {
                                executor.submit(func, file_to_download): file_to_download for file_to_download in
                                file_names
                            }
                            for future in as_completed(futures):
                                if future.exception():
                                    failed_downloads.append(futures[future])
                                pbar.update(1)
                    print("Download File Completed")

            except Exception as e:
                print("  fetch_scans_gcs Object not found  Exception \n\n", traceback.format_exc())
        else:
            file_names = []
            if data["DEPLOYMENT_TYPE"] == "INFER":
                for key in data["instances"]:
                    file_names.append(key)
                for fl in file_names:
                    try:
                        print(fl, ">>>>>>>>>>>>>>>>>>>>", output, ">>>>>>>>>>>>>>>>>>>>", jobID, ">>>>>>>>>>>>>>>>", fl)
                        minio_client.fget_object(data['credentials']['BucketName'], fl, os.path.join(output, jobID, fl))
                    except Exception as e:
                        print("Object not found ", fl)

            if data["DEPLOYMENT_TYPE"] in ["Standalone", "Standard"]:
                for key_ in data["uuid_mapping"].keys():
                    file_names.append(data["uuid_mapping"][key_] + ".dcm")
                for fl in file_names:
                    download_from_minio(fl, jobID, output)
                    print(fl, "Done")

    except Exception as e:
        print("fetch_scans_gcs\n\n", traceback.format_exc())


def unzip_file_lunit(filepath, jobID, output):
    original_patient_id = ""
    print("In UNZIP FILE >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n\n")
    print(output, "This is Output in unzip file")
    print("job ID: ", jobID)
    print("Filepath: ", filepath)
    result, type_ = initiate_download(jobID, filepath)
    print(type_)
    print(type(jobID))
    if result:
        if type_ == "JSON":
            fl = open(filepath.split(".")[0] + ".json", "r")
            data = json.loads(fl.read())
            try:
                print("\n\n This is the original patient ID ------->>>>>>>\n\n", data["patient_id"])
                original_patient_id = data["patient_id"]
            except Exception as e:
                print("Printing the patient ID Exception ---------->>>>>>>>>>>", e)
                print(traceback.format_exc())
            fl.close()
            try:
                os.makedirs(os.path.join(output, jobID))
            except Exception as e:
                print("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", e)
                pass
            fetch_scans(data, output, jobID)
            pass
        else:
            print(os.getcwd())
            print(">>>>>>>>", filepath)
            try:
                os.makedirs(output)
            except:
                pass
            with ZipFile(filepath, 'r') as zip:
                zip.printdir()
                zip.extractall(path=os.path.join(output, jobID))
                print("Extracted at", output, end=("/" + jobID))
            os.remove(filepath)
    return output + "/" + jobID, original_patient_id


def download_mulit_file(bucket, output, jobID, object_name):
    file_name = os.path.join(output, jobID, object_name)
    # print("studies are here >>> ", str(file_name))
    # if object_name is None:
    #     object_name = os.path.basename(file_name)

    if CLOUD_ENVIRONMENT == "AWS":
        try:
            response = s3_client.download_file(bucket, object_name, file_name)
        except ClientError as e:
            print(e,
                  "In download files >******************************************************************************************************")
            return False
        return True

    elif CLOUD_ENVIRONMENT == "GCS":
        try:
            bucket = storage_client.bucket(bucket)
            blob = bucket.blob(object_name)
            blob.download_to_filename(file_name)
        except Exception as e:
            print(e, "In download files *********************************")
            return False
        return True

    else:
        try:
            file_path = os.path.join('tmp/', file_name)
            minio_client.fget_object(bucket, object_name, file_path)
            print("File downloaded successfully!")
            return True
        except Exception as err:
            print(err)
            return False


def setup_request_logger(name, job_id=None):
    import logging
    import logstash
    from logging.handlers import TimedRotatingFileHandler
    TEMP_PATH = os.environ.get("TEMP_FOLDER_PATH")
    try:
        filename=f"{TEMP_PATH}/carpl_logger.log"
        if os.path.exists(filename):
            print(f"The file '{filename}' exists.")
        else:
            print(f"The file '{filename}' does not exist.")
            with open(filename, 'a') as file:
                pass
        host = os.environ['LOGSTASH_HOST']
        print("Logstash Host ", host)
        host_port = int(os.environ['LOGSTASH_HOST_PORT'])
        print("Logstash Host Port", host_port)
        # request_logger = logging.getLogger(__name__)
        if job_id:
            name = f"{name}--{job_id}"
        request_logger = logging.getLogger(name)
        print("Request Logger ", request_logger)
        local_file_logs=bool(os.environ.get('LOCAL_LOGS_WRITE',True))
        print("Local file logs ", local_file_logs)
        if local_file_logs:
            formatter = logging.basicConfig(filename=f"{TEMP_PATH}/carpl_logger.log",
                                filemode='a',
                                format='%(asctime)s %(name)s %(levelname)s-%(message)s',
                                datefmt='%d-%m-%Y %H:%M:%S')
            print("Formatter ", formatter)
            handler = TimedRotatingFileHandler(f'{TEMP_PATH}/carpl_logger.log',
                                            when='midnight',
                                            backupCount=100)
            print("Handler ", handler)
            request_logger.addHandler(handler)
        request_logger.setLevel(logging.DEBUG)
        request_logger.addHandler(logstash.TCPLogstashHandler(host, host_port, version=1))
        return request_logger
    except Exception as e:
        print("This is the Logstash Error ", traceback.format_exc())
        return traceback.format_exc()


def credit_management_logs(job_id, send_ip, false_param, failure_reason=None, error_log=None):
    try:
        parameters = [
            "inference_initiated", "uploading_s3", "recieve_at_hgw",
            "sent_to_aigw", "recieve_at_aigw", "preprocessing",
            "sent_to_aiserver", "inference_result_recieved",
            "postprocessing", "result_send_to_hgw", "result_recieve_to_hgw",
            "result_sending_to_hospital", "result_recieve_at_hospital",
            "inferencing_completed", "failure_reason", "error_log"
        ]
        headers = {'Content-Type': 'application/json'}
        payload = {param: True for param in parameters}
        if false_param is not None:
            payload.update({param: param != false_param for param in parameters})
            payload.update({param: False for param in parameters[parameters.index(false_param) + 1:]})

        payload['failure_reason'] = failure_reason
        payload['error_log'] = error_log
        payload["job_id"] = job_id

        payload = json.dumps(payload)
        print(payload)
        response = requests.request(
            "POST",
            f"http://{send_ip}/api/v2/inference/credit_manager_logs", headers=headers,
            data=payload,
        )

        return True
    except Exception as e:
        print(f'There is some error at {traceback.format_exc()}')
        return traceback.format_exc()
