from fastapi import FastAPI, HTTPException, Request
from contextlib import asynccontextmanager
import logging
import requests
import subprocess
from pydantic import BaseModel
from typing import List
from pathlib import Path
import os
import shutil
import git
from DVCManager import DVCManager
from DVCWorker import DVCWorker
from typing import Dict
from LoggerManager import LoggerManager
from kubernetes import client, config
from DVCManager import DVCManager
from DVCWorker import DVCWorker
from typing import Dict
from LoggerManager import LoggerManager
from DagManager import DagManager
from fastapi.responses import JSONResponse
import yaml  # 解析 DVC 檔案
import shutil
import re  # 引入正則表達式套件

import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd

from minio import Minio
from minio.error import S3Error
import uuid

import time
import json

# 设置基本的日志配置
logging.basicConfig(level=logging.DEBUG)

SERVER_MANAGER_URL = "http://10.52.52.136:8000"

MACHINE_ID = "machine_server_1"
MACHINE_IP = "10.52.52.136"
MACHINE_PORT = 8085
MACHINE_CAPACITY = 2

class RegisterRequest(BaseModel):
    machine_id: str
    ip: str
    port: int
    capacity: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.debug("Entering lifespan startup")
    register_data = RegisterRequest(
        machine_id=MACHINE_ID,
        ip=MACHINE_IP,
        port=MACHINE_PORT,
        capacity=MACHINE_CAPACITY
    )

    try:
        # #　去跟servermanager註冊自己的ip與資源用量
        # logging.debug("Sending registration request to Server Manager")
        # response = requests.post(f"{SERVER_MANAGER_URL}/register", json=register_data.dict())
        # logging.debug(f"Registration request response status: {response.status_code}")
        # response.raise_for_status()
        logging.info("Registered with Server Manager successfully")
    except requests.RequestException as e:
        logging.error(f"Failed to register with Server Manager: {e}")
        logging.debug(f"Response: {e.response.text if e.response else 'No response'}")

    yield

    logging.debug("Entering lifespan shutdown")
    try:
        # #　去跟servermanager 清除自己的ip與資料
        # response = requests.post(f"{SERVER_MANAGER_URL}/cleanup", json=register_data.dict())
        # response.raise_for_status()
        logging.info("Resources cleaned up successfully")
    except requests.RequestException as e:
        logging.error(f"Failed to clean up resources: {e}")



app = FastAPI(lifespan=lifespan)

# # PVC 掛載路徑
# STORAGE_PATH = "/mnt/storage"

# PVC 掛載路徑 (本地測試用)
STORAGE_PATH = "/mnt/storage/test/test_inference"


# MinIO 設定
MINIO_URL = "10.52.52.138:31000"
MINIO_ACCESS_KEY = "testdvctominio"
MINIO_SECRET_KEY = "testdvctominio"
BUCKET_NAME = "mock-dataset"


# 定義 Request Body Schema
class CreateFolderRequest(BaseModel):
    dag_id: str
    execution_id: str

class DagRequest(BaseModel):
    DAG_ID: str
    EXECUTION_ID: str
    TASK_STAGE_TYPE: str
    DATASET_NAME: str
    DATASET_VERSION: str
    CODE_REPO_URL: Dict[str, str]
    IMAGE_NAME: Dict[str, str]
    MODEL_NAME: str
    MODEL_VERSION: str
    DEPLOYER_NAME: str
    DEPLOYER_EMAIL: str


# 檢查 PVC 是否已掛載
def is_pvc_mounted():
    return os.path.exists(STORAGE_PATH) and os.path.ismount(STORAGE_PATH)

# 創建資料夾（如果不存在）
def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

# MinIO 客戶端初始化
def init_minio_client():
    return Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # 如果 MinIO 沒有 SSL，設置為 False
    )

# 解析 DVC 檔案並獲取 outs 路徑
def parse_dvc_file(dvc_file_path):
    with open(dvc_file_path, 'r') as file:
        dvc_data = yaml.safe_load(file)
        outs = dvc_data.get("outs", [])
        dataset_paths = [item['path'] for item in outs]
        return dataset_paths
    
# 初始化 Kubernetes 客戶端
def init_k8s_client():
    # 若在 K8s Cluster 內執行，則使用 In-Cluster Config
    config.load_incluster_config()
    return client.CoreV1Api()

# 實例化 LoggerManager
logger_manager = LoggerManager()
# 實例化 DVCManger
dvc_manager = DVCManager(logger_manager)
# 實例化 DagManger
dag_manager = DagManager(logger_manager)

@app.get("/health", status_code=200)
async def health_check():
    return JSONResponse(content={"status": "NCURSS_INFERENCE_MLServingPod is healthy"})

# [Inference/RegisterDag]
@app.post("/Inference/RegisterDag")
async def register_dag_and_logger_and_dvc_worker(request: DagRequest):
    """
    每隻 DAG 先來註冊，並生成專屬的 logger 與 DVC Worker
    """
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    # # 檢查 PVC 掛載狀態
    # if not is_pvc_mounted():
    #     raise HTTPException(status_code=500, detail="PVC is not mounted.")

    # 組合路徑：/mnt/storage/{dag_id}_{execution_id}
    dag_root_folder_path = os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")
    git_local_repo_for_dvc = os.path.join(dag_root_folder_path, "GIT_LOCAL_REPO_FOR_DVC")

    # 創建根目錄
    create_folder_if_not_exists(dag_root_folder_path)

    # 確認 DAG 是否已在 dag_manager 中註冊
    if dag_manager.is_registered(dag_id, execution_id):
        return {"status": "success", "message": "DAG is already registered."}

    # 登記 DAG 到 dag_manager
    dag_manager.register_dag(dag_id, execution_id, dag_root_folder_path)

    # 初始化並註冊 logger
    if not logger_manager.logger_exists(dag_id, execution_id):
        logger_manager.init_logger(dag_id, execution_id, dag_root_folder_path)

    # 初始化並註冊 DVCWorker
    create_folder_if_not_exists(git_local_repo_for_dvc)
    dvc_manager.init_worker(dag_id=dag_id, execution_id=execution_id, git_repo_path=git_local_repo_for_dvc)

    # 獲取 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Inference/RegisterDag")

    # **打印請求的 request body**
    request_dict = request.model_dump()
    logger.info(f"Received request body:\n{json.dumps(request_dict, indent=4)}")

    logger.info(f"DAG, Logger, and DVCWorker initialized for DAG_ID: {dag_id}, EXECUTION_ID: {execution_id}")

    return {"status": "success", "message": f"DAG, Logger, and DVCWorker initialized for DAG_ID: {dag_id}, EXECUTION_ID: {execution_id}"}

# [Inference/SetupFolder]
@app.post("/Inference/SetupFolder")
async def setup_folders_for_training(request: DagRequest):
    """
    為 Training 設定資料夾結構並 clone 相關程式碼
    """
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    task_stage_type = request.TASK_STAGE_TYPE

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    # 獲取 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Inference/SetupFolder")
    
    dvc_worker = dvc_manager.get_worker(dag_id, execution_id)
    
    
    # # 檢查 PVC 掛載狀態
    # if not is_pvc_mounted():
    #     raise HTTPException(status_code=500, detail="PVC is not mounted.")
    # logger.info("PVC IS MOUNTED!!!!")

    # 組合路徑：/mnt/storage/{dag_id}_{execution_id}
    dag_root_folder_path = os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")
    repo_inference_path = os.path.join(dag_root_folder_path, "NCU-RSS-1.5")

    # 確認 DAG 根目錄是否存在
    if not os.path.exists(dag_root_folder_path):
        raise HTTPException(status_code=404, detail="dag_root_folder was not created")


    # 從 CODE_REPO_URL 中獲取對應的 Repo URL
    code_repo_url = request.CODE_REPO_URL.get(task_stage_type)
    if not code_repo_url:
        raise HTTPException(status_code=400, detail="CODE_REPO_URL for the given TASK_STAGE_TYPE not found")

    try:
        logger.info(f"Received request for DAG_ID: {dag_id}, EXECUTION_ID: {execution_id}")

        # 1. 檢查GIT LOCAL REPO FOR DVC
        dvc_worker.ensure_git_repository()
        logger.info("Ensured Git repository for DVC")

        # 2. 在 dag_root_folder 内 git clone
        if not os.path.exists(repo_inference_path):
            # # 取得環境變數中的 GITHUB_TOKEN
            # github_token = os.getenv("GITHUB_TOKEN")
            # if not github_token:
            #     raise Exception("GITHUB_TOKEN not found in environment variables.")

            # # 使用 subprocess.run() 執行 git clone 指令
            # # **修正：直接內嵌 GITHUB_TOKEN 到 URL 中**
            # repo_url = f"https://{github_token}:x-oauth-basic@github.com/chen88088/NCU-RSS-1.5.git"
            repo_url = "https://github.com/chen88088/NCU-RSS-1.5.git"

            clone_command = ["git", "clone", repo_url, repo_inference_path]
            result = subprocess.run(clone_command, capture_output=True, text=True)
            
            # 紀錄輸出與錯誤訊息
            logger.info(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")

            # 檢查返回碼
            if result.returncode != 0:
                raise Exception("git clone failed")
            
            # 驗證是否成功 clone
            assert os.path.exists(repo_inference_path), "Repo NCURSS-Training clone failed"
            logger.info(f"Cloned NCURSS-Training repo to {repo_inference_path}")
        else:
            logger.info(f"NCURSS-Training repo already exists at {repo_inference_path}")

        return {"status": "success",  "message": f"Cloned NCURSS-Training repo to {repo_inference_path}"}

    except HTTPException as e:
        logger.error(f"HTTPException occurred: {str(e.detail)}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# [Inference/DownloadPreprocessingResult]
@app.post("/Inference/DownloadPreprocessingResult")
async def download_preprocessing_result(request: DagRequest):
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    task_stage_type = request.TASK_STAGE_TYPE  # e.g., "inference"

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")

    dag_root_folder_path = f"{dag_id}_{execution_id}"

    # 獲取對應的 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Inference/DownloadPreprocessingResult")

    dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

    # # 檢查 PVC 掛載狀態
    # if not is_pvc_mounted():
    #     raise HTTPException(status_code=500, detail="PVC is not mounted.")

    try:
        # 設定本地 DAG 路徑
        # 組合路徑：/mnt/storage/{dag_id}_{execution_id}
        dag_root_folder_path = os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")


        # 步驟 1. 下載Preprocessing 階段的資料於 "Temp_Preprocessing_Result_Download"
        # 設定 Preprocessing 結果存放的 Repository 位置
        temp_preprocessing_result_download_path = os.path.join(dag_root_folder_path, "Temp_Preprocessing_Result_Download")

        # **確保目錄存在**
        os.makedirs(temp_preprocessing_result_download_path, exist_ok=True)

        # DVC 檔案名稱
        dvc_filename = "result.dvc"
        result_folder = "result"
        
        logger.info(f"Downloading {dvc_filename} from MinIO to {temp_preprocessing_result_download_path}")

        # 執行 pull 操作
        pull_result = dvc_worker.pull(
            stage_type="Preprocessing",  # 指定 stage
            dvc_filename=dvc_filename,  # 下載的 .dvc 文件
            folder_path=str(temp_preprocessing_result_download_path)  # 本地儲存路徑
        )

        if pull_result["status"] == "error":
            logger.error(f"Failed to pull {result_folder} from DVC: {pull_result['message']}")
            raise HTTPException(status_code=500, detail=pull_result["message"])

        # 確認數據是否成功下載
        local_result_path = Path(temp_preprocessing_result_download_path) / result_folder
        if not local_result_path.exists():
            logger.error(f"Result folder {local_result_path} does not exist after pulling from DVC.")
            raise HTTPException(status_code=500, detail="Downloaded result folder not found.")

        logger.info(f"Successfully downloaded preprocessing results for {dag_id}_{execution_id}.")

        # 步驟 2. 設定最終放置 Preprocessing 結果的目標目錄
        # source
        source_temp_download_files_path = Path(temp_preprocessing_result_download_path) / "result"
        
        # target
        repo_inference_path = os.path.join(dag_root_folder_path, "NCU-RSS-1.5")
        target_result_path = Path(repo_inference_path) / "data/inference/For_training_testing/320x320/parcel_NIRRGA"

        # 確保資料夾存在
        target_result_path.mkdir(parents=True, exist_ok=True)
        source_temp_download_files_path.mkdir(parents=True, exist_ok=True)


        # 步驟 3: 直接重命名 temp_download 為最終目標資料夾
        logger.info(f"Renaming {source_temp_download_files_path} to {target_result_path}")

        # 確保目標資料夾不存在，否則 shutil.move 會報錯
        if target_result_path.exists():
            shutil.rmtree(target_result_path)  # 先刪除舊目錄，確保不衝突

        shutil.move(str(source_temp_download_files_path), str(target_result_path))

        logger.info(f"Successfully moved preprocessing results to {target_result_path}")


        return {
            "status": "success",
            "message": f"Preprocessing results '{result_folder}' successfully downloaded.",
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return {"status": "error", "message": str(e)}, 500

# [Inference/FetchModel]
@app.post("/Inference/FetchModel")
async def fetch_model(request: DagRequest):
    # **設定 MLflow Tracking Server**
    MLFLOW_TRACKING_URI = "http://10.52.52.142:5000"
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    # **設定 MinIO 作為 S3 儲存**
    os.environ["AWS_ACCESS_KEY_ID"] = "minio"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://10.52.52.142:9000"
    os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"

    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    model_name = request.MODEL_NAME
    model_version = int(request.MODEL_VERSION)

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    if not model_name or not model_version:
        raise HTTPException(status_code=400, detail="MODEL_NAME and MODEL_VERSION are required.")
    
    # 獲取對應的 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Inference/DownloadPreprocessingResult")

    # 組合路徑：/mnt/storage/{dag_id}_{execution_id}
    dag_root_folder_path = os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")
    repo_inference_path = os.path.join(dag_root_folder_path, "NCU-RSS-1.5")

    # 設定下載模型的目標路徑
    model_download_path = Path(os.path.join(repo_inference_path, "data/inference/saved_model_and_prediction"))
    model_download_path.mkdir(parents=True, exist_ok=True)

    try:
        # **步驟 1：從 MLflow Model Registry 取得模型 URI**
        client = MlflowClient()
        model_version_info = client.get_model_version(model_name, model_version)
        artifact_uri = model_version_info.source  # 取得模型在 MinIO/S3 的存放路徑
        logger.info(f"Model: {model_name} version{model_version} URI: {artifact_uri}")

        # **步驟 2：下載模型**
        download_path = mlflow.artifacts.download_artifacts(artifact_uri, dst_path=str(model_download_path))
        logger.info(f"Model downloaded to: {download_path}")

        return {
            "status": "success",
            "message": f"Model '{model_name}' version '{model_version}' successfully downloaded.",
            "path": str(download_path)
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500


# TODO: CONFIG MODIFY
# [Inference/ModifyPreprocessingConfig]
@app.post("/INFERENCE/preprocessing/ModifyPreprocessingConfig")
async def modify_preprocessing_config(request: Request):
    data = await request.json()
    dag_id = data.get("DAG_ID")
    execution_id = data.get("EXECUTION_ID")
    inference_root_folder_name = data.get("inference_root_folder")

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")

    # 获取或初始化 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)

    try:
        moa_inference_folder = Path.home() / "Desktop" / inference_root_folder_name
        root_folder_path = Path(moa_inference_folder)
        repo_preprocessing_path = Path(root_folder_path / "NCU-RSS-Predict-Preprocessing")
        config_path = Path(root_folder_path / "NCU-RSS-Predict-Preprocessing" / "configs" / "config.py")

        logger.info(f"Modifying config at {config_path} with data: {data}")

        # 读取并修改配置文件
        with open(config_path, "r", encoding='utf-8') as file:
            config_content = file.read()
        config_content = config_content.replace(r'D:\SHP', data["SHP_Path"])
        config_content = config_content.replace(r'D:\TIF', data["TIF_Path"])
        config_content = config_content.replace(r'C:\Users\chen88088\Desktop', str(repo_preprocessing_path))
        
        with open(config_path, "w", encoding='utf-8') as file:
            file.write(config_content)
        logger.info(f"Config file {config_path} modified successfully")

        # 验证文件内容
        with open(config_path, "r", encoding='utf-8') as file:
            verified_content = file.read()
        assert data["SHP_Path"] in verified_content, "SHP_Path not updated"
        assert data["TIF_Path"] in verified_content, "TIF_Path not updated"
        assert str(repo_preprocessing_path) in verified_content, "directory not updated"
        logger.info(f"Config file {config_path} verification successful")
        
        logger.info(f"[preprocessing]ModifyPreprocessingConfig finished for dag: {dag_id}_{execution_id}   !!!")

    except Exception as e:
        logger.error(f"An error occurred during config modification: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Config modification failed: {str(e)}")

    return {"status": "success", "message": "Config modified successfully"}


# # [Training/ModifyConfig]
# @app.post("/Training/ModifyConfig")
# async def modify_config(request: DagRequest):
#     # pass
#     return {"status": "success", "message": f"Config Mofification Successfully!!"}

# [Inference/ExecuteInferenceScripts]
@app.post("/Inference/ExecuteInferenceScripts")
async def execute_inference_scripts(request: DagRequest):
    """
    根據 TASK_STAGE_TYPE 起 Pod 並執行對應腳本
    """
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    task_stage_type = request.TASK_STAGE_TYPE

    # 從 IMAGE_NAME 中抓取對應的 Image
    image_name = request.IMAGE_NAME.get(task_stage_type)
    if not image_name:
        raise HTTPException(status_code=400, detail=f"No image found for TASK_STAGE_TYPE: {task_stage_type}")
    
    # 獲取 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Training/ExecuteTrainingScripts")
    

    # v1 = init_k8s_client()
    batch_v1 = client.BatchV1Api()  # 初始化 Batch API 用於創建 Job
    
    # # Pod 名稱
    # pod_name = f"{dag_id}-{execution_id}-{task_stage_type}-task-pod-{uuid.uuid4().hex[:6]}"

    # Job 名稱
    raw_job_name = f"task-job-{dag_id}-{execution_id}-{task_stage_type}-{uuid.uuid4().hex[:6]}"
    # 將 _ 換成 -，轉小寫，並移除不合法字元
    job_name = re.sub(r'[^a-z0-9\-]+', '', raw_job_name.lower().replace('_', '-'))

    
    # 確保 PVC_NAME 環境變量已設定
    pvc_name = os.getenv("PVC_NAME")
    if not pvc_name:
        raise HTTPException(status_code=500, detail="PVC_NAME not set in environment variables")
    
    # 組合工作目錄
    working_dir = f"/mnt/storage/{dag_id}_{execution_id}/NCU-RSS-1.5"
    output_dir = f"{working_dir}/data/train_test/For_training_testing/320x320/train_test"
    
    # 定義要執行的腳本
    scripts_to_run = [
        "python3 inference.py"
    ]

    # 組合成 Command
    command = " && ".join([f"cd {working_dir}"] + scripts_to_run )
    
    # **正確的 Job Manifest**
    job_manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": "ml-serving",
            "labels": {
                "app": "task-job",
                "type": "gpu"
            }
        },
        "spec": {
            "backoffLimit": 3,
            # **TTLAfterFinished: 完成後 600 秒自動刪除**
            "ttlSecondsAfterFinished": 600,
            "template": {
                "metadata": {
                    "name": job_name
                },
                "spec": {
                    "nodeSelector": {
                        "gpu-node": "true"
                    },
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "task-container",
                            "image": f"harbor.pdc.tw/{image_name}",
                            "command": ["/bin/bash", "-c", command],
                            "volumeMounts": [
                                {
                                    "name": "shared-storage",
                                    "mountPath": "/mnt/storage"
                                }
                            ],
                            "resources": {
                                "limits": {
                                    "nvidia.com/gpu": "1"
                                }
                            }
                        }
                    ],
                    "volumes": [
                        {
                            "name": "shared-storage",
                            "persistentVolumeClaim": {
                                "claimName": pvc_name
                            }
                        }
                    ]
                }
            }
        }
    }
    
    # 建立 Job
    try:
        logger.info("Job creating...........")
        batch_v1.create_namespaced_job(namespace="ml-serving", body=job_manifest)

        logger.info("Job execute start...........")
        # **等待 Job 完成**
        wait_for_job_completion(batch_v1, job_name, "ml-serving",logger)

        logger.info("Job finish!!!!!")

        # record_mlflow(request, output_dir)
        # logger.info("Record to MLflow Successfully!!!!")
        # logger.info(f"Task Job created and finished . job_name {job_name}")
        return {"status": "success", "message": "Task Job created and finished", "job_name": job_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create Task Job: {str(e)}")

def wait_for_job_completion(batch_v1, job_name, namespace, logger, timeout=3600):
    """
    監聽 K8s Job 狀態，等待 Job 執行完成
    """
    start_time = time.time()
    v1 = client.CoreV1Api()  # 初始化 K8s API
    pod_name = None  # 儲存 task pod 名稱
    while time.time() - start_time < timeout:
        job_status = batch_v1.read_namespaced_job_status(job_name, namespace)
        if job_status.status.succeeded == 1:
            logger.info(f"Job {job_name} completed successfully.")
            
            # **讀取 Pod 名稱**
            pod_list = v1.list_namespaced_pod(namespace, label_selector=f"job-name={job_name}").items
            
            if pod_list:
                pod_name = pod_list[0].metadata.name
                logger.info(f"Job pod name: {pod_name}")

                # **獲取並記錄 Pod 的 logs**
                try:
                    logs = v1.read_namespaced_pod_log(name=pod_name, namespace=namespace)
                    logger.info(f"Task Pod Logs for {job_name}:\n{logs}")
                except Exception as e:
                    logger.error(f"Failed to get logs for {pod_name}: {str(e)}")

            return
        
        elif job_status.status.failed is not None and job_status.status.failed > 0:
            raise Exception(f"Job {job_name} failed.")
        time.sleep(10)  # 每 10 秒檢查一次 Job 狀態


def record_mlflow(request: DagRequest, output_dir: str):
    """ 記錄執行結果到 MLflow """
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "default-key")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY", "default-secret")
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment(request.DAG_ID)
    dag_instance_unique_id = f'{request.DAG_ID}_{request.EXECUTION_ID}'

    with mlflow.start_run(run_name=dag_instance_unique_id):
        # 記錄 Request Body 為 Tags
        mlflow.set_tags(request.model_dump())
        
        # 讀取 Excel 內容
        excel_path = os.path.join(output_dir, "training_results.xlsx")
        df_params = pd.read_excel(excel_path, sheet_name="Parameters")
        df_metrics = pd.read_excel(excel_path, sheet_name="Metrics")
        
        # 記錄 Parameters
        params = df_params.iloc[0].to_dict()
        for key, value in params.items():
            mlflow.log_param(key, value)
        
        # 記錄 Metrics
        for index, row in df_metrics.iterrows():
            mlflow.log_metrics({
                "train_accuracy": row["train_accuracy"],
                "val_accuracy": row["val_accuracy"],
                "train_loss": row["train_loss"],
                "val_loss": row["val_loss"],
                "train_kappa": row["train_kappa"],
                "val_kappa": row["val_kappa"]
            }, step=index)
        
        # 上傳 Artifact
        mlflow.log_artifact(os.path.join(output_dir, "final_weight.h5"))
        mlflow.log_artifact(os.path.join(output_dir, "model.h5"))
        mlflow.log_artifact(os.path.join(output_dir, "model_val_acc.h5"))
        mlflow.log_artifact(os.path.join(output_dir, "val_acc.png"))
        mlflow.log_artifact(excel_path)

# [Inference/UploadInferenceOutputFiles]
@app.post("/Inference/UploadInferenceOutputFiles")
def upload_inference_output_files(request: DagRequest):
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    task_stage_type = request.TASK_STAGE_TYPE

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    
    # 獲取對應的 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Inference/UploadInferenceOutputFiles")

    dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

    logger.info("Received request to upload files")

    
    try:
        # 組合路徑：/mnt/storage/{dag_id}_{execution_id}
        dag_root_folder_path = os.path.join(STORAGE_PATH, f"{dag_id}_{execution_id}")
        root_folder_path = Path(dag_root_folder_path)
        repo_inference_path = os.path.join(dag_root_folder_path, "NCU-RSS-1.5")

        upload_source_folder = Path(os.path.join(repo_inference_path,"data/inference/saved_model_and_prediction"))

        # 注意：按照原腳本產生名為 "model" 的文件夾 命名為result_folder
        result_folder = "model"
        
        # 初始化本地dvc倉庫，並配置 MinIO as remote-storage
        # dvc_repo should be the to-be-uploaded-folder's parent folder
        dvc_repo = upload_source_folder
        logger.info(f"Initializing DVC repository at {dvc_repo}")
        
        init_result = dvc_worker.initialize_dvc(dvc_repo, task_stage_type)
        if init_result["status"] == "error":
            logger.error(f"Failed to initialize DVC: {init_result['message']}")
            raise HTTPException(status_code=500, detail=init_result["message"])

        # 使用 DVC 管理文件夾並且推送到 MinIO
        logger.info(f"Adding and pushing folder {result_folder} to DVC")
        add_and_push_mask_result = dvc_worker.add_and_push_data(
            folder_path=f"{dvc_repo}/{result_folder}",
            folder_name=result_folder,
            stage_type= task_stage_type
        )
        if add_and_push_mask_result["status"] == "error":
            logger.error(f"Failed to add and push {result_folder}: {add_and_push_mask_result['message']}")
            raise HTTPException(status_code=500, detail=add_and_push_mask_result["message"])

        
        # 提交所有更改並推送到 Git
        logger.info(f"Committing and pushing DVC changes for {result_folder} and {result_folder} to Git")
        git_commit_result = dvc_worker.git_add_commit_and_push(
            project_path=root_folder_path,
            message=f"Add and track folder {result_folder}  with DVC"
        )
        if git_commit_result["status"] == "error":
            logger.error(f"Failed to commit and push to Git: {git_commit_result['message']}")
            raise HTTPException(status_code=500, detail=git_commit_result["message"])

        logger.info(f"Successfully added and pushed {result_folder} to DVC and Git")

        # sucess 
        logger.info(f"[Inference]UploadInferenceOutputFiles finished for dag: {dag_id}_{execution_id}   !!!")

        return {"status": "success", "message": f"The folders '{result_folder}' have been added and pushed to DVC."}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return {"status": "error", "message": str(e)}, 500


@app.post("/Inference/UploadLogToS3")
async def upload_log_to_s3(request: DagRequest):
    
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    task_stage_type = request.TASK_STAGE_TYPE

    if not dag_id or not execution_id:
        raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
    # 組合路徑：/mnt/storage/{dag_id}_{execution_id}/LOGS
    storage_path = STORAGE_PATH
    logs_folder_path = Path(storage_path) / f"{dag_id}_{execution_id}" / "LOGS"

    # 獲取 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Inference/UploadLogToS3")
    dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

    try:
        

        # Log 檔案路徑
        log_file_path = logs_folder_path / f"{dag_id}_{execution_id}.txt"

        if not log_file_path.exists():
            raise HTTPException(status_code=404, detail=f"Log file {log_file_path} not found.")

        # 重新命名檔案
        renamed_log_filename = f"{dag_id}_{execution_id}_{task_stage_type}.txt"

        # 上傳到 MinIO
        target_path = f"{dag_id}_{execution_id}/logs/{renamed_log_filename}"

        logger.info(f"Uploading log file: {log_file_path} to MinIO at {target_path}")

        # 使用 DVCWorker 的 S3 客戶端上傳
        dvc_worker.s3_client.upload_file(
            Filename=str(log_file_path),
            Bucket=dvc_worker.minio_bucket,
            Key=target_path
        )

        logger.info("Log file uploaded successfully.")

        return {
            "status": "success",
            "message": f"Log file uploaded to MinIO at {target_path}"
        }

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}") 

#########################################################################

# [Training/ExecuteTrainingScripts]
@app.post("/Training/ExecuteTrainingScripts")
async def execute_training_scripts(request: DagRequest):
    """
    根據 TASK_STAGE_TYPE 起 Pod 並執行對應腳本
    """
    dag_id = request.DAG_ID
    execution_id = request.EXECUTION_ID
    task_stage_type = request.TASK_STAGE_TYPE

    # 從 IMAGE_NAME 中抓取對應的 Image
    image_name = request.IMAGE_NAME.get(task_stage_type)
    if not image_name:
        raise HTTPException(status_code=400, detail=f"No image found for TASK_STAGE_TYPE: {task_stage_type}")
    
    # 獲取 Logger 和 DVCWorker
    logger = logger_manager.get_logger(dag_id, execution_id)
    if logger:
        logger_manager.log_section_header(logger, "Training/ExecuteTrainingScripts")
    

    # v1 = init_k8s_client()
    batch_v1 = client.BatchV1Api()  # 初始化 Batch API 用於創建 Job
    
    # # Pod 名稱
    # pod_name = f"{dag_id}-{execution_id}-{task_stage_type}-task-pod-{uuid.uuid4().hex[:6]}"

    # Job 名稱
    raw_job_name = f"task-job-{dag_id}-{execution_id}-{task_stage_type}-{uuid.uuid4().hex[:6]}"
    # 將 _ 換成 -，轉小寫，並移除不合法字元
    job_name = re.sub(r'[^a-z0-9\-]+', '', raw_job_name.lower().replace('_', '-'))

    
    # 確保 PVC_NAME 環境變量已設定
    pvc_name = os.getenv("PVC_NAME")
    if not pvc_name:
        raise HTTPException(status_code=500, detail="PVC_NAME not set in environment variables")
    
    # 組合工作目錄
    working_dir = f"/mnt/storage/{dag_id}_{execution_id}/NCU-RSS-1.5"
    output_dir = f"{working_dir}/data/train_test/For_training_testing/320x320/train_test"
    
    # 定義要執行的腳本
    scripts_to_run = [
        "python3 kmeans_cluster_for_train_test.py",
        "python3 random_sampling_for_parcel_dataset_for_train_test.py",
        "python3 train_and_val_model_with_excel.py"
    ]

    # 組合成 Command
    command = " && ".join([f"cd {working_dir}"] + scripts_to_run )
    
    # **正確的 Job Manifest**
    job_manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": "ml-serving",
            "labels": {
                "app": "task-job",
                "type": "gpu"
            }
        },
        "spec": {
            "backoffLimit": 3,
            # **TTLAfterFinished: 完成後 60 秒自動刪除**
            "ttlSecondsAfterFinished": 60,
            "template": {
                "metadata": {
                    "name": job_name
                },
                "spec": {
                    "nodeSelector": {
                        "gpu-node": "true"
                    },
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "task-container",
                            "image": f"harbor.pdc.tw/{image_name}",
                            "command": ["/bin/bash", "-c", command],
                            "volumeMounts": [
                                {
                                    "name": "shared-storage",
                                    "mountPath": "/mnt/storage"
                                }
                            ],
                            "resources": {
                                "limits": {
                                    "nvidia.com/gpu": "1"
                                }
                            }
                        }
                    ],
                    "volumes": [
                        {
                            "name": "shared-storage",
                            "persistentVolumeClaim": {
                                "claimName": pvc_name
                            }
                        }
                    ]
                }
            }
        }
    }
    
    # 建立 Job
    try:
        logger.info("Job creating...........")
        batch_v1.create_namespaced_job(namespace="ml-serving", body=job_manifest)

        logger.info("Job execute start...........")
        # **等待 Job 完成**
        wait_for_job_completion(batch_v1, job_name, "ml-serving",logger)

        logger.info("Job finish!!!!!")

        record_mlflow(request, output_dir)
        logger.info("Record to MLflow Successfully!!!!")
        logger.info(f"Task Job created and finished . job_name {job_name}")
        return {"status": "success", "message": "Task Job created and finished", "job_name": job_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create Task Job: {str(e)}")

def wait_for_job_completion(batch_v1, job_name, namespace, logger, timeout=3600):
    """
    監聽 K8s Job 狀態，等待 Job 執行完成
    """
    start_time = time.time()
    v1 = client.CoreV1Api()  # 初始化 K8s API
    pod_name = None  # 儲存 task pod 名稱
    while time.time() - start_time < timeout:
        job_status = batch_v1.read_namespaced_job_status(job_name, namespace)
        if job_status.status.succeeded == 1:
            logger.info(f"Job {job_name} completed successfully.")
            
            # **讀取 Pod 名稱**
            pod_list = v1.list_namespaced_pod(namespace, label_selector=f"job-name={job_name}").items
            
            if pod_list:
                pod_name = pod_list[0].metadata.name
                logger.info(f"Job pod name: {pod_name}")

                # **獲取並記錄 Pod 的 logs**
                try:
                    logs = v1.read_namespaced_pod_log(name=pod_name, namespace=namespace)
                    logger.info(f"Task Pod Logs for {job_name}:\n{logs}")
                except Exception as e:
                    logger.error(f"Failed to get logs for {pod_name}: {str(e)}")

            return
        
        elif job_status.status.failed is not None and job_status.status.failed > 0:
            raise Exception(f"Job {job_name} failed.")
        time.sleep(10)  # 每 10 秒檢查一次 Job 狀態


def record_mlflow(request: DagRequest, output_dir: str):
    """ 記錄執行結果到 MLflow """
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "default-key")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY", "default-secret")
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment(request.DAG_ID)
    dag_instance_unique_id = f'{request.DAG_ID}_{request.EXECUTION_ID}'

    with mlflow.start_run(run_name=dag_instance_unique_id):
        # 記錄 Request Body 為 Tags
        mlflow.set_tags(request.model_dump())
        
        # 讀取 Excel 內容
        excel_path = os.path.join(output_dir, "training_results.xlsx")
        df_params = pd.read_excel(excel_path, sheet_name="Parameters")
        df_metrics = pd.read_excel(excel_path, sheet_name="Metrics")
        
        # 記錄 Parameters
        params = df_params.iloc[0].to_dict()
        for key, value in params.items():
            mlflow.log_param(key, value)
        
        # 記錄 Metrics
        for index, row in df_metrics.iterrows():
            mlflow.log_metrics({
                "train_accuracy": row["train_accuracy"],
                "val_accuracy": row["val_accuracy"],
                "train_loss": row["train_loss"],
                "val_loss": row["val_loss"],
                "train_kappa": row["train_kappa"],
                "val_kappa": row["val_kappa"]
            }, step=index)
        
        # 上傳 Artifact
        mlflow.log_artifact(os.path.join(output_dir, "final_weight.h5"))
        mlflow.log_artifact(os.path.join(output_dir, "model.h5"))
        mlflow.log_artifact(os.path.join(output_dir, "model_val_acc.h5"))
        mlflow.log_artifact(os.path.join(output_dir, "val_acc.png"))
        mlflow.log_artifact(excel_path)




#################################################################




# @app.post("/INFERENCE/preprocessing/ExecutePredictPreprocessing")
# async def execute_predict_preprocessing(request: Request):
#     data = await request.json()
#     dag_id = data.get("DAG_ID")
#     execution_id = data.get("EXECUTION_ID")
#     inference_root_folder_name = data.get("inference_root_folder")

#     if not dag_id or not execution_id:
#         raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")

#     # 获取或初始化 Logger 和 DVCWorker
#     logger = logger_manager.get_logger(dag_id, execution_id)

#     moa_inference_folder = Path.home() / "Desktop" / inference_root_folder_name
#     root_folder_path = Path(moa_inference_folder)
#     repo_preprocessing_path = Path(root_folder_path / "NCU-RSS-Predict-Preprocessing")
#     script_path = Path(repo_preprocessing_path / "prepare_inference_data_15.py")
#     python_executable = r'C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe'

#     try:
#         logger.info(f"Executing script {script_path} with Python {python_executable}")

#         # 使用 subprocess.run 执行 Python 脚本
#         result = subprocess.run([python_executable, script_path], check=True, text=True, capture_output=True)
        
#         logger.info(f"Script executed successfully. stdout: {result.stdout}, stderr: {result.stderr}")

#         logger.info(f"[preprocessing]ExecutePredictPreprocessing finished for dag: {dag_id}_{execution_id}   !!!")
#         return {
#             "status": "success",
#             "stdout": result.stdout,
#             "stderr": result.stderr
#         } 
        
#     except subprocess.CalledProcessError as e:
#         logger.error(f"Error executing script: {e.stderr}")
#         return {
#             "status": "error",
#             "stdout": e.stdout,
#             "stderr": e.stderr
#         }, 500
#     except Exception as e:
#         logger.error(f"Unexpected error occurred: {str(e)}")
#         return {"message": str(e)}, 500

# @app.post("/INFERENCE/preprocessing/UploadMaskAndNirrgFolders")
# async def upload_mask_and_nirrg_folders(request: Request):
#     data = await request.json()  # 获取 POST 请求的 JSON 数据
#     dag_id = data.get("DAG_ID")
#     execution_id = data.get("EXECUTION_ID")

#     if not dag_id or not execution_id:
#         raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")

#     # 获取或初始化 Logger 和 DVCWorker
#     logger = logger_manager.get_logger(dag_id, execution_id)
#     dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

#     try:
#         inference_root_folder_name = data.get("inference_root_folder")
#         moa_inference_folder = Path.home() / "Desktop" / inference_root_folder_name
#         root_folder_path = Path(moa_inference_folder)
#         repo_preprocessing_path = root_folder_path / "NCU-RSS-Predict-Preprocessing"
        
#         # 初始化 DVC 本地仓库，并配置 MinIO as remote-storage
#         dvc_repo = repo_preprocessing_path / "data"
#         logger.info(f"Initializing DVC repository at {dvc_repo}")
        
#         init_result = dvc_worker.initialize_dvc(dvc_repo)
#         if init_result["status"] == "error":
#             logger.error(f"Failed to initialize DVC: {init_result['message']}")
#             raise HTTPException(status_code=500, detail=init_result["message"])

#         # 定义子文件夹的相对路径
#         src_mask_folder = 'RSS15_Training_rice_mask'
#         src_nirrg_folder = 'predict_NIRRG'

#         # 使用 DVC 管理文件夹并推送到 MinIO
#         logger.info(f"Adding and pushing folder {src_mask_folder} to DVC")
#         add_and_push_mask_result = dvc_worker.add_and_push_data(
#             folder_path=f"{dvc_repo}/{src_mask_folder}",
#             folder_name=src_mask_folder
#         )
#         if add_and_push_mask_result["status"] == "error":
#             logger.error(f"Failed to add and push {src_mask_folder}: {add_and_push_mask_result['message']}")
#             raise HTTPException(status_code=500, detail=add_and_push_mask_result["message"])

#         logger.info(f"Adding and pushing folder {src_nirrg_folder} to DVC")
#         add_and_push_nirrg_result = dvc_worker.add_and_push_data(
#             folder_path=f"{dvc_repo}/{src_nirrg_folder}",
#             folder_name=src_nirrg_folder
#         )
#         if add_and_push_nirrg_result["status"] == "error":
#             logger.error(f"Failed to add and push {src_nirrg_folder}: {add_and_push_nirrg_result['message']}")
#             raise HTTPException(status_code=500, detail=add_and_push_nirrg_result["message"])

#         # 将所有更改提交并推送到 Git
#         logger.info(f"Committing and pushing DVC changes for {src_mask_folder} and {src_nirrg_folder} to Git")
#         git_commit_result = dvc_worker.git_add_commit_and_push(
#             project_path=root_folder_path,
#             message=f"Add and track folder {src_mask_folder} and {src_nirrg_folder} with DVC"
#         )
#         if git_commit_result["status"] == "error":
#             logger.error(f"Failed to commit and push to Git: {git_commit_result['message']}")
#             raise HTTPException(status_code=500, detail=git_commit_result["message"])

#         logger.info(f"Successfully added and pushed {src_mask_folder} and {src_nirrg_folder} to DVC and Git")

#         logger.info(f"[preprocessing]UploadMaskAndNirrgFolders finished for dag: {dag_id}_{execution_id}   !!!")

#         return {"status": "success", "message": f"The folders '{src_mask_folder}' and '{src_nirrg_folder}' have been added and pushed to DVC."}

#     except HTTPException as e:
#         raise e
#     except Exception as e:
#         logger.error(f"Unexpected error occurred: {str(e)}")
#         return {"status": "error", "message": str(e)}, 500

# @app.post("/INFERENCE/inference/DownloadPreprocessingMaskAndNirrgFolders")
# async def download_preprocessing_mask_and_nirrg_folders(request: Request):
#     data = await request.json()  # 获取 POST 请求的 JSON 数据

#     dag_id = data.get("DAG_ID")
#     execution_id = data.get("EXECUTION_ID")
#     inference_model_id = data.get("model")

#     if not dag_id or not execution_id:
#         raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
#     # 获取 Logger 和 DVCWorker
#     logger = logger_manager.get_logger(dag_id, execution_id)
#     dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

#     # 定义 DVC 文件名
#     dvc_mask_filename = "RSS15_Training_rice_mask.dvc"
#     dvc_nirrg_filename = "predict_NIRRG.dvc"

#     # 临时目录，用于拉取 DVC 数据
#     inference_root_folder_name = data.get("inference_root_folder")
#     moa_inference_folder = Path.home() / "Desktop" / inference_root_folder_name
#     root_folder_path = Path(moa_inference_folder)
#     inference_folder_path = root_folder_path / f'inference_{inference_model_id}' 
#     temp_inference_download_folder = inference_folder_path / "temp_inference_download"
#     temp_inference_download_folder.mkdir(parents=True, exist_ok=True)

#     try:
#         # 拉取 RSS15_Training_rice_mask 数据
#         logger.info(f"Pulling {dvc_mask_filename} into {temp_inference_download_folder}")
#         result_mask = dvc_worker.pull(dvc_mask_filename, temp_inference_download_folder)
#         if result_mask["status"] == "error":
#             logger.error(f"Failed to pull {dvc_mask_filename}: {result_mask['message']}")
#             raise HTTPException(status_code=500, detail=f"Failed to pull mask data: {result_mask['message']}")

#         # 拉取 predict_NIRRG 数据
#         logger.info(f"Pulling {dvc_nirrg_filename} into {temp_inference_download_folder}")
#         result_nirrg = dvc_worker.pull(dvc_nirrg_filename, temp_inference_download_folder)
#         if result_nirrg["status"] == "error":
#             logger.error(f"Failed to pull {dvc_nirrg_filename}: {result_nirrg['message']}")
#             raise HTTPException(status_code=500, detail=f"Failed to pull NIRRG data: {result_nirrg['message']}")

#         # 目标文件夹路径
#         dest_mask_folder = inference_folder_path / 'NCU-RSS-1.5' / 'data' / 'inference' / 'parcel_mask'
#         dest_nirrg_folder = inference_folder_path / 'NCU-RSS-1.5' / 'data' / 'inference' / 'NRG_png'

#         # 确保目标文件夹的父目录存在
#         os.makedirs(dest_mask_folder.parent, exist_ok=True)
#         os.makedirs(dest_nirrg_folder.parent, exist_ok=True)

#         # 如果目标目录已经存在，先删除
#         if dest_mask_folder.exists():
#             logger.info(f"Deleting existing folder {dest_mask_folder}")
#             shutil.rmtree(dest_mask_folder)
#             if dest_mask_folder.exists():
#                 logger.error(f"Failed to delete {dest_mask_folder}")
#                 raise HTTPException(status_code=500, detail=f"Failed to delete {dest_mask_folder}")

#         if dest_nirrg_folder.exists():
#             logger.info(f"Deleting existing folder {dest_nirrg_folder}")
#             shutil.rmtree(dest_nirrg_folder)
#             if dest_nirrg_folder.exists():
#                 logger.error(f"Failed to delete {dest_nirrg_folder}")
#                 raise HTTPException(status_code=500, detail=f"Failed to delete {dest_nirrg_folder}")

#         # 重命名目录为目标目录
#         logger.info(f"Renaming {temp_inference_download_folder / 'RSS15_Training_rice_mask'} to {dest_mask_folder}")
#         (temp_inference_download_folder / 'RSS15_Training_rice_mask').rename(dest_mask_folder)
#         if not dest_mask_folder.exists():
#             logger.error(f"Failed to rename folder to {dest_mask_folder}")
#             raise HTTPException(status_code=500, detail=f"Failed to rename folder to {dest_mask_folder}")

#         logger.info(f"Renaming {temp_inference_download_folder / 'predict_NIRRG'} to {dest_nirrg_folder}")
#         (temp_inference_download_folder / 'predict_NIRRG').rename(dest_nirrg_folder)
#         if not dest_nirrg_folder.exists():
#             logger.error(f"Failed to rename folder to {dest_nirrg_folder}")
#             raise HTTPException(status_code=500, detail=f"Failed to rename folder to {dest_nirrg_folder}")

#         # 返回成功响应
#         logger.info(f"Successfully downloaded and renamed preprocessing folders for DAG {dag_id}, execution {execution_id}")
#         logger.info(f"[inference]DownloadPreprocessingMaskAndNirrgFolders finished for dag: {dag_id}_{execution_id}   !!!")
#         return {"status": "success"}
#     except Exception as e:
#         logger.error(f"Unexpected error occurred: {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.post("/INFERENCE/inference/FetchModel")
# async def fetch_model(request: Request):
#     data = await request.json()  # 获取 POST 请求的 JSON 数据

#     # 获取 DAG_ID 和 EXECUTION_ID
#     dag_id = data.get("DAG_ID")
#     execution_id = data.get("EXECUTION_ID")
#     download_url = data.get("model_download_url")
#     modelused_id = data.get("model")
#     inference_root_folder_name = data.get("inference_root_folder")

#     # 验证 DAG_ID 和 EXECUTION_ID 是否存在
#     if not dag_id or not execution_id:
#         raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")

#     # 获取 Logger 
#     logger = logger_manager.get_logger(dag_id, execution_id)
    
#     logger.info("Received request to download model")
#     logger.info(f"Download URL: {download_url}")

#     # 构建目标路径
#     moa_inference_folder = Path.home() / "Desktop" / inference_root_folder_name
#     root_folder_path = Path(moa_inference_folder)
#     save_directory = root_folder_path / f"inference_{modelused_id}" / "NCU-RSS-1.5" / "data" / "inference" / "saved_model_and_prediction"
#     logger.info(f"Save directory: {save_directory}")

#     # 确保目标目录存在
#     save_directory.mkdir(parents=True, exist_ok=True)
#     logger.info(f"Directory {save_directory} is ready")

#     try:
#         # 使用流式下载处理大文件
#         with requests.get(download_url, stream=True) as r:
#             r.raise_for_status()
#             model_file_path = save_directory / "model.h5"
#             with open(model_file_path, "wb") as f:
#                 for chunk in r.iter_content(chunk_size=8192):
#                     f.write(chunk)
#             logger.info(f"Model downloaded and saved to {model_file_path}")
#             logger.info(f"[inference]FetchModel finished for dag: {dag_id}_{execution_id}   !!!")

#     except requests.RequestException as e:
#         logger.error(f"Request failed: {e}")
#         raise HTTPException(status_code=400, detail=str(e))
#     except Exception as e:
#         logger.error(f"An error occurred: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

#     return {"status": "success", "file_path": str(model_file_path)}

# @app.post("/INFERENCE/inference/GenerateParcelDatasetForInference")
# async def generate_parcel_dataset_for_inference(request: Request):
#     data = await request.json()  # 获取 POST 请求的 JSON 数据

#     # 获取 DAG_ID 和 EXECUTION_ID
#     dag_id = data.get("DAG_ID")
#     execution_id = data.get("EXECUTION_ID")
#     modelused_id = data.get("model")
#     inference_root_folder_name = data.get("inference_root_folder")

#     # 验证 DAG_ID 和 EXECUTION_ID 是否存在
#     if not dag_id or not execution_id:
#         raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")

#     # 获取 Logger 和 DVCWorker
#     logger = logger_manager.get_logger(dag_id, execution_id)
#     dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

#     logger.info("Received request to run script")

#     # 构建目标路径
#     moa_inference_folder = Path.home() / "Desktop" / inference_root_folder_name
#     root_folder_path = Path(moa_inference_folder)
#     script_directory = root_folder_path / f"inference_{modelused_id}" / "NCU-RSS-1.5"
#     script_path = script_directory / "generate_parcel_dataset_for_inference.py"
#     log_directory = script_directory / "data" / "logs"

#     logger.info(f"Script path: {script_path}")
#     logger.info(f"Log directory: {log_directory}")

#     # 确保日志文件夹存在
#     if not log_directory.exists():
#         os.makedirs(log_directory)
#         logger.info(f"Created log directory: {log_directory}")

#     if not script_path.exists():
#         logger.error("Script file not found")
#         raise HTTPException(status_code=404, detail="Script file not found")

#     try:
#         # 使用 subprocess 运行脚本，并捕获输出
#         result = subprocess.run(
#             ["python", str(script_path)],
#             capture_output=True,
#             text=True,
#             check=True,
#             cwd=str(script_directory)  # 设置工作目录为脚本目录
#         )

#         # 将脚本输出记录到日志
#         logger.info(f"Script output:\n{result.stdout}")
#         logger.error(f"Script errors:\n{result.stderr}")
        
#         logger.info(f"[inference]GenerateParcelDatasetForInference finished for dag: {dag_id}_{execution_id}   !!!")
#         return {"status": "success", "output": result.stdout, "errors": result.stderr}
#     except subprocess.CalledProcessError as e:
#         logger.error(f"Script failed with error: {e.stderr}")
#         raise HTTPException(status_code=500, detail=f"Script failed with error: {e.stderr}")
#     except Exception as e:
#         logger.error(f"An error occurred: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# @app.post("/INFERENCE/inference/ExecuteInference")
# async def execute_inference(request: Request):
#     data = await request.json()
    
#     dag_id = data.get("DAG_ID")
#     execution_id = data.get("EXECUTION_ID")
    
#     if not dag_id or not execution_id:
#         raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
#     logger = logger_manager.get_logger(dag_id, execution_id)
    
#     logger.info("Received request to run inference script")

#     modelused_id = data.get("model")

#     # 构建目标路径
#     inference_root_folder_name = data.get("inference_root_folder")
#     moa_inference_folder = Path.home() / "Desktop" / inference_root_folder_name
#     root_folder_path = Path(moa_inference_folder)

#     # 构建目标路径
#     script_directory = root_folder_path / f"inference_{modelused_id}" / "NCU-RSS-1.5"
#     script_path = script_directory / "inference.py"
#     log_directory = script_directory / "data" / "logs"

#     logger.info(f"Script path: {script_path}")
#     logger.info(f"Log directory: {log_directory}")

#     # 确保日志文件夹存在
#     if not log_directory.exists():
#         log_directory.mkdir(parents=True, exist_ok=True)
#         logger.info(f"Created log directory: {log_directory}")

#     if not script_path.exists():
#         logger.error("Script file not found")
#         raise HTTPException(status_code=404, detail="Script file not found")

#     try:
#         # 使用 subprocess 运行脚本，并捕获输出
#         result = subprocess.run(
#             ["python", str(script_path)],
#             capture_output=True,
#             text=True,
#             check=True,
#             cwd=str(script_directory)  # 设置工作目录为脚本目录
#         )

#         # 将脚本输出记录到日志
#         logger.info(f"Script output:\n{result.stdout}")
#         logger.error(f"Script errors:\n{result.stderr}")
#         logger.info(f"[inference]ExecuteInference finished for dag: {dag_id}_{execution_id}   !!!")
#         return {"status": "success", "output": result.stdout, "errors": result.stderr}
#     except subprocess.CalledProcessError as e:
#         logger.error(f"Script failed with error: {e.stderr}")
#         raise HTTPException(status_code=500, detail=f"Script failed with error: {e.stderr}")
#     except Exception as e:
#         logger.error(f"An error occurred: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
# @app.post("/INFERENCE/inference/UploadInferenceOutputFiles")
# async def upload_inference_output_files(request: Request):
#     data = await request.json()
    
#     dag_id = data.get("DAG_ID")
#     execution_id = data.get("EXECUTION_ID")

#     if not dag_id or not execution_id:
#         raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
#     logger = logger_manager.get_logger(dag_id, execution_id)
#     dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

#     logger.info("Received request to upload files")

#     modelused_id = data.get("model")

#     # 构建目标路径
#     inference_root_folder_name = data.get("inference_root_folder")
#     moa_inference_folder = Path.home() / "Desktop" / inference_root_folder_name
#     root_folder_path = Path(moa_inference_folder)

#     # 构建源路径和 DVC 文件路径
#     source_directory = root_folder_path / f"inference_{modelused_id}" / "NCU-RSS-1.5" / "data" / "inference" / "saved_model_and_prediction" / "model"

#     # 注意：按照原脚本产生名为 "model" 的文件夹
#     folder_name = f"model"
    
#     logger.info(f"Source directory: {source_directory}")
#     logger.info(f"DVC folder name: {folder_name}")

#     if not source_directory.exists():
#         logger.error("Source directory not found")
#         raise HTTPException(status_code=404, detail="Source directory not found")

#     try:
#         dvc_repo = source_directory.parent
#         init_result = dvc_worker.initialize_dvc(dvc_repo)
#         if init_result["status"] == "error":
#             logger.error(f"Failed to initialize DVC: {init_result['message']}")
#             raise HTTPException(status_code=500, detail=init_result["message"])
        
#         # 使用 DVCWorker 将文件夹添加到 DVC 并推送到远程存储
#         result = dvc_worker.add_and_push_data(str(source_directory), folder_name)
#         if result["status"] == "error":
#             logger.error(f"Failed to upload data: {result['message']}")
#             raise HTTPException(status_code=500, detail=f"Failed to upload data: {result['message']}")

#         logger.info(f"Files uploaded successfully with DVC management for {folder_name}")

#         logger.info(f"[inference]UploadInferenceOutputFiles finished for dag: {dag_id}_{execution_id}   !!!")

#         return {"status": "success", "message": f"Files uploaded successfully with DVC management for {folder_name}"}
#     except Exception as e:
#         logger.error(f"An error occurred during upload: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# @app.post("/INFERENCE/postprocessing/DownloadInferenceOutputFiles")
# async def download_inference_output_files(request: Request):
#     data = await request.json()
    
#     dag_id = data.get("DAG_ID")
#     execution_id = data.get("EXECUTION_ID")

#     if not dag_id or not execution_id:
#         raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
#     logger = logger_manager.get_logger(dag_id, execution_id)
#     dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

#     logger.info("Received request to download files")

#     # 构建目标路径
#     inference_root_folder_name = data.get("inference_root_folder")
#     moa_inference_folder = Path.home() / "Desktop" / inference_root_folder_name
#     root_folder_path = Path(moa_inference_folder)

#     # 构建目标路径和 DVC 文件路径
#     destination_directory = root_folder_path / "IMG"
#     temp_postprocessing_download_directory = root_folder_path / "NCU-RSS-Predict-Postprocessing" / "temp_postprocessing_download"
#     temp_postprocessing_download_directory.mkdir(parents=True, exist_ok=True)

#     dvc_filename = "model.dvc"

#     logger.info(f"Temporary directory: {temp_postprocessing_download_directory}")
#     logger.info(f"DVC file name: {dvc_filename}")

#     if not destination_directory.exists():
#         logger.error("Destination directory not found")
#         raise HTTPException(status_code=404, detail="Destination directory not found")

#     try:
#         # 使用 DVCWorker 从远程存储拉取文件到临时目录
#         pull_result = dvc_worker.pull(dvc_filename, str(temp_postprocessing_download_directory))
        
#         if pull_result["status"] == "error":
#             logger.error(f"Failed to pull data: {pull_result['stderr']}")
#             raise HTTPException(status_code=500, detail=f"Failed to pull data: {pull_result['stderr']}")

#         # 如果目标目录已经存在，先删除
#         if destination_directory.exists():
#             shutil.rmtree(destination_directory)
#             if destination_directory.exists():
#                 logger.error(f"Failed to delete {destination_directory}")
#                 raise HTTPException(status_code=500, detail=f"Failed to delete {destination_directory}")
        
#         # 重命名临时目录为目标目录
#         temp_postprocessing_download_content_folder = temp_postprocessing_download_directory / "model"
#         if temp_postprocessing_download_content_folder.exists():
#             temp_postprocessing_download_content_folder.rename(destination_directory)
#             logger.info(f"Renamed {temp_postprocessing_download_content_folder} to {destination_directory}")
#             if not destination_directory.exists():
#                 logger.error(f"Failed to rename folder to {destination_directory}")
#                 raise HTTPException(status_code=500, detail=f"Failed to rename folder to {destination_directory}")

#         logger.info(f"Files downloaded and renamed to {destination_directory}")
#         logger.info(f"[postprocessing]DownloadInferenceOutputFiles finished for dag: {dag_id}_{execution_id}   !!!")
#         return {"status": "success", "message": f"Files downloaded and renamed to {destination_directory}"}
#     except Exception as e:
#         logger.error(f"An error occurred during download: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# @app.post("/INFERENCE/postprocessing/UpdatePostprocessingConfig")
# async def update_Postprocessing_config(request: Request):
#     data = await request.json()
    
#     dag_id = data.get("DAG_ID")
#     execution_id = data.get("EXECUTION_ID")

#     if not dag_id or not execution_id:
#         raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
#     logger = logger_manager.get_logger(dag_id, execution_id)

#     logger.info("Received request to update config file")
    
#     # 构建目标路径
#     inference_root_folder_name = data.get("inference_root_folder")
#     moa_inference_folder = Path.home() / "Desktop" / inference_root_folder_name
#     root_folder_path = Path(moa_inference_folder)
#     TIF_folder_path = data["TIF_Path"]
#     SHP_folder_path = data["SHP_Path"]

#     # 构建config.py文件路径
#     config_path = root_folder_path / "NCU-RSS-Predict-Postprocessing" / "configs" / "config.py"

#     logger.info(f"Config file path: {config_path}")

#     if not config_path.exists():
#         logger.error("Config file not found")
#         raise HTTPException(status_code=404, detail="Config file not found")

#     try:
#         # 读取config.py文件
#         with open(config_path, "r", encoding="utf-8") as file:
#             config_content = file.read()

#         # 更新config.py内容
#         config_content = config_content.replace(r'SHP_Path = r"\SHP"', f'SHP_Path = r"{SHP_folder_path}"')
#         config_content = config_content.replace(r'TIF_Path = r"\TIF"', f'TIF_Path = r"{TIF_folder_path}"')
#         img_path = root_folder_path / "IMG"
#         config_content = config_content.replace(r'IMG_Path = r"\IMG"', f'IMG_Path = r"{img_path}"')
#         directory_path = root_folder_path / "NCU-RSS-Predict-Postprocessing"
#         config_content = config_content.replace(r'directory = r"D:\testMOA\NCU-RSS-Predict-Postprocessing"', f'directory = r"{directory_path }"')

#         # 写回config.py文件
#         with open(config_path, "w", encoding="utf-8") as file:
#             file.write(config_content)

#         logger.info("Config file updated successfully")
#         logger.info(f"[postprocessing]UpdatePostprocessingConfig finished for dag: {dag_id}_{execution_id}   !!!")
#         return {"status": "success"}
#     except Exception as e:
#         logger.error(f"An error occurred: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# @app.post("/INFERENCE/postprocessing/ExecutePostprocessing")
# async def execute_postprocessing(request: Request):
#     data = await request.json()
    
#     dag_id = data.get("DAG_ID")
#     execution_id = data.get("EXECUTION_ID")

#     if not dag_id or not execution_id:
#         raise HTTPException(status_code=400, detail="DAG_ID and EXECUTION_ID are required.")
    
#     logger = logger_manager.get_logger(dag_id, execution_id)
#     dvc_worker = dvc_manager.get_worker(dag_id, execution_id)

#     logger.info("Received request to execute postprocessing script")

#     # 构建推理根文件夹路径
#     inference_root_folder_name = data.get("inference_root_folder")
#     moa_inference_folder = Path.home() / "Desktop" / inference_root_folder_name
#     root_folder_path = Path(moa_inference_folder)

#     # 构建 main.py 文件路径
#     script_directory = root_folder_path / "NCU-RSS-Predict-Postprocessing"
#     script_path = script_directory / "main.py"
#     python_executable = r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"

#     logger.info(f"Script path: {script_path}")

#     if not script_path.exists():
#         logger.error("Script file not found")
#         raise HTTPException(status_code=404, detail="Script file not found")

#     try:
#         # 使用 subprocess 运行脚本，并捕获输出
#         result = subprocess.run(
#             [python_executable, str(script_path)],
#             capture_output=True,
#             text=True,
#             check=True,
#             cwd=str(script_directory)  # 设置工作目录为脚本目录
#         )

#         # 将脚本输出记录到日志
#         logger.info(f"Script output:\n{result.stdout}")
#         if result.stderr:
#             logger.error(f"Script errors:\n{result.stderr}")

#         logger.info(f"[postprocessing]ExecutePostprocessing finished for dag: {dag_id}_{execution_id}   !!!")

#         return {"status": "success", "output": result.stdout, "errors": result.stderr}
#     except subprocess.CalledProcessError as e:
#         logger.error(f"Script failed with error: {e.stderr}")
#         raise HTTPException(status_code=500, detail=f"Script failed with error: {e.stderr}")
#     except Exception as e:
#         logger.error(f"An error occurred: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
# '''
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("MachineServer:app", host=MACHINE_IP, port=MACHINE_PORT, reload=True)



