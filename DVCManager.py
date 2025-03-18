from DVCWorker import DVCWorker
from LoggerManager import LoggerManager
from pathlib import Path
import config
import boto3
class DVCManager:
    def __init__(self, logger_manager: LoggerManager):
        self.workers = {}
        self.logger_manager = logger_manager
        
    def init_worker(self, dag_id: str, execution_id: str , git_repo_path: str):
        worker_key = f"{dag_id}_{execution_id}"
        if worker_key not in self.workers:
            logger = self.logger_manager.get_logger(dag_id, execution_id)
            worker = DVCWorker(
                dag_id=dag_id,
                execution_id=execution_id,
                minio_bucket=config.MINIO_BUCKET,
                minio_url=config.MINIO_URL,
                access_key=config.MINIO_ACCESS_KEY,
                secret_key=config.MINIO_SECRET_KEY,
                git_repo_path= Path(git_repo_path).resolve(),  # or any other path from config
                logger=logger,
                dataset_storage_minio_url = config.DATASET_STORAGE_MINIO_URL,
                dataset_storage_minio_bucket= config.DATASET_STORAGE_MINIO_BUCKET,
                dataset_storage_minio_access_key= config.DATASET_STORAGE_MINIO_ACCESS_KEY,
                dataset_storage_minio_secret_key= config.DATASET_STORAGE_MINIO_SECRET_KEY

            )
            self.workers[worker_key] = worker
            logger.info(f"DVCWorker initialized for dag: {worker_key}")
            return worker
        else:
            logger.warning(f"DVCWorker already exists for dag: {worker_key}")
            return self.workers[worker_key]

    def get_worker(self, dag_id: str, execution_id: str):
        worker_key = f"{dag_id}_{execution_id}"
        return self.workers.get(worker_key)

    def worker_exists(self, dag_id: str, execution_id: str):
        worker_key = f"{dag_id}_{execution_id}"
        return worker_key in self.workers