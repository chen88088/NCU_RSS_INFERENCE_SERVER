from LoggerManager import LoggerManager
from datetime import datetime
class DagManager:
    def __init__(self, logger_manager: LoggerManager):
        self.registered_dags = {}
        self.logger_manager = logger_manager

    def is_registered(self, dag_id: str, execution_id: str) -> bool:
        return (dag_id, execution_id) in self.registered_dags

    def register_dag(self, dag_id: str, execution_id: str, log_folder_root_path: str = "default_log_folder") -> None:
        """註冊一个新的 DAG，同時留下日誌"""
        if not self.is_registered(dag_id, execution_id):
            self.registered_dags[(dag_id, execution_id)] = {
                "registration_time": datetime.now(),
                "status": "registered"
            }

            # 確保有啟動logger啟動logger
            if not self.logger_manager.logger_exists(dag_id, execution_id):
                self.logger_manager.init_logger(dag_id, execution_id, log_folder_root_path)

            # 獲取並使用logger取並使用logger
            logger = self.logger_manager.get_logger(dag_id, execution_id)
            if logger is not None:
                logger.info(f"DAG registered with ID: {dag_id} and Execution ID: {execution_id}")
            else:
                raise ValueError(f"Logger for DAG_ID {dag_id} and EXECUTION_ID {execution_id} not initialized properly.")

        else:
            logger = self.logger_manager.get_logger(dag_id, execution_id)
            if logger is not None:
                logger.info(f"DAG already registered with ID: {dag_id} and Execution ID: {execution_id}")
            else:
                raise ValueError(f"Logger for DAG_ID {dag_id} and EXECUTION_ID {execution_id} not found.")

