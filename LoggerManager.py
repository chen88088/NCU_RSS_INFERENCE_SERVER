import logging
from pathlib import Path
from datetime import datetime

class LoggerManager:
    def __init__(self):
        # 字典用於管理紀錄的logger
        self.loggers = {}

    def init_logger(self, dag_id: str, execution_id: str, log_folder_root_path: str):
        """初始化logger並存储到 loggers 字典中"""
        # 創建專屬的 DAG 目錄
        dag_logs_folder = Path.home() / "Desktop" / log_folder_root_path/ "LOGS" 
        dag_logs_folder.mkdir(parents=True, exist_ok=True)

        # 生成唯一的日誌文件路徑（加入時間戳記防止衝突）
        log_filename = f"{dag_id}_{execution_id}.txt"
        log_file_path = dag_logs_folder / log_filename

        # 配置日誌紀錄器
        logger = logging.getLogger(f"{dag_id}_{execution_id}")
        logger.setLevel(logging.INFO)

        # 創建文件處理器
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.INFO)

        # 創建日誌格式
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(formatter)

        # 清除之前的處理器（如果有）
        if logger.hasHandlers():
            logger.handlers.clear()

        # 添加新的文件處理器
        logger.addHandler(file_handler)

        # 保存logger到 loggers 字典
        self.loggers[f"{dag_id}_{execution_id}"] = logger

        # 留下註冊紀錄
        logger.info(f"Logger registered for dag: {dag_id}_{execution_id}  !!! ")

    def get_logger(self, dag_id: str, execution_id: str):
        """獲取已經初始化的日誌紀錄器"""
        return self.loggers.get(f"{dag_id}_{execution_id}")

    def logger_exists(self, dag_id: str, execution_id: str):
        """檢查logger是否存在"""
        return f"{dag_id}_{execution_id}" in self.loggers
    
    def log_section_header(self, logger, section_title: str):
        """記錄分隔標題以提升可讀性"""
        border = "*" * 60
        centered_title = f"**********[{section_title}]**********"
        logger.info(border)
        logger.info(border)
        logger.info(centered_title.center(60))
        logger.info(border)
        logger.info(border)