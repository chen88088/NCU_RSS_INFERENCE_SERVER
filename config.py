# config.py

# Server Manager Configuration
SERVER_MANAGER_URL = "http://10.52.52.136:8000"

# Machine Configuration
MACHINE_ID = "machine_server_1"
MACHINE_IP = "10.52.52.136"
MACHINE_PORT = 8085
MACHINE_CAPACITY = 2

# MinIO Configuration for dag
MINIO_URL = "http://10.52.52.138:31000"
MINIO_BUCKET = "testdvcfilemanagementfordag"
MINIO_ACCESS_KEY = "testdvctominio"
MINIO_SECRET_KEY = "testdvctominio"

# MinIO Configuration for dataset
DATASET_STORAGE_MINIO_URL = "10.52.52.138:31000"
DATASET_STORAGE_MINIO_BUCKET = "mock-dataset"
DATASET_STORAGE_MINIO_ACCESS_KEY = "testdvctominio"
DATASET_STORAGE_MINIO_SECRET_KEY = "testdvctominio"
