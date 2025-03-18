# 使用與本地環境一致的 Python 版本
FROM python:3.10-slim

# 建立工作目錄
WORKDIR /app

# 更新套件並安裝 Git
RUN apt update && \
    apt install -y git && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

# 複製專案檔案到 Docker Image 中
COPY . .

# 安裝依賴
RUN pip install --no-cache-dir -r requirements.txt

# 暴露 API Port
EXPOSE 8001

# 啟動 FastAPI 應用程式
CMD ["uvicorn", "NCU_RSS_TRAINING_SERVER:app", "--host", "0.0.0.0", "--port", "8001"]
