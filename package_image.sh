#!/bin/bash

# 設定參數
IMAGE_NAME=ncu-rss-inference-server
TAG=latest
REGISTRY=harbor.pdc.tw/moa_ncu
FULL_IMAGE=$REGISTRY/$IMAGE_NAME:$TAG

echo "🚀 Start packaging Docker image..."

# Step 1: Build image
echo "🔧 Building image: $IMAGE_NAME..."
docker build -t $IMAGE_NAME .

# Step 2: 取得 image ID
IMAGE_ID=$(docker images $IMAGE_NAME:latest -q)

# Step 3: Tag image
echo "🏷️ Tagging image ($IMAGE_ID) as $FULL_IMAGE..."
docker tag $IMAGE_ID $FULL_IMAGE

# Step 4: Push image
echo "📤 Pushing image to Harbor registry..."
docker push $FULL_IMAGE

echo "✅ Done! Image available at: $FULL_IMAGE"
