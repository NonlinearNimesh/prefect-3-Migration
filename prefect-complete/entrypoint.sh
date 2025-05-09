#!/bin/sh
set -e

echo "🔧 Starting container with WORK_POOL_INFRA=${WORK_POOL_INFRA}"

# Run logic based on infrastructure type
if [ "$WORK_POOL_INFRA" = "Docker" ]; then
    echo "🔧 Installing Docker CLI since WORK_POOL_INFRA is Docker..."
    apt-get update && apt-get install -y docker.io && apt-get clean
    python /app/create-docker-pool.py || echo "⚠️ Failed to create Kubernetes work pool"

    echo "🛠️ Created Docker work pool: ${WORK_POOL_INFRA}"
    #prefect work-pool create --type docker "$WORK_POOL_INFRA" || echo "⚠️ Work pool may already exist."

elif [ "$WORK_POOL_INFRA" = "Process" ]; then
    echo "🛠️ Creating Process work pool: ${WORK_POOL_INFRA}"
    prefect work-pool create --type process "$WORK_POOL_INFRA" || echo "⚠️ Work pool may already exist."

elif [ "$WORK_POOL_INFRA" = "Kubernetes" ]; then
    echo "🛠️ Running Kubernetes work pool setup script"
    python /app/create-k8s-pool.py || echo "⚠️ Failed to create Kubernetes work pool"
fi

echo "🚀 Starting Prefect worker for pool: ${WORK_POOL_INFRA}"
exec prefect worker start --pool "$WORK_POOL_INFRA"
