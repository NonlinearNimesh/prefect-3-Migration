#!/bin/sh
set -e

echo "🔧 Starting container with WORK_POOL_INFRA=${WORK_POOL_INFRA}"

# Wait for Prefect Server to be healthy
echo "⏳ Waiting for Prefect server to become healthy..."
until curl -s http://prefect-server:4200/api/health | grep true > /dev/null; do
  echo "❌ Prefect server is unavailable - sleeping"
  sleep 2
done
echo "✅ Prefect server is healthy!"
sleep 20  # Optional buffer time

# Create work pool based on infrastructure type and generate deployment YAML
if [ "$WORK_POOL_INFRA" = "Docker" ]; then
    echo "🔧 Installing Docker CLI since WORK_POOL_INFRA is Docker..."
    apt-get update && apt-get install -y docker.io && apt-get clean

    echo "🛠️ Creating Docker work pool..."
    python /app/create-docker-pool.py || echo "⚠️ Failed to create Docker work pool"

    echo "📄 Generating Docker-specific prefect.yaml..."
    python /app/generate_prefect_docker_yaml.py

elif [ "$WORK_POOL_INFRA" = "Process" ]; then
    echo "🛠️ Creating Process work pool..."
    prefect work-pool create --type process "$WORK_POOL_INFRA" || echo "⚠️ Work pool may already exist."

    echo "📄 Generating Process-specific prefect.yaml..."
    python /app/generate_prefect_yaml.py

elif [ "$WORK_POOL_INFRA" = "Kubernetes" ]; then
    echo "🛠️ Creating Kubernetes work pool..."
    python /app/create-k8s-cluster-pool.py || echo "⚠️ Failed to create Kubernetes work pool"

    echo "📄 Generating Kubernetes-specific prefect.yaml..."
    python /app/generate_prefect_k8s_yaml.py
fi

# Deploy all flows
echo "🚀 Deploying all flows..."
prefect deploy --all || echo "⚠️ Flow deployment failed"

# Start worker
echo "🧰 Starting Prefect worker for pool: ${WORK_POOL_INFRA}"
exec prefect worker start --pool "$WORK_POOL_INFRA"
