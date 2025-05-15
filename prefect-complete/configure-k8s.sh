#!/bin/bash

set -e

NAMESPACE="namespace-prefect"
SERVICE_ACCOUNT_NAME="my-service-account-prefect"
CLUSTER_ROLE_BINDING_NAME="admin-binding"

echo "Creating namespace: $NAMESPACE"
kubectl create namespace $NAMESPACE || echo "Namespace already exists"

echo "Creating service account: $SERVICE_ACCOUNT_NAME in namespace: $NAMESPACE"
kubectl create serviceaccount $SERVICE_ACCOUNT_NAME -n $NAMESPACE || echo "Service account already exists"

echo "Creating cluster role binding: $CLUSTER_ROLE_BINDING_NAME"
kubectl create clusterrolebinding $CLUSTER_ROLE_BINDING_NAME \
  --clusterrole=cluster-admin \
  --serviceaccount=$NAMESPACE:$SERVICE_ACCOUNT_NAME || echo "ClusterRoleBinding already exists"

echo "Sleeping 5 seconds to ensure resources are created..."
sleep 5

echo "Creating a token for the service account..."
SERVICE_ACCOUNT_TOKEN=$(kubectl create token $SERVICE_ACCOUNT_NAME -n $NAMESPACE)
echo "Token for service account:"
echo "$SERVICE_ACCOUNT_TOKEN"

echo "Launching a test nginx pod in $NAMESPACE"
kubectl run my-test-pod --image=nginx --restart=Never -n $NAMESPACE || echo "Test pod creation failed"

echo "Done!"
