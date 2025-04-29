#!/bin/bash

set -e

echo "Waiting for Prefect server to be ready..."

until [ "$(curl -s http://prefect-server:4200/api/health)" = "true" ]; do
  >&2 echo "Prefect server is unavailable - sleeping"
  sleep 2
done

echo "Prefect server is up - executing commands"
exec "$@"

