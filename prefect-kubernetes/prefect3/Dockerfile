FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy application code
COPY . /app/

# Install system dependencies
RUN apt-get update && apt-get install -y curl && apt-get clean

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip

# Install exact Prefect version to match server
RUN pip install prefect==3.2.10

# Install project dependencies
RUN pip install -r requirements.txt

# Prefect API URL (overridden at runtime by docker-compose, but fine to include here)
ENV PREFECT_API_URL="http://172.31.156.146:4200/api"

# Default command to generate and start the worker
#CMD ["sh", "-c", "python generate_prefect_yaml.py && prefect worker start --pool my-docker-pool"]
CMD ["sh", "-c", "python -v && prefect worker start --pool my-k8s-pool"]
