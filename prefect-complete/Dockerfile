
FROM prefecthq/prefect:3.4.0-python3.12

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy application code and entrypoint script
COPY . /app/
COPY entrypoint.sh /entrypoint.sh


RUN chmod +x /entrypoint.sh \
 && apt-get update && apt-get install -y curl && apt-get clean

# Install Python dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt

