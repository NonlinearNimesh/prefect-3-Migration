FROM python:3.10-slim
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY . /app/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install prefect>=3.0.0
ENV PREFECT_API_URL="http://172.31.156.146:4200/api"
#RUN prefect project init --force
CMD ["prefect", "worker", "start", "--pool", "my-docker-pool"]
