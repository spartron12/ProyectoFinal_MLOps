FROM apache/airflow:2.6.0-python3.9
USER airflow
COPY requirements.txt /work/requirements.txt
RUN pip install --user --no-cache-dir -r /work/requirements.txt
EXPOSE 8080 8081 8082