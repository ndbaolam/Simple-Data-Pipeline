FROM apache/airflow:2.10.5
ADD airflow-requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r airflow-requirements.txt