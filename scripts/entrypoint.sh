#!/bin/bash

set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  pip install --upgrade pip
  pip install -r /opt/airflow/requirements.txt
fi

airflow db init

airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin || true

airflow db migrate

exec airflow webserver
