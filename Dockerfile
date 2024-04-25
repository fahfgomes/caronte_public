FROM apache/airflow:latest

USER root

RUN pip install 'apache-airflow[postgres,aws]'

USER airflow
