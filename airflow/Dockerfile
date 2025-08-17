FROM apache/airflow:2.7.1-python3.9

USER root

RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y libgomp1

USER airflow
# This is to fix a bug in Airflow with PostgreSQL connection
RUN pip install git+https://github.com/mpgreg/airflow-provider-great-expectations.git@87a42e275705d413cd4482134fc0d94fa1a68e6f

# Requirement for running Docker Operator
COPY ./requirements.txt /requirements.txt
RUN pip install --user -r /requirements.txt
ENV PATH="/home/airflow/.local/bin:$PATH"