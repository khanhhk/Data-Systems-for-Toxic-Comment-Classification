# System Architecture
![](images/Architecture.svg)

# Project Structure
```txt
├── .dvc
│    ├── .gitignore
│    └──  config
├── airflow                               
│    ├── dags
│    ├── Dockerfile
│    └── requirements
├── batch_processing                               
│    ├── main.py
│    ├── minio_config.py
│    └── spark_session.py    
├── configs                              
│    └──  config.yaml              
├── data                               
│    ├── deltalake
│    ├── kafka                                  
│    └── raw
├── data_transformation                             
│    ├── analyses
│    ├── macros
│    ├── models
│    ├── seeds
│    ├── snapshots
│    ├── .gitignore
│    ├── .user.yml
│    ├── dbt_project.yml
│    ├── packages.yml                        
│    └── profiles.yml
├── data_validation                            
│    ├── gx
│    ├── full_flow.ipynb                                 
│    └── reload_and_validate.ipynb
├── debezium
│    ├── configs        
│    └── run.sh                  
├── dvc                              
│    ├── config.py                                      
│    ├── dataloader.py
│    ├── extract_data.py                          
│    ├── model.py
│    ├── requirements.txt           
│    └── train.py
├── images
├── jars
├── monitoring                             
│    ├── alertmanager        
│    ├── elk
│    ├── grafana            
│    ├── prometheus              
│    └── prom-graf-docker-compose.yaml         
├── stream_processing                                          
│    └── main.py              
├── trino
│    ├── catalog
│    └── etc
├── utils                                           
│    ├── create_schema.py         
│    ├── create_table.py                         
│    ├── helpers.py
│    ├── investigate_delta_table.py
│    ├── postgresql_client.py
│    ├── streaming_data_to_postgresql.py
│    ├── upload_data_to_datalake.py
│    └── write_delta_table.py
├── .dvcignore
├── .env
├── .gitignore
├── airflow-docker-compose.yaml
├── batch-docker-compose.yaml
├── dvc.lock
├── dvc.yaml
├── requirements.txt
└── stream-docker-compose.yaml
```

# Table of contents

1. [Batch Processing with PySpark](#1-batch-processing-with-pyspark)
    1. [Start Services](#11-start-services)

    2. [Push Data to MinIO](#12-push-data-to-minio)

    3. [Create Data Schema and Tables](#13-create-data-schema-and-tables)

    4. [Run Batch Processing](#14-run-batch-processing)

2. [Stream Processing with Apache Flink](#2-stream-processing-with-apache-flink)

    1. [Start Services](#21-start-services)

    2. [Register Connectors](#22-register-connectors)

    3. [Initialize the Database](#23-initialize-the-database)

    4. [Run Stream Processing](#14-run-stream-processing)

## 1. Batch Processing with PySpark
### 1.1 Start Services
```shell
docker compose -f batch-docker-compose.yaml up -d
```
### 1.2 Push Data to MinIO
Run the following commands in sequence:
```shell
python utils/write_delta_table.py
python utils/upload_data_to_datalake.py
python utils/investigate_delta_table.py
```
Once completed, access `MinIO` at `http://localhost:9001/` to verify the uploaded data.

### 1.3 Create Data Schema and Tables
```shell
python utils/create_schema.py
python utils/create_table.py
```
### 1.4 Run Batch Processing
```shell
python batch_processing/main.py
```

## 2. Stream Processing with Apache Flink
### 2.1 Start Services
```shell
docker compose -f stream-docker-compose.yaml up -d
```
### 2.2 Register Connectors
Connect `Debezium` with `PostgreSQL` to capture Change Data Capture (CDC) events:
```shell
cd debezium/
bash run.sh register_connector configs/postgresql-cdc.json
```
Access Debezium UI at `http://localhost:8085/`.


### 2.3 Initialize the Database
Periodically insert new records into the target table:
```shell
python utils/streaming_data_to_postgresql.py
```

Access the `Control Center` at `http://localhost:9021/` to monitor incoming records.

### 2.4 Run Stream Processing
```shell
python stream_processing/main.py
```

### 3.1 Data validation with Great Expectations
You can find examples of data validation using Great Expectations in the `notebooks` folder `full_flow.ipynb` and `reload_and_validate.ipynb`.

Great Expectations is a Python-based library that allows you to define, manage, and validate expectations about data in your data pipelines and projects.

### 3.2 Data transformation with dbt
```bash
dbt clean
dbt deps
dbt build --target prod
```

### 3.3 Data Version Control (DVC)
```bash
dvc init
git add .dvc .gitignore
git commit -m "Initialize DVC"
```

```bash
dvc remote add -d minio_remote s3://raw/dvc
dvc remote modify minio_remote endpointurl http://localhost:9000
dvc remote modify minio_remote access_key_id minio_access_key
dvc remote modify minio_remote secret_access_key minio_secret_key
dvc remote modify minio_remote use_ssl false
```

```bash
dvc stage add -n extract_data \
  -d data_version_control/extract_data.py \
  -o data/production/cleaned_data.csv \
  python data_version_control/extract_data.py
```

```bash
dvc stage add -n train_model \
  -d data_version_control/train.py \
  -d data_version_control/dataloader.py \
  -d data_version_control/model.py \
  -d data_version_control/config.py \
  -d data/production/cleaned_data.csv \
  python data_version_control/train.py
```

```bash
dvc repro
git add dvc.yaml dvc.lock .dvc/config
git commit -m "Add DVC pipeline"
dvc push
git push origin dev
```
### 3.4 Airflow
Start the docker compose for Airflow:
```shell
docker compose -f airflow-docker-compose.yaml up -d
```

## 4. Monitoring
This section demonstrates how to monitor your services locally using ELK Stack, Jaeger, Prometheus, Grafana, and Alertmanager.
#### 4.1 Elastic Search
Start the ELK stack with Filebeat using the following command:
```bash
cd monitoring/elk
docker compose -f elk-docker-compose.yml -f extensions/filebeat/filebeat-compose.yml up -d
```
You can access Kibana at [http://localhost:5601](http://localhost:5601) to explore logs collected by Filebeat from container output and shipped to Elasticsearch. Credentials for Kibana are defined in `local/elk/.env`.

#### 4.2 Jaeger
Jaeger helps trace the execution time of specific code blocks across your microservices.

To start Jaeger with Prometheus and Grafana:
```bash
cd local
docker compose -f prom-graf-docker-compose.yaml up -d
```
Access Jaeger at [http://localhost:16686](http://localhost:16686).
+ Automatic tracing
```bash
cd instrument/traces
opentelemetry-instrument uvicorn embedding_trace_automatic:app
```

In the Jaeger UI, traces for instrumented code blocks will be displayed on the right-hand side, allowing you to analyze execution durations and dependencies.

+ Manual tracing
```bash
cd instrument/traces
uvicorn embedding_trace_manual:app
```

#### 4.3 Prometheus
Prometheus is available at [http://localhost:9090](http://localhost:9090). You can query any available metric via the UI. Click the highlighted dropdown to list all metrics currently being scraped by Prometheus.

#### 4.4 Grafana
Grafana can be accessed at [http://localhost:3001](http://localhost:3001) with default credentials: `username: admin, password: admin`. You can either create your own dashboards or import community dashboards from Grafana Labs.

For example, the following dashboard (imported from Grafana Labs) visualizes container-level metrics such as CPU usage, memory usage, and memory cache using cAdvisor and Prometheus.

Additionally, you can build custom dashboards to monitor both node-level and application-specific resource usage.


#### 4.5 Alertmanager
While monitoring services and infrastructure, you can define custom alerting rules to notify when resource usage exceeds predefined thresholds. These rules and notification settings are configured in `alertmanager/config.yml`.

In this project, Alertmanager is configured to send alerts to Discord in the following scenarios:
+ When the available memory on a node drops below 5%.
+ When the embedding vector size differs from 768.
+ When the embedding service consumes more than 1.5 GB of RAM.