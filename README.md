# System Architecture
![](images/Architecture.svg)

# Project Structure
```txt
├── batch_processing                               
│    ├── main.py
│    ├── minio_config.py
│    └──  spark_session.py    
├── configs                              
│    └──  config.yaml              
├── data                               
│    ├── deltalake                                      
│    └── raw
├── debezium
│    ├── configs        
│    └── run.sh
├── images                            
├── model                              
│    ├── config.py                                      
│    ├── dataloader.py                             
│    ├── model.py                
│    └── train.py                    
├── stream_processing                             
│    ├── kafka_connect                       
│    ├── kafka_producer                
│    ├── datastream_api.py                  
│    ├── run.sh                 
│    └── table_api.py              
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
├── batch-docker-compose.yaml
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
### Run Batch Processing
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
 