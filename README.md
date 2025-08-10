## System Architecture
![](images/Architecture.svg)

## Batch processing with pyspark
```shell
docker compose -f docker-compose.yaml up -d
```
### Push data to MinIO
```shell
python utils/write_delta_table.py
```

```shell
python utils/upload_data_to_datalake.py
```

```shell
python utils/investigate_delta_table.py
```

After pushing the data to MinIO, access `MinIO` at 
`http://localhost:9001/`, you should see your data already there.

### Create data schema
```shell
python utils/create_schema.py
python utils/create_table.py
```
### Batch processing
```shell
python batch_processing/main.py
```

## streaming processing with flink
```shell
docker compose -f stream-docker-compose.yaml up -d
```

```shell
cd debezium/
bash run.sh register_connector configs/postgresql-cdc.json
```
