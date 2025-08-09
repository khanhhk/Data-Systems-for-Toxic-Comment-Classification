## System Architecture
![](images/Architecture.svg)

```shell
docker compose -f docker-compose.yml up -d
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

## Create data schema
```shell
python utils/create_schema.py
python utils/create_table.py
```

## Trino
Truy cập trino via dbeaver với các thông tin:
+ host: localhost
+ port: 8080
+ username: trino

##
