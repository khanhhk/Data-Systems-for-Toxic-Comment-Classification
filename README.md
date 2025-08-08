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
After putting your files to `MinIO`, please execute `trino` container by the following command:
```shell
docker exec -ti datalake-trino bash
```

When you are already inside the `trino` container, typing `trino` to in an interactive mode

After that, run the following command to register a new schema for our data:

```sql
---Create a new schema to store tables
CREATE SCHEMA IF NOT EXISTS hive.m2_project
WITH (
    location = 's3://raw/'
);

---Create a new table in the newly created schema
CREATE TABLE IF NOT EXISTS hive.m2_project.olist (
    event_timestamp TIMESTAMP,
    pressure DOUBLE,
    velocity DOUBLE,
    speed DOUBLE
)
WITH (
    external_location = 's3://raw/olist/',
    format = 'PARQUET'
);


Truy cập trino via dbeaver với các thông tin:
+ host: localhost
+ port: 8080
+ username: trino

##
