import os
import pandas as pd

data_sample = "./data/raw/toxic_text.csv"
df = pd.read_csv(data_sample)
columns = df.columns

scripts = "CREATE SCHEMA IF NOT EXISTS hive.deltalake WITH (location = 's3://raw/');\n"

scripts += "CREATE TABLE IF NOT EXISTS hive.deltalake.toxic_text(\n"
for i, col in enumerate(columns):
    if col == "tpep_pickup_datetime" or col == "tpep_dropoff_datetime":
        scripts += col + " TIMESTAMP"
    elif df[col].dtype == "int64":
        scripts += col + " INT"
    elif df[col].dtype == "float64":
        scripts += col + " DOUBLE"
    else:
        scripts += col + " VARCHAR"

    if (i < len(columns) -1):
        scripts += ", \n"
    else: 
        scripts += "\n"

scripts = scripts[:-1] + "\n) WITH (\nexternal_location = 's3://raw/deltalake', \nformat = 'PARQUET' \n);"

print(scripts)