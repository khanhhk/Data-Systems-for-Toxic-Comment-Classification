import os
import sys
from time import sleep
import pandas as pd
import numpy as np
from helpers import load_cfg
from postgresql_client import PostgresSQLClient

TABLE_NAME = "m2.streaming"
CSV_FILE = "./data/raw/toxic_text.csv"
NUM_ROWS = 10000
SLEEP_SECS = 2


CFG_PATH = "./configs/config.yaml"
cfg = load_cfg(CFG_PATH)["dw_postgres"]


def main():
    pc = PostgresSQLClient(
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )

    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
        print(f"Columns in {TABLE_NAME}: {columns}")
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")
        return

    parse_date_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    usecols = None
    try:
        df = pd.read_csv(
            CSV_FILE,
            nrows=NUM_ROWS,
            usecols=usecols,
            low_memory=False,
            # parse các cột datetime nếu có
            parse_dates=[c for c in parse_date_cols if c in parse_date_cols],
        )
    except Exception as e:
        print(f"Failed to read CSV: {e}")
        return

    # Chuẩn hóa kiểu dữ liệu để khớp Postgres (ví dụ: chuyển datetime -> str ISO)
    for col in parse_date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    # Đảm bảo DataFrame chỉ còn các cột đúng thứ tự bảng
    missing = [c for c in columns if c not in df.columns]
    if missing:
        print(f"CSV is missing columns required by table: {missing}")
        return
    df = df[columns].copy()

    # Thay NaN bằng None để psycopg2 hiểu là NULL
    df = df.replace({np.nan: None})

    # Insert từng dòng để mô phỏng stream
    # Parameterized query để tránh lỗi quote & injection
    placeholders = ",".join(["%s"] * len(columns))
    col_list = ",".join(columns)
    sql = f"INSERT INTO {TABLE_NAME} ({col_list}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        rec = row.tolist()
        try:
            # Nếu PostgresSQLClient.execute_query hỗ trợ params:
            pc.execute_query(sql, params=rec)
            print(f"Sent: {format_record(row)}")
        except TypeError:
            # Fallback: nếu execute_query KHÔNG có params, dùng string build (ít an toàn hơn)
            # Cân nhắc tự escape hoặc chỉnh PostgresSQLClient cho phép params.
            values_sql = tuple(rec)
            pc.execute_query(f"INSERT INTO {TABLE_NAME} ({col_list}) VALUES {values_sql}")
            print(f"Sent (fallback): {format_record(row)}")
        except Exception as e:
            print(f"Insert failed: {e}")
        print("-" * 100)
        sleep(SLEEP_SECS)


def format_record(row):
    taxi_res = {k: (None if pd.isna(v) else (str(v) if "datetime" in k else v))
                for k, v in row.to_dict().items()}
    return taxi_res


if __name__ == "__main__":
    main()
