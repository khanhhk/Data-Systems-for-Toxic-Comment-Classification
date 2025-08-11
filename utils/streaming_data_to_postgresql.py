from helpers import load_cfg
from postgresql_client import PostgresSQLClient
import time
import pandas as pd

TABLE_NAME = "m2.streaming"
CSV_FILE = "./data/raw/test_1.csv"
SLEEP_SECS = 2


CFG_PATH = "./configs/config.yaml"
cfg = load_cfg(CFG_PATH)["dw_postgres"]


def main():
    pc = PostgresSQLClient(
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )

    columns_idx = pc.get_columns(table_name=TABLE_NAME)
    columns = list(columns_idx) if columns_idx is not None else []

    if len(columns) == 0:
        raise RuntimeError(f"No columns resolved for table {TABLE_NAME}")
    print(f"Columns in {TABLE_NAME}: {columns}")

    df = pd.read_csv(CSV_FILE)
    placeholders = ",".join(["%s"] * len(columns))
    col_list = ",".join(columns)
    insert_sql = f"INSERT INTO {TABLE_NAME} ({col_list}) VALUES ({placeholders})"

    for idx, row in df.iterrows():
        values = tuple([row[c] for c in columns])
        try:
            pc.execute_query_params(insert_sql, values)
            print(f"Inserted row {idx}: " + str({c: v for c, v in zip(columns, values)}))
        except Exception as e:
            print(f"Failed to insert row {idx}: {e}")
        time.sleep(SLEEP_SECS)

if __name__ == "__main__":
    main()


