import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from utils.postgresql_client import PostgresSQLClient
from utils.helpers import load_cfg

TABLE_NAME = "production.table_clean"
COLUMNS = "input_ids, attention_mask, labels"
CFG_PATH = "./configs/config.yaml"
cfg = load_cfg(CFG_PATH)["dw_postgres"]

def extract_and_save_data():
    pc = PostgresSQLClient(
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )

    query = f"SELECT {COLUMNS} FROM {TABLE_NAME}"
    conn = pc.create_conn()
    df = pd.read_sql(query, conn)
    conn.close()

    os.makedirs("./data/production", exist_ok=False)
    df.to_csv("data/production/cleaned_data.csv", index=False)
    print("✅ Data exported to data/production/cleaned_data.csv")

if __name__ == "__main__":
    extract_and_save_data()
