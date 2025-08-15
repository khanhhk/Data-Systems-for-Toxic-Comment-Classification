import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from utils.postgresql_client import PostgresSQLClient
from utils.helpers import load_cfg
from loguru import logger

TABLE_NAME = "production.table_clean"
COLUMNS = "input_ids, attention_mask, labels"
CFG_PATH = "./configs/config.yaml"
cfg = load_cfg(CFG_PATH)["dw_postgres"]

def is_running_in_docker():
    return os.environ.get("ENV_TYPE") == "docker"
    
def extract_and_save_data():
    is_docker = is_running_in_docker()
    host = cfg.get("host_docker") if is_docker else cfg.get("host")
    logger.info(f"🔌 Using PostgreSQL host: {host}")

    pc = PostgresSQLClient(
        host=host,
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )

    query = f"SELECT {COLUMNS} FROM {TABLE_NAME}"
    conn = pc.create_conn()
    df = pd.read_sql(query, conn)
    conn.close()

    os.makedirs("./data/production", exist_ok=True)
    df.to_csv("data/production/cleaned_data.csv", index=False)
    print("✅ Data exported to data/production/cleaned_data.csv")

if __name__ == "__main__":
    extract_and_save_data()
