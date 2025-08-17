from load_config_from_file import load_cfg
from postgresql_client import PostgresSQLClient

CFG_PATH = "./configs/config.yaml"
cfg = load_cfg(CFG_PATH)["dw_postgres"]


def main():
    pc = PostgresSQLClient(
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )

    create_m2_schema = """CREATE SCHEMA IF NOT EXISTS m2;"""

    create_staging_schema = """CREATE SCHEMA IF NOT EXISTS staging;"""

    create_production_schema = """CREATE SCHEMA IF NOT EXISTS production;"""

    try:
        pc.execute_query(create_m2_schema)
        pc.execute_query(create_staging_schema)
        pc.execute_query(create_production_schema)
    except Exception as e:
        print(f"Failed to create schema with error: {e}")


if __name__ == "__main__":
    main()
