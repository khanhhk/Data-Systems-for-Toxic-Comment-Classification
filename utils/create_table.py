from postgresql_client import PostgresSQLClient
from helpers import load_cfg
CFG_PATH = "./configs/config.yaml"
cfg = load_cfg(CFG_PATH)["dw_postgres"]

def main():

    pc = PostgresSQLClient(
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )

    create_table_m2 = """
        CREATE TABLE IF NOT EXISTS m2.streaming( 
            comment_text VARCHAR, 
            labels INT
        );
    """

    create_table_staging = """
        CREATE TABLE IF NOT EXISTS staging.streaming(
            labels INT,
            input_ids VARCHAR,
            attention_mask VARCHAR
        );
    """
    try:
        pc.execute_query(create_table_m2)
        pc.execute_query(create_table_staging)
    except Exception as e:
        print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()