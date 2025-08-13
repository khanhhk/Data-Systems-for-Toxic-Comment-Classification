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

    create_table_m2_streaming = """
        CREATE TABLE IF NOT EXISTS m2.streaming( 
            comment_text VARCHAR, 
            labels BIGINT
        );
    """

    create_table_staging_streaming = """
        CREATE TABLE IF NOT EXISTS staging.streaming(
            id VARCHAR PRIMARY KEY,
            labels BIGINT,
            input_ids VARCHAR,
            attention_mask VARCHAR
        );
    """

    create_table_staging_test_1 = """
        CREATE TABLE IF NOT EXISTS staging.test_1(
            labels BIGINT,
            input_ids VARCHAR,
            attention_mask VARCHAR
        );
    """

    create_table_staging_test_2 = """
        CREATE TABLE IF NOT EXISTS staging.test_2(
            labels BIGINT,
            input_ids VARCHAR,
            attention_mask VARCHAR
        );
    """
    try:
        pc.execute_query(create_table_m2_streaming)
        pc.execute_query(create_table_staging_streaming)
        pc.execute_query(create_table_staging_test_1)
        pc.execute_query(create_table_staging_test_2)
    except Exception as e:
        print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()