import os
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient
load_dotenv(".env")

def main():

    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    create_table_m2 = """
        CREATE TABLE IF NOT EXISTS m2.toxic_text( 
            comment_text VARCHAR, 
            labels INT
        );
    """

    create_table_staging = """
        CREATE TABLE IF NOT EXISTS staging.toxic_text(
            comment_text VARCHAR, 
            labels INT
        );
    """
    try:
        pc.execute_query(create_table_m2)
        pc.execute_query(create_table_staging)
    except Exception as e:
        print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()