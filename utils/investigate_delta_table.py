# Please refer to the following documentation for more information
# about Delta Lake: https://delta-io.github.io/delta-rs/python/usage.html
import json
from pathlib import Path

from deltalake import DeltaTable
from load_config_from_file import load_cfg

CFG_FILE = "./configs/config.yaml"


def main():
    data_cfg = load_cfg(CFG_FILE)["data"]

    # Load Delta Lake table
    print("*" * 80)
    dt = DeltaTable(
        Path(data_cfg["deltalake_folder_path"]) / "text_comment_1", version=0
    )
    print("[INFO] Loaded Delta Lake table successfully!")

    # Investigate Delta Lake table
    print("*" * 80)
    print("[INFO] Delta Lake table schema:")
    print(json.loads(dt.schema().to_json()))
    print("*" * 40)
    print("[INFO] Delta Lake table's current version:")
    print(dt.version())
    print("*" * 40)
    print("[INFO] Delta Lake files:")
    print(dt.file_uris())
    print("*" * 40)

    # Query some data
    print("[INFO] Querying some data from the Delta Lake table:")
    print(dt.to_pandas(columns=["comment_text", "labels"]))
    print("*" * 40)

    # Investigate history of actions performed on the table
    print("[INFO] History of actions performed on the table")
    print(dt.history())
    print("*" * 40)


if __name__ == "__main__":
    main()
