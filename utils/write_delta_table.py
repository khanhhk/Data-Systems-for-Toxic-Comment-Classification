import shutil
from pathlib import Path

import pandas as pd
from deltalake.writer import write_deltalake
from load_config_from_file import load_cfg

CFG_PATH = "./configs/config.yaml"

if __name__ == "__main__":
    cfg = load_cfg(CFG_PATH)

    csv_files = Path(cfg["data"]["folder_path"]).glob("*.csv")

    # Write data into deltalake format
    for csv_file in csv_files:
        file_name = csv_file.stem
        delta_path = Path(cfg["data"]["deltalake_folder_path"]) / file_name

        if delta_path.exists():
            shutil.rmtree(delta_path)

        try:
            df = pd.read_csv(csv_file)
            write_deltalake(str(delta_path), df)
            print(f"✅ Generated DeltaLake table: {file_name}")
        except Exception as e:
            print(f"❌ Failed to process {csv_file}: {e}")
