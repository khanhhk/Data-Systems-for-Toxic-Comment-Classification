import os
import shutil
from glob import glob

import pandas as pd
from helpers import load_cfg

from deltalake.writer import write_deltalake

CFG_PATH = "./config/datalake.yaml"

if __name__ == "__main__":
    cfg = load_cfg(CFG_PATH)
    
    csv_files = glob(cfg["data"]["folder_path"] + "/*.csv")

    # Write data into deltalake format
    for csv_file in csv_files:
        file_name = csv_file.split("/")[-1].split(".")[0]
        df = pd.read_csv(csv_file)
        if os.path.exists(
            os.path.join(cfg["data"]["deltalake_folder_path"], file_name)
        ):
            shutil.rmtree(
                os.path.join(cfg["data"]["deltalake_folder_path"], file_name)
            )
        write_deltalake(
            os.path.join(cfg["data"]["deltalake_folder_path"], file_name), df
        )
        print(f"Generated the file {file_name} successfully!")