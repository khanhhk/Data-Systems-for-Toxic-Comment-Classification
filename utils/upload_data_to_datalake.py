import os
from pathlib import Path

from load_config_from_file import load_cfg
from minio import Minio

CFG_FILE = "./configs/config.yaml"


def upload_local_directory_to_minio(minio_client, local_path, bucket_name, minio_path):
    local_path = Path(local_path)

    for local_file in local_path.rglob("*"):
        if local_file.is_file():
            remote_path = os.path.join(
                minio_path, str(local_file.relative_to(local_path))
            )
            minio_client.fput_object(bucket_name, remote_path, str(local_file))
            print(f"📤 Uploaded {local_file} → {bucket_name}/{remote_path}")


def main():
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    data_cfg = cfg["data"]

    minio_client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=datalake_cfg.get("secure", False),
    )

    if not minio_client.bucket_exists(datalake_cfg["bucket_name"]):
        minio_client.make_bucket(datalake_cfg["bucket_name"])
        print(f"✅ Created bucket: {datalake_cfg['bucket_name']}")
    else:
        print(f"ℹ️ Bucket {datalake_cfg['bucket_name']} already exists")

    upload_local_directory_to_minio(
        minio_client,
        data_cfg["deltalake_folder_path"],
        datalake_cfg["bucket_name"],
        datalake_cfg["folder_name"],
    )


if __name__ == "__main__":
    main()
