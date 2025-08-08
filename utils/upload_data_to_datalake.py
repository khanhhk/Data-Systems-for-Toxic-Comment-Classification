import glob
import os

from helpers import load_cfg
from minio import Minio

CFG_FILE = "./config/datalake.yaml"


def upload_local_directory_to_minio(minio_client, local_path, bucket_name, minio_path):
    assert os.path.isdir(local_path)

    for local_file in glob.glob(local_path + "/**"):
        if not os.path.isfile(local_file):
            upload_local_directory_to_minio(
                minio_client,
                local_file,
                bucket_name,
                minio_path + "/" + os.path.basename(local_file),
            )
        else:
            remote_path = os.path.join(minio_path, local_file[1 + len(local_path) :])
            minio_client.fput_object(bucket_name, remote_path, local_file)


def main():
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    data_cfg = cfg["data"]

    # Create a client with the MinIO server playground, its access key
    # and secret key.
    minio_client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False,
    )

    # Create bucket if not exist.
    found = minio_client.bucket_exists(bucket_name=datalake_cfg["bucket_name"])
    if not found:
        minio_client.make_bucket(bucket_name=datalake_cfg["bucket_name"])
    else:
        print(f'Bucket {datalake_cfg["bucket_name"]} already exists, skip creating!')

    # Upload files.
    upload_local_directory_to_minio(
        minio_client,
        data_cfg["deltalake_folder_path"],
        datalake_cfg["bucket_name"],
        datalake_cfg["folder_name"],
    )


if __name__ == "__main__":
    main()
