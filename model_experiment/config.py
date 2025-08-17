import os
from pathlib import Path

import torch


class Config:
    BATCH_SIZE = 64
    MAX_LENGTH = 512
    EVAL_SIZE = 0.3
    TRAIN_EPOCHS = 1
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    TOXIC_THRESHOLD = 0.45
    MODEL_FOLDER = Path(os.getenv("MODEL_FOLDER", "model_checkpoints"))
    DATA_FILE = Path(os.getenv("DATA_FILE", "data/production/cleaned_data.csv"))
    MODEL_NAME = "distilbert-base-uncased"
    MLFLOW_TRACKING_URI = os.getenv(
        "MLFLOW_TRACKING_URI", f"file:{Path('mlruns').resolve()}"
    )
