import os
import torch
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent
class Config:
    BATCH_SIZE = 64
    MAX_LENGTH = 512
    EVAL_SIZE = 0.3
    TRAIN_EPOCHS = 1
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    TOXIC_THRESHOLD = 0.45
    MODEL_FOLDER = Path(os.getenv("MODEL_FOLDER", BASE_DIR / "model_checkpoints"))
    DATA_FILE = Path(os.getenv("DATA_FILE", BASE_DIR / "data/production/cleaned_data.csv"))
    MODEL_NAME = "distilbert-base-uncased"