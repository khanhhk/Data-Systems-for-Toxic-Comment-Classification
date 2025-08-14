import torch
class Config:
    BATCH_SIZE = 64
    MAX_LENGTH = 512
    EVAL_SIZE = 0.3
    TRAIN_EPOCHS = 1
    MODEL_FOLDER = "./model/checkpoints"
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    TOXIC_THRESHOLD = 0.45
    MODEL_NAME = "distilbert-base-uncased"
    DATA_FILE = "./data/production/cleaned_data.csv"