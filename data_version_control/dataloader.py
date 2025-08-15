from datasets import load_dataset
from torch.utils.data import DataLoader
from config import Config
import json
from transformers import DataCollatorWithPadding, AutoTokenizer

tokenizer = AutoTokenizer.from_pretrained(Config.MODEL_NAME)
data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

def parse_json_columns(example):
    example["input_ids"] = json.loads(example["input_ids"])
    example["attention_mask"] = json.loads(example["attention_mask"])
    return example

dataset = load_dataset("csv", data_files=str(Config.DATA_FILE))["train"]
dataset = dataset.train_test_split(test_size=Config.EVAL_SIZE, seed=0)
dataset = dataset.map(parse_json_columns)

train_dataloader = DataLoader(
    dataset["train"], 
    batch_size=Config.BATCH_SIZE,
    shuffle=True,
    collate_fn=data_collator
)

val_dataloader = DataLoader(
    dataset["test"], 
    batch_size=Config.BATCH_SIZE,
    collate_fn=data_collator
)