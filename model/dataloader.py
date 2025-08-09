from datasets import load_dataset
from torch.utils.data import DataLoader
from transformers import DataCollatorWithPadding, AutoTokenizer
from config import Config

tokenizer = AutoTokenizer.from_pretrained(Config.MODEL_NAME)

dataset = load_dataset("csv", data_files=Config.DATA_FILE)['train']
dataset = dataset.train_test_split(test_size=Config.EVAL_SIZE, seed=0)

def preprocess_function(examples):
    return tokenizer(
        examples["comment_text"], 
        max_length=Config.MAX_LENGTH, 
        truncation=True)

tokenized_dataset = dataset.map(preprocess_function, batched=True)
tokenized_dataset = tokenized_dataset.remove_columns(['comment_text'])

data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

train_dataloader = DataLoader(
    tokenized_dataset["train"], 
    batch_size=Config.BATCH_SIZE,
    shuffle=True,
    collate_fn=data_collator
)

val_dataloader = DataLoader(
    tokenized_dataset["test"], 
    batch_size=Config.BATCH_SIZE, 
    collate_fn=data_collator
)