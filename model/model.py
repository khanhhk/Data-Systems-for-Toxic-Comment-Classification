import torch
from torch import nn
from transformers import DistilBertModel
from config import Config

class BertClassifier(nn.Module):
    def __init__(self):
        super().__init__()
        self.bert = DistilBertModel.from_pretrained(Config.MODEL_NAME)
        self.dropout = nn.Dropout(0.3)
        self.linear1 = nn.Linear(768, 3072)
        self.linear2 = nn.Linear(3072, 1)

    def forward(self, input_ids, attention_mask):
        vec = self.bert(input_ids=input_ids, attention_mask=attention_mask).last_hidden_state[:, 0, :]
        x = self.dropout(vec)
        x = torch.relu(self.linear1(x))
        x = self.linear2(x)
        return x