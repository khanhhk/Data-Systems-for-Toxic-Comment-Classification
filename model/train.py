from loguru import logger
from sklearn.metrics import classification_report, roc_auc_score
import os
import torch
from torch import nn, optim

from config import Config
from dataloader import val_dataloader, train_dataloader
from model import BertClassifier

device = Config.DEVICE
logger.info(f"Device: {device}")

classifier = BertClassifier().to(device)

for param in classifier.parameters():
    param.requires_grad = False
for param in classifier.linear1.parameters():
    param.requires_grad = True
for param in classifier.linear2.parameters():
    param.requires_grad = True

optimizer = optim.Adam([
    {'params': classifier.linear1.parameters(), 'lr': 5e-4},
    {'params': classifier.linear2.parameters(), 'lr': 1e-5}
])

loss_function = nn.BCEWithLogitsLoss()

for epoch in range(Config.TRAIN_EPOCHS):
    classifier.train()
    total_loss = 0
    logger.info(f"[Epoch {epoch+1}/{Config.TRAIN_EPOCHS}] Starting training...")

    for step, batch in enumerate(train_dataloader, start=1):
        optimizer.zero_grad()
        input_ids = batch["input_ids"].to(device)
        attention_mask = batch["attention_mask"].to(device)
        labels = batch["labels"].unsqueeze(1).float().to(device)

        outputs = classifier(input_ids, attention_mask)
        loss = loss_function(outputs, labels)
        loss.backward()
        optimizer.step()

        total_loss += loss.item()
        if step % 10 == 0 or step == len(train_dataloader):
            logger.info(f"[Epoch {epoch+1} | Step {step}/{len(train_dataloader)}] Batch Loss: {loss.item():.4f}")


    avg_train_loss = total_loss / len(train_dataloader)
    logger.info(f"[Epoch {epoch+1}] Training completed. Average Loss: {avg_train_loss:.4f}")

    classifier.eval()
    labels_all, preds_all, scores_all = [], [], []
    logger.info(f"[Epoch {epoch+1}] Starting validation...")

    with torch.no_grad():
        for batch in val_dataloader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].unsqueeze(1).to(device)

            scores = torch.sigmoid(classifier(input_ids, attention_mask))
            preds = (scores > Config.TOXIC_THRESHOLD).int()

            scores_all.extend(scores.cpu().numpy().flatten())
            preds_all.extend(preds.cpu().numpy())
            labels_all.extend(labels.cpu().numpy())

    auc = roc_auc_score(labels_all, scores_all)
    logger.info(f"[Epoch {epoch+1}] Validation AUC: {auc:.4f}")
    logger.info(f"[Epoch {epoch+1}] Classification Report:\n{classification_report(labels_all, preds_all)}")

    os.makedirs(Config.MODEL_FOLDER, exist_ok=True)
    model_path = os.path.join(Config.MODEL_FOLDER, f"checkpoint_epoch{epoch+1}.pt")
    torch.save(classifier.state_dict(), model_path)
    logger.info(f"[Epoch {epoch+1}] Model checkpoint saved: {model_path}")