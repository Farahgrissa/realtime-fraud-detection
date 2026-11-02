import pandas as pd

df = pd.read_csv("data/paysim.csv")

# Stratified 80/20 split so both parts keep the same fraud rate
stream = df.groupby("isFraud", group_keys=False).sample(frac=0.2, random_state=42)
train = df.drop(stream.index)

train.to_csv("data/paysim_train.csv", index=False)
stream.sort_values("step").to_csv("data/paysim_stream.csv", index=False)
print(len(train), "train rows,", len(stream), "stream rows")