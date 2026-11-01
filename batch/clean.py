import pandas as pd

df = pd.read_csv(
    "data/household_power_consumption.txt",
    sep=";",
    na_values=["?"],
    low_memory=False,
)
print("lignes brutes :", len(df))

df["timestamp"] = pd.to_datetime(
    df["Date"] + " " + df["Time"], format="%d/%m/%Y %H:%M:%S"
)
df = df.drop(columns=["Date", "Time"])

manquantes = df.isna().any(axis=1).sum()
print("lignes avec valeur manquante :", manquantes)
df = df.dropna()

df.columns = [c.lower() for c in df.columns]
df = df[["timestamp"] + [c for c in df.columns if c != "timestamp"]]

df.to_csv("data/household_clean.csv", index=False)
print("lignes propres :", len(df))
print(df.head(3))