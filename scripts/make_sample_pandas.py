# scripts/make_sample_pandas.py
import os
import glob
import hashlib
import random
import pandas as pd

BASE = "data/log_search/log_search"
OUT = "data/sample"
SAMPLE_USERS = 200

files = glob.glob(os.path.join(BASE, "**", "*.parquet"), recursive=True) + glob.glob(
    os.path.join(BASE, "**", "*.csv"), recursive=True
)
if not files:
    raise SystemExit(f"No parquet or csv files found in {BASE}")

# load a few files until we have enough rows (avoid loading the entire dataset)
rows = []
count = 0
for f in files:
    if f.endswith(".parquet"):
        part = pd.read_parquet(f)
    else:
        part = pd.read_csv(f)
    rows.append(part)
    count += len(part)
    if count > 20000:
        break

df = pd.concat(rows, ignore_index=True)


# anonymize user id
def hash_id(x):
    return hashlib.sha256(str(x).encode()).hexdigest()[:16]


if "user_id" not in df.columns:
    raise SystemExit("input data missing user_id column")

df["anon_user_id"] = df["user_id"].apply(hash_id)

users = df["anon_user_id"].dropna().unique().tolist()
sample_users = random.sample(users, min(SAMPLE_USERS, len(users)))

sample_df = df[df["anon_user_id"].isin(sample_users)].copy()

os.makedirs(OUT, exist_ok=True)
sample_df.to_parquet(os.path.join(OUT, "sample.parquet"), index=False)
print(f"Wrote sample with {len(sample_df)} rows to {OUT}")
