import pandas as pd
import os, sys, re

def clean_patch(text):
    if pd.isna(text): return ""
    return re.sub(r"[^\x20-\x7E\n\r\t]", "", str(text))

def main(data_dir, out_dir):
    df = pd.read_parquet(os.path.join(data_dir, "pr_commit_details.parquet"))
    df["patch"] = df["patch"].map(clean_patch)
    out = df[["pr_id","sha","message","filename","status","additions","deletions","changes","patch"]].copy()
    out.columns = ["PRID","PRSHA","PRCOMMITMESSAGE","PRFILE","PRSTATUS","PRADDS","PRDELSS","PRCHANGECOUNT","PRDIFF"]
    os.makedirs(out_dir, exist_ok=True)
    out.to_csv(os.path.join(out_dir,"task4_pr_commit_details.csv"), index=False)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
