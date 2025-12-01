import pandas as pd
import os, sys

def main(data_dir, out_dir):
    df = pd.read_parquet(os.path.join(data_dir, "pr_task_type.parquet"))
    out = df[["id","title","reason","type","confidence"]].copy()
    out.columns = ["PRID","PRTITLE","PRREASON","PRTYPE","CONFIDENCE"]
    os.makedirs(out_dir, exist_ok=True)
    out.to_csv(os.path.join(out_dir,"task3_pr_task_type.csv"), index=False)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
