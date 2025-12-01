import pandas as pd
import os, sys

def main(data_dir, out_dir):
    df = pd.read_parquet(os.path.join(data_dir, "all_pull_request.parquet"))
    out = df[["title","id","agent","body","repo_id","repo_url"]].copy()
    out.columns = ["TITLE","ID","AGENTNAME","BODYSTRING","REPOID","REPOURL"]
    os.makedirs(out_dir, exist_ok=True)
    out.to_csv(os.path.join(out_dir,"task1_all_pull_request.csv"), index=False)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
