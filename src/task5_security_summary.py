import pandas as pd
import os, sys, json

def contains_security(title, body, keywords):
    t = str(title).lower() if title is not None else ""
    b = str(body).lower() if body is not None else ""
    return int(any(kw.lower() in t or kw.lower() in b for kw in keywords))

def main(data_dir, out_dir, keywords_file=None):
    pr = pd.read_csv(os.path.join(out_dir, "task1_all_pull_request.csv"))
    types = pd.read_csv(os.path.join(out_dir, "task3_pr_task_type.csv"))

    if keywords_file and os.path.exists(keywords_file):
        with open(keywords_file, "r", encoding="utf-8") as f:
            try:
                keywords = json.load(f)
                if not isinstance(keywords, list):
                    raise ValueError("Keywords JSON must be a list")
            except json.JSONDecodeError:
                keywords = [line.strip() for line in f if line.strip()]
    else:
        keywords = [
            "security","vulnerability","xss","csrf","sqli","injection","rce",
            "authorization","authentication","crypto","credential","permissions",
            "privilege","sandbox","leak","exfiltration","secure","hardening",
            "patch","cve","exploit"
        ]

    merged = pr.merge(
        types[["PRID","PRTYPE","CONFIDENCE"]],
        left_on="ID", right_on="PRID", how="left"
    )

    merged["TITLE"] = merged["TITLE"].fillna("")
    merged["BODYSTRING"] = merged["BODYSTRING"].fillna("")

    merged["SECURITY"] = merged.apply(
        lambda r: contains_security(r["TITLE"], r["BODYSTRING"], keywords),
        axis=1
    )

    out = merged[["ID","AGENTNAME","PRTYPE","CONFIDENCE","SECURITY"]].copy()
    out.columns = ["ID","AGENT","TYPE","CONFIDENCE","SECURITY"]

    out["CONFIDENCE"] = pd.to_numeric(out["CONFIDENCE"], errors="coerce")
    out["SECURITY"] = out["SECURITY"].astype(int)

    os.makedirs(out_dir, exist_ok=True)
    out.to_csv(os.path.join(out_dir, "task5_pr_security_summary.csv"), index=False)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python src/task5_security_summary.py <DATA_DIR> <OUT_DIR> [KEYWORDS_FILE]")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) >= 4 else None)
