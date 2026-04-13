"""Build enriched 40-man roster CSV merging all data sources."""
import pandas as pd
import os
import sys
import unicodedata
import zipfile
import io

sys.stdout.reconfigure(encoding="utf-8")

def norm(s):
    if not isinstance(s, str):
        return ""
    try:
        s = s.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()

# Paths
R40_PATH = os.path.expanduser("~/Downloads/40man_rosters_2025.csv")
COMB_PATH = os.path.expanduser("~/Desktop/MLB Data/Data/mlb_combined_2021_2025.csv")
PAY_ZIP = os.path.join(os.path.dirname(os.path.dirname(__file__)), "2026 Payroll.zip")
OUT_PATH = os.path.expanduser("~/Desktop/40man_roster_enriched.csv")

# 1. Load 40-man roster
r40 = pd.read_csv(R40_PATH)
r40["_key"] = r40["full_name"].apply(norm)
print(f"40-Man Roster: {len(r40)} players")

# 2. Load combined stats
comb = pd.read_csv(COMB_PATH, low_memory=False)
comb.columns = [c.strip() for c in comb.columns]
num_cols = ["Year", "WAR_Total", "Salary_M", "Age", "PA", "IP", "HR", "AVG", "ERA", "FIP", "WHIP"]
for c in num_cols:
    if c in comb.columns:
        comb[c] = pd.to_numeric(comb[c], errors="coerce")
comb["_key"] = comb["Player"].apply(norm)

comb25 = comb[comb["Year"] == 2025].drop_duplicates("_key", keep="first")
comb_best = comb.sort_values("Year", ascending=False).drop_duplicates("_key", keep="first")
print(f"Combined: {len(comb)} rows, {len(comb25)} in 2025")

# 3. Load payroll from zip
pay_rows = []
if os.path.exists(PAY_ZIP):
    with zipfile.ZipFile(PAY_ZIP) as z:
        for f in z.namelist():
            if f.endswith(".xlsx"):
                data = z.read(f)
                for sheet, stage in [("Guaranteed", "FA"), ("Eligible For Arb", "Arb"),
                                     ("Not Yet Eligible For Arb", "Pre-Arb")]:
                    try:
                        df = pd.read_excel(io.BytesIO(data), sheet_name=sheet)
                        if "Player" in df.columns:
                            for _, row in df.iterrows():
                                n = norm(str(row.get("Player", "")))
                                if n:
                                    pay_rows.append({
                                        "_key": n, "pay_stage": stage,
                                        "salary_2026": row.get("2026", row.get("AAV", None)),
                                        "salary_2027": row.get("2027", None),
                                        "salary_2028": row.get("2028", None),
                                    })
                    except Exception:
                        pass
pay_df = pd.DataFrame(pay_rows).drop_duplicates("_key", keep="first") if pay_rows else pd.DataFrame()
print(f"Payroll: {len(pay_df)} players from Excel")

# 4. Build enriched roster
out = r40[["player_id", "full_name", "position", "jersey_number", "status", "team", "_key"]].copy()

# Merge 2025 stats
s_cols = {c: c for c in ["_key", "WAR_Total", "Age", "PA", "IP", "HR", "AVG", "ERA", "FIP", "Stage_Clean", "Salary_M"]
          if c in comb25.columns}
rename25 = {"WAR_Total": "war_2025", "Age": "age", "PA": "pa_2025", "IP": "ip_2025",
            "HR": "hr_2025", "AVG": "avg_2025", "ERA": "era_2025", "FIP": "fip_2025",
            "Stage_Clean": "stage", "Salary_M": "salary_2025"}
merge25 = comb25[list(s_cols.keys())].rename(columns=rename25)
out = out.merge(merge25, on="_key", how="left")

# Merge best available for fallback
best_cols = [c for c in ["_key", "Year", "WAR_Total", "Age", "Stage_Clean"] if c in comb_best.columns]
best = comb_best[best_cols].rename(columns={"Year": "best_year", "WAR_Total": "best_war",
                                             "Age": "best_age", "Stage_Clean": "best_stage"})
out = out.merge(best, on="_key", how="left")
out["war_2025"] = out["war_2025"].fillna(out["best_war"])
out["age"] = out["age"].fillna(out["best_age"])
out["stage"] = out["stage"].fillna(out["best_stage"])

# Merge payroll
if not pay_df.empty:
    out = out.merge(pay_df, on="_key", how="left")
    out["stage"] = out["stage"].fillna(out.get("pay_stage"))
else:
    out["salary_2026"] = None
    out["salary_2027"] = None
    out["salary_2028"] = None

# Data status
out["data_status"] = "Full"
out.loc[out.war_2025.isna() & out.salary_2026.notna(), "data_status"] = "Payroll Only"
out.loc[out.war_2025.notna() & out.salary_2026.isna(), "data_status"] = "Stats Only"
out.loc[out.war_2025.isna() & out.salary_2026.isna(), "data_status"] = "No Data"

# Clean salary columns — strip $ and , and convert to numeric
for sc in ["salary_2026", "salary_2027", "salary_2028"]:
    if sc in out.columns:
        out[sc] = (out[sc].astype(str).str.replace(r"[\$,]", "", regex=True)
                   .str.strip().replace({"nan": None, "None": None, "": None}))
        out[sc] = pd.to_numeric(out[sc], errors="coerce")
        # If values are raw dollars (> 1000), convert to $M
        med = out[sc].dropna().median()
        if pd.notna(med) and med > 1000:
            out[sc] = out[sc] / 1_000_000

# Defaults
out.loc[out.data_status == "No Data", "stage"] = "Pre-Arb"
out.loc[out.salary_2026.isna(), "salary_2026"] = 0.74

# Clean
out = out.drop(columns=["_key", "best_war", "best_age", "best_stage", "pay_stage"], errors="ignore")

final_cols = ["player_id", "full_name", "position", "jersey_number", "status", "team",
              "stage", "age", "war_2025", "salary_2025", "salary_2026", "salary_2027", "salary_2028",
              "pa_2025", "ip_2025", "hr_2025", "avg_2025", "era_2025", "fip_2025",
              "best_year", "data_status"]
final_cols = [c for c in final_cols if c in out.columns]
out = out[final_cols]

out.to_csv(OUT_PATH, index=False)

print(f"\n=== ENRICHED 40-MAN ROSTER ===")
print(f"Total: {len(out)} players")
print(f"\nData coverage:")
for s, cnt in out.data_status.value_counts().items():
    print(f"  {s:15s}: {cnt:4d} ({cnt / len(out) * 100:.1f}%)")
print(f"\nPlayers with 2025 fWAR: {out.war_2025.notna().sum()}")
print(f"Players with known 2026 salary: {(out.salary_2026 > 0.74).sum()}")
print(f"Players at league min ($0.74M): {(out.salary_2026 == 0.74).sum()}")
print(f"\nSaved to: {OUT_PATH}")
