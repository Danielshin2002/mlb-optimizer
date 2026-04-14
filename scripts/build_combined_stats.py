"""
build_combined_stats.py
-----------------------
Build mlb_combined_2021_2025.csv from FanGraphs batting + pitching leaderboard
CSVs plus payroll Excel files in Downloads/NewData/{year} Payroll/.

Each year folder contains:
  fangraphs-leaderboards (56).csv  — batting  (all players, 0 PA min)
  fangraphs-leaderboards (57).csv  — pitching (all players, 0 IP min)
  {Team}-Payroll-{year}.xlsx       — payroll workbooks (contract, age, salary)

Output columns:
  - All batting + pitching stats from FanGraphs
  - fWAR: unified WAR (Ohtani = bat+pit, everyone else = max of bat/pit)
  - Age, Service_Time, Salary, Contract, Contract_Length, Contract_Total, Stage
  - Position, Bats, Throws (fetched from MLB API via MLBAMID)

Output: data/mlb_combined_2021_2025.csv
"""

import json
import os
import re
import sys
import time
import urllib.request
import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
YEARS = [2021, 2022, 2023, 2024, 2025]
BASE_DIR = os.path.expanduser("~/Downloads/NewData")
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
os.makedirs(OUT_DIR, exist_ok=True)

# Current year (all payroll files downloaded in 2026, so ages reflect 2026)
CURRENT_YEAR = 2026

TEAM_MAP = {
    "Angels": "LAA", "Astros": "HOU", "Athletics": "ATH",
    "Blue Jays": "TOR", "Braves": "ATL", "Brewers": "MIL",
    "Cardinals": "STL", "Cubs": "CHC", "Diamondbacks": "ARI",
    "Dodgers": "LAD", "Giants": "SFG", "Guardians": "CLE",
    "Indians": "CLE",
    "Mariners": "SEA", "Marlins": "MIA", "Mets": "NYM",
    "Nationals": "WSN", "Orioles": "BAL", "Padres": "SDP",
    "Phillies": "PHI", "Pirates": "PIT", "Rangers": "TEX",
    "Rays": "TBR", "Red Sox": "BOS", "Reds": "CIN",
    "Rockies": "COL", "Royals": "KCR", "Tigers": "DET",
    "Twins": "MIN", "White Sox": "CHW", "Yankees": "NYY",
}

# Stage mapping for "No Longer On 40-Man" based on next-year column value
NEXT_YEAR_STAGE = {
    "pre-arb": "Pre-Arb",
    "arb 1":   "Pre-Arb",
    "arb 2":   "Arbitration",
    "arb 3":   "Arbitration",
    "arb 4":   "Arbitration",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean_col(name) -> str:
    """Strip BOM and whitespace from column names."""
    return str(name).strip().lstrip("\ufeff")


def load_fg(year: int, kind: str) -> pd.DataFrame:
    """Load a FanGraphs leaderboard CSV (batting or pitching) for a given year."""
    num = "56" if kind == "batting" else "57"
    path = os.path.join(BASE_DIR, f"{year} Payroll", f"fangraphs-leaderboards ({num}).csv")
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = [clean_col(c) for c in df.columns]
    return df


def parse_dollar(val) -> float | None:
    """Convert dollar string like '$36,000,000' to float."""
    if pd.isna(val):
        return None
    s = str(val).replace(",", "").replace("$", "").strip()
    if not s or s.lower() in ("nan", "none", "", "free agent", "arb 1", "arb 2",
                                "arb 3", "arb 4", "club option", "player option",
                                "mutual option", "vesting option", "pre-arb"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_contract(contract_str: str) -> tuple[int | None, float | None]:
    """Parse '9 yr, $360M (2023-31)' into (length=9, total=360000000)."""
    if not isinstance(contract_str, str) or not contract_str.strip():
        return None, None

    length = None
    total = None

    m = re.search(r"(\d+)\s*yr", contract_str, re.IGNORECASE)
    if m:
        length = int(m.group(1))

    m = re.search(r"\$([0-9.,]+)\s*M", contract_str, re.IGNORECASE)
    if m:
        total = float(m.group(1).replace(",", "")) * 1_000_000

    return length, total


def classify_off_40man_stage(row, year: int) -> str:
    """Determine stage for a player on the 'No Longer On 40-Man' sheet
    by checking the year+1 column value."""
    next_yr = year + 1
    next_val = None
    for key in [next_yr, str(next_yr), float(next_yr)]:
        v = row.get(key)
        if v is not None and pd.notna(v):
            next_val = str(v).strip().lower()
            break
    if next_val and next_val in NEXT_YEAR_STAGE:
        return NEXT_YEAR_STAGE[next_val]
    return "Free Agent"


def load_payroll_year(year: int) -> pd.DataFrame:
    """Load all payroll Excel files for a year and return a combined DataFrame."""

    payroll_dir = os.path.join(BASE_DIR, f"{year} Payroll")
    xlsx_files = sorted(f for f in os.listdir(payroll_dir) if f.endswith(".xlsx"))

    sheet_stage_map = {
        "Guaranteed":                 "Free Agent",
        "Eligible For Arb":           "Arbitration",
        "Not Yet Eligible For Arb":   "Pre-Arb",
        "No Longer On 40-Man Roster": None,  # determined per-row
    }

    all_rows = []
    for fname in xlsx_files:
        fpath = os.path.join(payroll_dir, fname)

        for sheet_name, default_stage in sheet_stage_map.items():
            try:
                df = pd.read_excel(fpath, sheet_name=sheet_name, header=0)
            except Exception:
                continue

            df.columns = [clean_col(c) for c in df.columns]

            if "Player" not in df.columns or "playerId" not in df.columns:
                continue

            for _, row in df.iterrows():
                pid = row.get("playerId")
                if pd.isna(pid):
                    continue
                try:
                    pid = int(float(pid))
                except (ValueError, TypeError):
                    continue

                # Age: adjust from current (2026) to the season year
                raw_age = row.get("Age")
                age = None
                if pd.notna(raw_age):
                    try:
                        age = round(float(raw_age) - (CURRENT_YEAR - year), 1)
                    except (ValueError, TypeError):
                        pass

                # Service time (already accurate per year)
                svc = row.get("Service Time")
                svc_val = None
                if pd.notna(svc):
                    try:
                        svc_val = round(float(svc), 3)
                    except (ValueError, TypeError):
                        pass

                # Contract string
                contract = row.get("Contract")
                contract_str = str(contract).strip() if pd.notna(contract) else None
                c_length, c_total = parse_contract(contract_str) if contract_str else (None, None)

                # Salary: the value under the year column (not AAV)
                salary = None
                for yr_key in [year, str(year), float(year)]:
                    if yr_key in df.columns:
                        salary = parse_dollar(row.get(yr_key))
                        if salary is not None:
                            break

                # Stage
                if default_stage is not None:
                    stage = default_stage
                else:
                    stage = classify_off_40man_stage(row, year)

                all_rows.append({
                    "playerId": pid,
                    "Age": age,
                    "Service_Time": svc_val,
                    "Contract": contract_str,
                    "Contract_Length": c_length,
                    "Contract_Total": c_total,
                    "Salary": salary,
                    "Stage": stage,
                })

    payroll_df = pd.DataFrame(all_rows)

    # Deduplicate: a player might appear in multiple team files (traded)
    # Keep the one with highest salary
    if not payroll_df.empty:
        payroll_df = (
            payroll_df.sort_values("Salary", ascending=False, na_position="last")
                      .drop_duplicates(subset=["playerId"], keep="first")
                      .reset_index(drop=True)
        )

    return payroll_df


# ---------------------------------------------------------------------------
# MLB API: fetch Position, Bats, Throws
# ---------------------------------------------------------------------------
MLB_API_CACHE_PATH = os.path.join(OUT_DIR, "_mlb_api_player_cache.json")
BATCH_SIZE = 100  # MLB API supports comma-separated IDs


def load_mlb_cache() -> dict:
    if os.path.exists(MLB_API_CACHE_PATH):
        with open(MLB_API_CACHE_PATH, "r") as f:
            return json.load(f)
    return {}


def save_mlb_cache(cache: dict):
    with open(MLB_API_CACHE_PATH, "w") as f:
        json.dump(cache, f)


def fetch_mlb_people(mlbam_ids: list[int], cache: dict) -> dict:
    """Fetch player info from MLB API in batches. Returns {mlbamid: {pos, bats, throws}}."""
    # Filter out already-cached and invalid IDs
    to_fetch = [mid for mid in mlbam_ids if str(mid) not in cache and mid > 0]

    if to_fetch:
        print(f"  Fetching {len(to_fetch)} players from MLB API ({len(cache)} cached)...")
        for i in range(0, len(to_fetch), BATCH_SIZE):
            batch = to_fetch[i:i + BATCH_SIZE]
            ids_str = ",".join(str(int(x)) for x in batch)
            url = f"https://statsapi.mlb.com/api/v1/people?personIds={ids_str}"
            try:
                resp = urllib.request.urlopen(url, timeout=30)
                data = json.loads(resp.read())
                for p in data.get("people", []):
                    mid = str(p["id"])
                    cache[mid] = {
                        "Position": p.get("primaryPosition", {}).get("abbreviation", ""),
                        "Bats": p.get("batSide", {}).get("code", ""),
                        "Throws": p.get("pitchHand", {}).get("code", ""),
                    }
            except Exception as e:
                print(f"    WARNING: MLB API batch failed: {e}")
            # Small delay to be nice to the API
            if i + BATCH_SIZE < len(to_fetch):
                time.sleep(0.3)

        save_mlb_cache(cache)
        print(f"    Done. Cache now has {len(cache)} players.")
    else:
        print(f"  MLB API: all {len(mlbam_ids)} players already cached.")

    return cache


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
all_frames = []

for year in YEARS:
    print(f"\n{'='*60}")
    print(f"Processing {year}")
    print(f"{'='*60}")

    # --- Load FanGraphs stats ---
    bat = load_fg(year, "batting")
    pit = load_fg(year, "pitching")
    print(f"  Batting:  {len(bat)} players")
    print(f"  Pitching: {len(pit)} players")

    # --- Rename pitching columns to avoid clashes ---
    pit_rename = {}
    for c in pit.columns:
        if c in ("Name", "Team", "NameASCII", "PlayerId", "MLBAMID"):
            continue
        if c == "G":
            pit_rename[c] = "G_pit"
        elif c == "BABIP":
            pit_rename[c] = "BABIP_pit"
        elif c == "WAR":
            pit_rename[c] = "WAR_pit"
        else:
            pit_rename[c] = c
    pit = pit.rename(columns=pit_rename)

    # --- Merge batting + pitching on PlayerId ---
    merged = pd.merge(bat, pit, on="PlayerId", how="outer", suffixes=("", "_pit_dup"))

    for col in ("Name", "Team", "NameASCII", "MLBAMID"):
        dup = f"{col}_pit_dup"
        if dup in merged.columns:
            merged[col] = merged[col].fillna(merged[dup])
            merged.drop(columns=[dup], inplace=True)

    # --- Classify player type ---
    has_bat = merged["PA"].notna() & (merged["PA"] > 0)
    has_pit = merged["IP"].notna() & (merged["IP"] > 0)
    merged["Pitcher"] = (~has_bat & has_pit).astype(int)

    # --- Compute fWAR ---
    bat_war = pd.to_numeric(merged.get("WAR", 0), errors="coerce").fillna(0)
    pit_war = pd.to_numeric(merged.get("WAR_pit", 0), errors="coerce").fillna(0)

    is_ohtani = merged["Name"].str.contains("Shohei Ohtani", na=False)

    merged["fWAR"] = 0.0
    merged.loc[has_bat & ~has_pit, "fWAR"] = bat_war[has_bat & ~has_pit]
    merged.loc[~has_bat & has_pit, "fWAR"] = pit_war[~has_bat & has_pit]
    both = has_bat & has_pit & ~is_ohtani
    merged.loc[both & (bat_war.abs() >= pit_war.abs()), "fWAR"] = bat_war[both & (bat_war.abs() >= pit_war.abs())]
    merged.loc[both & (bat_war.abs() < pit_war.abs()), "fWAR"] = pit_war[both & (bat_war.abs() < pit_war.abs())]
    merged.loc[is_ohtani, "fWAR"] = bat_war[is_ohtani] + pit_war[is_ohtani]
    merged["fWAR"] = merged["fWAR"].round(3)

    # --- Load payroll data ---
    payroll = load_payroll_year(year)
    print(f"  Payroll:  {len(payroll)} players from Excel files")

    # Merge payroll into stats on PlayerId
    if not payroll.empty:
        merged = pd.merge(
            merged, payroll,
            left_on="PlayerId", right_on="playerId",
            how="left",
        )
        if "playerId" in merged.columns:
            merged.drop(columns=["playerId"], inplace=True)

    # --- Add metadata ---
    merged["Year"] = year
    merged["Player"] = merged["Name"]

    all_frames.append(merged)

    # --- Year summary ---
    matched = merged["Age"].notna().sum()
    stage_counts = merged["Stage"].value_counts().to_dict() if "Stage" in merged.columns else {}
    print(f"\n  Merged: {len(merged)} total players")
    print(f"    With payroll data: {matched}")
    print(f"    Stages: {stage_counts}")

    ohtani = merged[merged["Name"].str.contains("Ohtani", na=False)]
    if not ohtani.empty:
        o = ohtani.iloc[0]
        print(f"  Ohtani: age={o.get('Age')}, stage={o.get('Stage')}, fWAR={o['fWAR']}")


# ---------------------------------------------------------------------------
# Combine all years
# ---------------------------------------------------------------------------
combined = pd.concat(all_frames, ignore_index=True)

# ---------------------------------------------------------------------------
# Fetch Position, Bats, Throws from MLB API (once for all unique MLBAMIDs)
# ---------------------------------------------------------------------------
print(f"\n{'='*60}")
print("Fetching player info from MLB API")
print(f"{'='*60}")

mlbam_ids = combined["MLBAMID"].dropna().astype(int).unique().tolist()
mlb_cache = load_mlb_cache()
mlb_cache = fetch_mlb_people(mlbam_ids, mlb_cache)

# Map into the DataFrame
combined["Position"] = combined["MLBAMID"].apply(
    lambda x: mlb_cache.get(str(int(x)), {}).get("Position", "") if pd.notna(x) else ""
)
# Refine pitchers: SP if GS > 4, else RP (keep TWP as-is)
_is_p = combined["Position"] == "P"
_gs = pd.to_numeric(combined["GS"], errors="coerce").fillna(0)
combined.loc[_is_p & (_gs > 4), "Position"] = "SP"
combined.loc[_is_p & (_gs <= 4), "Position"] = "RP"
combined["Bats"] = combined["MLBAMID"].apply(
    lambda x: mlb_cache.get(str(int(x)), {}).get("Bats", "") if pd.notna(x) else ""
)
combined["Throws"] = combined["MLBAMID"].apply(
    lambda x: mlb_cache.get(str(int(x)), {}).get("Throws", "") if pd.notna(x) else ""
)

# ---------------------------------------------------------------------------
# Backward-compatibility columns for the app
# ---------------------------------------------------------------------------
# WAR_Total = fWAR (app references WAR_Total throughout)
combined["WAR_Total"] = combined["fWAR"]

# Salary_M = Salary / 1,000,000 (app expects Salary_M in millions)
combined["Salary_M"] = pd.to_numeric(combined["Salary"], errors="coerce") / 1_000_000

# Stage_Clean: "Free Agent" → "FA", "Arbitration" → "Arb", "Pre-Arb" stays
_stage_map = {"Free Agent": "FA", "Arbitration": "Arb", "Pre-Arb": "Pre-Arb"}
combined["Stage_Clean"] = combined["Stage"].map(_stage_map).fillna("")

# ---------------------------------------------------------------------------
# Final column ordering
# ---------------------------------------------------------------------------
id_cols = ["Player", "Name", "NameASCII", "PlayerId", "MLBAMID",
           "Team", "Year", "Position", "Bats", "Throws", "Pitcher",
           "Age", "Service_Time", "Stage",
           "Contract", "Contract_Length", "Contract_Total", "Salary"]

bat_cols = ["G", "PA", "HR", "R", "RBI", "SB", "BB%", "K%",
            "ISO", "BABIP", "AVG", "OBP", "SLG",
            "wOBA", "xwOBA", "wRC+", "BsR", "Off", "Def", "WAR"]

pit_cols = ["W", "L", "SV", "G_pit", "GS", "IP",
            "K/9", "BB/9", "HR/9", "BABIP_pit",
            "LOB%", "GB%", "HR/FB", "vFA (pi)",
            "ERA", "xERA", "FIP", "xFIP", "WAR_pit"]

summary_cols = ["fWAR", "WAR_Total", "Salary_M", "Stage_Clean"]

ordered = []
for c in id_cols + bat_cols + pit_cols + summary_cols:
    if c in combined.columns:
        ordered.append(c)
for c in combined.columns:
    if c not in ordered:
        ordered.append(c)
combined = combined[ordered]

# Sort
combined = combined.sort_values(["Year", "Team", "fWAR"], ascending=[True, True, False]).reset_index(drop=True)

# Save
out_path = os.path.join(OUT_DIR, "mlb_combined_2021_2025.csv")
combined.to_csv(out_path, index=False)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'='*60}")
print(f"COMBINED STATS 2021-2025")
print(f"{'='*60}")
print(f"Total rows: {len(combined)}")
print(f"Unique players: {combined['PlayerId'].nunique()}")
print(f"Columns ({len(combined.columns)}): {list(combined.columns)}")
print(f"Payroll match rate: {combined['Salary'].notna().sum()}/{len(combined)} "
      f"({100*combined['Salary'].notna().mean():.0f}%)")
print(f"Position fill rate: {(combined['Position'] != '').sum()}/{len(combined)} "
      f"({100*(combined['Position'] != '').mean():.0f}%)")
print()

stage_dist = combined["Stage"].value_counts()
print("Stage distribution:")
for s, n in stage_dist.items():
    print(f"  {s}: {n}")
print()

for yr in YEARS:
    yr_df = combined[combined["Year"] == yr]
    top = yr_df.sort_values("fWAR", ascending=False).iloc[0]
    with_sal = yr_df["Salary"].notna().sum()
    print(f"  {yr}: {len(yr_df)} players | payroll: {with_sal} | "
          f"top: {top['Player']} ({top['Team']}) fWAR={top['fWAR']:.1f}")

print(f"\nSaved: {out_path}")
