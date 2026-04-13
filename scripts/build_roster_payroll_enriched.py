"""
build_roster_payroll_enriched.py
--------------------------------
Merges the 2026 40-man roster CSV with all payroll Excel data to create
a single enriched player-level file. Every 40-man player gets a row.
Payroll columns are left blank for players not in the Excel files.
"""

import os
import sys
import unicodedata
import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROSTER_PATH = r"C:\Users\Ethan Davis\40man_rosters_2026.csv"
PAYROLL_DIR = r"C:\Users\Ethan Davis\Downloads\Payrolls"
OUT_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                        "data", "roster_payroll_2026_enriched.csv")

TEAM_MAP = {
    "Angels": "LAA", "Astros": "HOU", "Athletics": "ATH",
    "Blue Jays": "TOR", "Braves": "ATL", "Brewers": "MIL",
    "Cardinals": "STL", "Cubs": "CHC", "Diamondbacks": "ARI",
    "Dodgers": "LAD", "Giants": "SFG", "Guardians": "CLE",
    "Mariners": "SEA", "Marlins": "MIA", "Mets": "NYM",
    "Nationals": "WSN", "Orioles": "BAL", "Padres": "SDP",
    "Phillies": "PHI", "Pirates": "PIT", "Rangers": "TEX",
    "Rays": "TBR", "Red Sox": "BOS", "Reds": "CIN",
    "Rockies": "COL", "Royals": "KCR", "Tigers": "DET",
    "Twins": "MIN", "White Sox": "CHW", "Yankees": "NYY",
}

YEARS = [2026, 2027, 2028, 2029, 2030, 2031, 2032]

SHEET_MAP = {
    "Guaranteed": "Guaranteed",
    "Eligible For Arb": "Arb-Eligible",
    "Not Yet Eligible For Arb": "Pre-Arb",
    "No Longer On 40-Man Roster": "Off 40-Man",
}


def norm(s):
    """Normalize player name for matching."""
    if not isinstance(s, str):
        return ""
    try:
        s = s.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def parse_dollar(val):
    """Parse dollar string to float ($M). Returns None if not a number."""
    if pd.isna(val):
        return None
    s = str(val).replace(",", "").replace("$", "").strip()
    if not s or s.lower() in ("nan", "none", "tbd", ""):
        return None
    try:
        v = float(s)
        if abs(v) > 1000:
            v /= 1_000_000
        return round(v, 3)
    except ValueError:
        return None


def classify_cell(val):
    """Classify a year cell value: dollar amount, ARB status, or other."""
    if pd.isna(val):
        return None, None
    s = str(val).strip()
    su = s.upper()
    if "FREE AGENT" in su:
        return "FREE AGENT", None
    if "PRE-ARB" in su or "PRE ARB" in su:
        return "Pre-ARB", None
    if su.startswith("ARB"):
        return s, None  # "ARB 1", "ARB 2", etc.
    if "TBD" in su:
        return "TBD", None
    # Try parsing as dollar amount
    dollar = parse_dollar(val)
    if dollar is not None:
        return "Signed", dollar
    return None, None


# ---------------------------------------------------------------------------
# Load 40-man roster
# ---------------------------------------------------------------------------
roster = pd.read_csv(ROSTER_PATH)
roster["_key"] = roster["full_name"].apply(norm)
print(f"40-Man Roster: {len(roster)} players across {roster.team.nunique()} teams")

# ---------------------------------------------------------------------------
# Read all payroll Excel files into a single lookup
# ---------------------------------------------------------------------------
payroll_rows = []

for fname in sorted(os.listdir(PAYROLL_DIR)):
    if not fname.endswith(".xlsx"):
        continue
    team_name = fname.replace("-Payroll-2026.xlsx", "")
    team_abbr = TEAM_MAP.get(team_name, team_name[:3].upper())
    fpath = os.path.join(PAYROLL_DIR, fname)

    for sheet_name, stage_label in SHEET_MAP.items():
        try:
            df = pd.read_excel(fpath, sheet_name=sheet_name, header=0)
        except Exception:
            continue

        if "Player" not in df.columns:
            continue

        for _, row in df.iterrows():
            player = row.get("Player")
            if pd.isna(player):
                continue

            rec = {
                "_key": norm(str(player)),
                "pay_team": team_abbr,
                "pay_sheet": stage_label,
                "pay_player_name": str(player).strip(),
                "pay_age": row.get("Age"),
                "pay_service_time": row.get("Service Time"),
                "pay_contract": row.get("Contract"),
                "pay_info": row.get("Info"),
                "pay_aav_M": parse_dollar(row.get("AAV")),
                "pay_player_id": row.get("playerId"),
            }

            # Parse each year column
            for yr in YEARS:
                # Try int and float column names
                cell = row.get(yr, row.get(float(yr)))
                status, dollar = classify_cell(cell)
                rec[f"status_{yr}"] = status
                rec[f"salary_{yr}_M"] = dollar

            payroll_rows.append(rec)

pay_df = pd.DataFrame(payroll_rows)
# Deduplicate — keep first occurrence per player per team
pay_df = pay_df.drop_duplicates(subset=["_key", "pay_team"], keep="first")
print(f"Payroll Excel: {len(pay_df)} player entries from {pay_df.pay_team.nunique()} teams")

# ---------------------------------------------------------------------------
# Merge: 40-man roster LEFT JOIN payroll
# ---------------------------------------------------------------------------
merged = roster.merge(pay_df, left_on=["_key", "team"], right_on=["_key", "pay_team"], how="left")

# Clean up
drop_cols = ["_key", "pay_team"]
merged = merged.drop(columns=[c for c in drop_cols if c in merged.columns])

# Determine contract stage from payroll sheet
merged["contract_stage"] = merged["pay_sheet"]
# Players not in payroll: likely pre-arb or new to 40-man
merged.loc[merged["contract_stage"].isna(), "contract_stage"] = None

# Flag match status
merged["in_payroll"] = merged["pay_player_name"].notna()

# Reorder columns
roster_cols = ["player_id", "full_name", "team", "team_name", "position",
               "position_type", "jersey_number", "status", "status_code",
               "age", "bats", "throws", "birth_date", "height", "weight",
               "mlb_debut_date", "team_id"]
pay_cols = ["in_payroll", "contract_stage", "pay_contract", "pay_service_time",
            "pay_aav_M", "pay_info", "pay_player_id"]
year_cols = []
for yr in YEARS:
    year_cols.extend([f"salary_{yr}_M", f"status_{yr}"])
all_cols = [c for c in roster_cols + pay_cols + year_cols if c in merged.columns]
rest = [c for c in merged.columns if c not in all_cols and c not in ("pay_player_name", "pay_sheet", "pay_age")]
merged = merged[all_cols + rest]

# Save
merged.to_csv(OUT_PATH, index=False)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'='*60}")
print(f"ENRICHED ROSTER: {len(merged)} players")
print(f"{'='*60}")
print(f"In payroll:     {merged.in_payroll.sum()} ({merged.in_payroll.mean()*100:.1f}%)")
print(f"Not in payroll: {(~merged.in_payroll).sum()} ({(~merged.in_payroll).mean()*100:.1f}%)")
print()

print("By contract stage:")
for stg, cnt in merged.contract_stage.value_counts(dropna=False).items():
    label = stg if pd.notna(stg) else "(no payroll data)"
    print(f"  {label:20s}: {cnt:4d}")
print()

print("By roster status:")
for st, sub in merged.groupby("status"):
    n_pay = sub.in_payroll.sum()
    print(f"  {st:20s}: {len(sub):4d} total, {n_pay:4d} have payroll ({n_pay/len(sub)*100:.0f}%)")
print()

# Salary coverage
has_26_sal = merged["salary_2026_M"].notna().sum()
has_27_sal = merged["salary_2027_M"].notna().sum()
has_28_sal = merged["salary_2028_M"].notna().sum()
print(f"Players with 2026 salary: {has_26_sal}")
print(f"Players with 2027 salary: {has_27_sal}")
print(f"Players with 2028 salary: {has_28_sal}")
print()

# Total committed dollars
for yr in YEARS:
    col = f"salary_{yr}_M"
    total = merged[col].sum() if col in merged.columns else 0
    n = merged[col].notna().sum() if col in merged.columns else 0
    print(f"  {yr}: ${total:.0f}M committed across {n} players")

print(f"\nSaved: {OUT_PATH}")
