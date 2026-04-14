"""
build_team_payroll_totals.py
----------------------------
Extract team payroll totals from the Luxury Tax Payroll Estimate sheet
in each team's payroll Excel file for years 2021-2025.

2026 is EXCLUDED — it has its own separate pipeline.

Output: data/team_payroll_totals_2021_2026.csv

Columns:
  team, year, pre_tax_payroll_M, luxury_tax_payroll_M,
  cbt_threshold_M, over_cbt, cbt_overage_M,
  guaranteed_aav_M, arb_salaries_M, off_40man_aav_M,
  incentives_M, other_payments_M, other_payments_lux_M,
  pre_arb_estimated_M, minor_league_M, player_benefits_M,
  pre_arb_bonus_pool_M
"""

import os
import sys
import re
import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
YEARS = [2021, 2022, 2023, 2024, 2025, 2026]

PAYROLL_DIRS = {
    yr: os.path.expanduser(f"~/Downloads/{yr} Payroll")
    for yr in YEARS
}
# 2026 files are in a different folder
PAYROLL_DIRS[2026] = os.path.expanduser("~/Downloads/Payrolls")

OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
os.makedirs(OUT_DIR, exist_ok=True)

TEAM_MAP = {
    "Angels": "LAA", "Astros": "HOU", "Athletics": "ATH",
    "Blue Jays": "TOR", "Braves": "ATL", "Brewers": "MIL",
    "Cardinals": "STL", "Cubs": "CHC", "Diamondbacks": "ARI",
    "Dodgers": "LAD", "Giants": "SFG", "Guardians": "CLE",
    "Indians": "CLE",  # 2021 name
    "Mariners": "SEA", "Marlins": "MIA", "Mets": "NYM",
    "Nationals": "WSN", "Orioles": "BAL", "Padres": "SDP",
    "Phillies": "PHI", "Pirates": "PIT", "Rangers": "TEX",
    "Rays": "TBR", "Red Sox": "BOS", "Reds": "CIN",
    "Rockies": "COL", "Royals": "KCR", "Tigers": "DET",
    "Twins": "MIN", "White Sox": "CHW", "Yankees": "NYY",
}

CBT_THRESHOLDS = {
    2021: 210, 2022: 230, 2023: 233, 2024: 237, 2025: 241, 2026: 244,
}


def parse_dollar(val):
    """Convert dollar string to float in $M."""
    if pd.isna(val):
        return None
    s = str(val).replace(",", "").replace("$", "").strip()
    if not s or s.lower() in ("nan", "none", ""):
        return None
    try:
        v = float(s)
        if abs(v) > 1000:
            v /= 1_000_000
        return round(v, 3)
    except ValueError:
        return None


def extract_team_year(fpath, team_abbr, year):
    """Extract all line items from the Luxury Tax Payroll Estimate sheet."""
    try:
        df = pd.read_excel(fpath, sheet_name="Luxury Tax Payroll Estimate", header=None)
    except Exception as e:
        print(f"  WARNING: Could not read {fpath}: {e}")
        return None

    # Find the column for this year
    header_row = df.iloc[0]
    yr_col = None
    for ci, val in enumerate(header_row):
        v = str(val).strip()
        if v == str(year) or v == f"{year}.0":
            yr_col = ci
            break
    if yr_col is None:
        # Try column index 1 (first data column is usually the current year)
        yr_col = 1

    # Parse each row by matching description text
    result = {
        "team": team_abbr,
        "year": year,
    }

    for _, row in df.iterrows():
        desc = str(row.iloc[0]).strip().lower()
        val = parse_dollar(row.iloc[yr_col]) if yr_col < len(row) else None

        if "aavs for players with guaranteed" in desc and "does not include" in desc:
            # Active 40-man guaranteed contracts
            result["guaranteed_aav_M"] = val
        elif "aavs for players with guaranteed" in desc and "no longer on" in desc:
            # Off 40-man guaranteed contracts (dead money)
            result["off_40man_aav_M"] = val
        elif "salaries for players eligible for arb" in desc:
            result["arb_salaries_M"] = val
        elif "no longer on" in desc:
            result["off_40man_aav_M"] = val
        elif "earned incentives" in desc:
            result["incentives_M"] = val
        elif "other payments" in desc and "luxury tax" in desc:
            result["other_payments_lux_M"] = val
        elif "other payments" in desc:
            result["other_payments_M"] = val
        elif "not yet eligible" in desc or "non-guaranteed" in desc:
            result["pre_arb_estimated_M"] = val
        elif "minor leagues" in desc or "40-man roster players in minor" in desc:
            result["minor_league_M"] = val
        elif "player benefits" in desc:
            result["player_benefits_M"] = val
        elif "pre-arbitration bonus" in desc or "pre arbitration bonus" in desc:
            result["pre_arb_bonus_pool_M"] = val
        elif "estimated luxury tax payroll" in desc:
            result["luxury_tax_payroll_M"] = val

    # Compute pre-tax payroll (guaranteed + arb + pre-arb + off-40man player salaries)
    g = result.get("guaranteed_aav_M") or 0
    a = result.get("arb_salaries_M") or 0
    p = result.get("pre_arb_estimated_M") or 0
    o = result.get("off_40man_aav_M") or 0
    result["pre_tax_payroll_M"] = round(g + a + p + o, 3)

    # CBT
    cbt = CBT_THRESHOLDS.get(year, 244)
    lux = result.get("luxury_tax_payroll_M")
    result["cbt_threshold_M"] = cbt
    result["over_cbt"] = lux > cbt if lux is not None else None
    result["cbt_overage_M"] = round(lux - cbt, 1) if lux is not None else None

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
all_rows = []

for year in YEARS:
    payroll_dir = PAYROLL_DIRS[year]
    if not os.path.isdir(payroll_dir):
        print(f"{year}: Directory not found: {payroll_dir}")
        continue

    xlsx_files = sorted(f for f in os.listdir(payroll_dir) if f.endswith(".xlsx"))
    print(f"{year}: Processing {len(xlsx_files)} team files...")

    for fname in xlsx_files:
        # Extract team name from filename
        team_name = re.sub(r"-Payroll-\d{4}.*\.xlsx$", "", fname)
        # Handle duplicate files like "Padres-Payroll-2021 (1).xlsx"
        team_name = re.sub(r"\s*\(\d+\)$", "", team_name)
        team_abbr = TEAM_MAP.get(team_name, team_name[:3].upper())

        fpath = os.path.join(payroll_dir, fname)
        result = extract_team_year(fpath, team_abbr, year)
        if result:
            all_rows.append(result)

# Build DataFrame
df = pd.DataFrame(all_rows)

# Reorder columns
col_order = [
    "team", "year", "pre_tax_payroll_M", "luxury_tax_payroll_M",
    "cbt_threshold_M", "over_cbt", "cbt_overage_M",
    "guaranteed_aav_M", "arb_salaries_M", "off_40man_aav_M",
    "incentives_M", "other_payments_M", "other_payments_lux_M",
    "pre_arb_estimated_M", "minor_league_M", "player_benefits_M",
    "pre_arb_bonus_pool_M",
]
col_order = [c for c in col_order if c in df.columns]
df = df[col_order]

# Deduplicate (some teams may have duplicate files like "Padres (1)")
df = df.drop_duplicates(subset=["team", "year"], keep="first")

# Sort
df = df.sort_values(["year", "team"]).reset_index(drop=True)

# Save
out_path = os.path.join(OUT_DIR, "team_payroll_totals_2021_2026.csv")
df.to_csv(out_path, index=False)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n{'='*60}")
print(f"TEAM PAYROLL TOTALS 2021-2025")
print(f"{'='*60}")
print(f"Total rows: {len(df)} ({df['team'].nunique()} teams × {df['year'].nunique()} years)")
print()

for yr in YEARS:
    yr_df = df[df["year"] == yr]
    if yr_df.empty:
        continue
    over = yr_df[yr_df["over_cbt"] == True]
    avg_lux = yr_df["luxury_tax_payroll_M"].mean()
    top = yr_df.sort_values("luxury_tax_payroll_M", ascending=False).iloc[0]
    bot = yr_df.sort_values("luxury_tax_payroll_M", ascending=True).iloc[0]
    print(f"{yr}: {len(yr_df)} teams | Avg lux tax: ${avg_lux:.0f}M | "
          f"Over CBT (${CBT_THRESHOLDS[yr]}M): {len(over)} | "
          f"Top: {top['team']} ${top['luxury_tax_payroll_M']:.0f}M | "
          f"Bot: {bot['team']} ${bot['luxury_tax_payroll_M']:.0f}M")

print(f"\nSaved: {out_path}")
