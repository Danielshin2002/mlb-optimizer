"""
fill_roster_blanks.py
---------------------
Takes roster_payroll_2026_enriched.csv and fills in blank payroll cells
for players not in the Excel files using MLB service time rules:

- League min: $780K (2026), +$20K each year (2027=$800K, 2028=$820K, etc.)
- Pre-Arb: debut within 3 seasons (2023-2025) or no debut yet
- Arb: debut 2020-2022 (3-6 service years by 2026)
- FA: debut before 2020 (6+ service years) — rare for blank players
- Arb salary estimates: Year 1 = $2M, Year 2 = $4M, Year 3 = $7M, Year 4 = $10M
"""

import os
import sys
import pandas as pd
import numpy as np

sys.stdout.reconfigure(encoding="utf-8")

INPUT = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                     "data", "roster_payroll_2026_enriched.csv")
OUTPUT = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                      "data", "roster_payroll_2026_complete.csv")

YEARS = [2026, 2027, 2028, 2029, 2030, 2031, 2032]

# League minimum by year ($M)
LEAGUE_MIN = {yr: round(0.780 + (yr - 2026) * 0.020, 3) for yr in YEARS}
# 2026: 0.780, 2027: 0.800, 2028: 0.820, etc.

# Estimated arb salaries ($M) by arb year
ARB_SALARY = {"ARB 1": 2.0, "ARB 2": 4.0, "ARB 3": 7.0, "ARB 4": 10.0}


def compute_service_years(debut_date_str, as_of_year=2026):
    """Estimate MLB service years from debut date to a given year."""
    if pd.isna(debut_date_str):
        return None
    try:
        debut = pd.to_datetime(debut_date_str)
        return as_of_year - debut.year
    except Exception:
        return None


def determine_stage_and_fill(row):
    """For a blank-payroll player, determine contract stage and fill salary/status for each year."""
    debut = row.get("mlb_debut_date")
    svc = compute_service_years(debut, 2026)

    # Determine stage progression starting from 2026
    if svc is None or svc <= 0:
        # No debut or debuted 2026+ — pre-arb for foreseeable future
        base_stage = "Pre-Arb"
        arb_start_year = None  # unknown
    elif svc <= 3:
        # Debuted 2023-2025 — pre-arb in 2026, enters arb later
        base_stage = "Pre-Arb"
        debut_yr = pd.to_datetime(debut).year
        arb_start_year = debut_yr + 3  # arb starts 3 years after debut
    elif svc <= 6:
        # Debuted 2020-2022 — in arbitration window
        base_stage = "Arb"
        debut_yr = pd.to_datetime(debut).year
        arb_start_year = debut_yr + 3
    else:
        # 7+ years — should be FA (rare for blank players)
        base_stage = "FA"
        arb_start_year = None

    results = {"contract_stage": base_stage}

    for yr in YEARS:
        sal_col = f"salary_{yr}_M"
        stat_col = f"status_{yr}"

        # Skip if already has data
        if pd.notna(row.get(sal_col)) or pd.notna(row.get(stat_col)):
            continue

        if svc is None:
            # No debut — pre-arb at league min
            results[sal_col] = LEAGUE_MIN[yr]
            results[stat_col] = "Pre-ARB"
            continue

        debut_yr = pd.to_datetime(debut).year
        years_since_debut = yr - debut_yr

        if years_since_debut < 3:
            # Pre-arb
            results[sal_col] = LEAGUE_MIN[yr]
            results[stat_col] = "Pre-ARB"
        elif years_since_debut == 3:
            results[sal_col] = ARB_SALARY["ARB 1"]
            results[stat_col] = "ARB 1"
        elif years_since_debut == 4:
            results[sal_col] = ARB_SALARY["ARB 2"]
            results[stat_col] = "ARB 2"
        elif years_since_debut == 5:
            results[sal_col] = ARB_SALARY["ARB 3"]
            results[stat_col] = "ARB 3"
        elif years_since_debut == 6:
            # Could be ARB 4 (super two) or FA
            results[stat_col] = "FREE AGENT"
            results[sal_col] = None
        else:
            # FA
            results[stat_col] = "FREE AGENT"
            results[sal_col] = None

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
df = pd.read_csv(INPUT)
print(f"Loaded: {len(df)} players")
print(f"Already have payroll: {df.in_payroll.sum()}")
print(f"Need filling: {(~df.in_payroll).sum()}")
print()

# Process blank players
filled_count = 0
for idx, row in df.iterrows():
    if row["in_payroll"]:
        continue

    updates = determine_stage_and_fill(row)

    for col, val in updates.items():
        if col in df.columns:
            if pd.isna(df.at[idx, col]) or col == "contract_stage":
                df.at[idx, col] = val
        else:
            df.at[idx, col] = val

    filled_count += 1

print(f"Filled {filled_count} players")
print()

# Summary
print("Contract stage distribution (all players):")
for stg, cnt in df.contract_stage.value_counts(dropna=False).items():
    label = stg if pd.notna(stg) else "(still blank)"
    print(f"  {label:20s}: {cnt:4d}")
print()

# Salary coverage after fill
for yr in YEARS:
    col = f"salary_{yr}_M"
    if col in df.columns:
        n = df[col].notna().sum()
        total = df[col].sum()
        print(f"  {yr}: {n:4d} players with salary, ${total:.0f}M total")

print()

# Save
df.to_csv(OUTPUT, index=False)
print(f"Saved: {OUTPUT}")
