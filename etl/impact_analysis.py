"""
Phase 9: Impact analysis (SIMULATED - every saving below depends on ASSUMPTIONS).

Measured inputs : classifier confidence, accuracy, auto-route share (test set 2025+),
                  pipeline runtime (Airflow run, 2026-10-15).
Assumed inputs  : minutes to route a ticket by hand, minutes to fix a misrouted ticket,
                  hours to build the weekly report by hand.
Ticket volume   : the 2025+ narrative-bearing complaints in this dataset, treated as a
                  stand-in for a servicing queue. It is NOT a real company's ticket volume.

Run from the repo root (pandas only, no Spark, safe to run alongside other jobs):
    python etl/impact_analysis.py 2>&1 | tee impact_output.txt
"""
from pathlib import Path

import pandas as pd

PRED = "data/processed/ticket_predictions.parquet"
THRESHOLDS = [0.5, 0.6, 0.7, 0.8, 0.9]

# ---- ASSUMPTIONS (change these and re-run) -------------------------------------------
MANUAL_MIN_PER_TICKET = [1.0, 2.0, 3.0]   # minutes to read and route one ticket by hand
REWORK_MIN_PER_MISROUTE = 5.0             # minutes to detect and fix one misrouted ticket
MANUAL_REPORT_HOURS = 3.0                 # hours to build the weekly report by hand
# ---- MEASURED -------------------------------------------------------------------------
PIPELINE_MINUTES = 11.6                   # Airflow run on 2026-10-15: 11m 35s, all tasks green

d = pd.read_parquet(PRED)
d["date"] = pd.to_datetime(d["date_received"])
d["month"] = d["date"].dt.to_period("M").astype(str)

rows = []
for scope, x in [("all 2025+ test set", d), ("excluding Jan 2025", d[d["month"] != "2025-01"])]:
    weeks = max((x["date"].max() - x["date"].min()).days / 7, 1)
    per_week = len(x) / weeks
    for t in THRESHOLDS:
        m = x["confidence"] >= t
        auto_share = m.mean()
        acc = (x.loc[m, "issue_pred"] == x.loc[m, "issue_true"]).mean() if m.any() else float("nan")
        auto_per_week = per_week * auto_share
        breakeven = (1 - acc) * REWORK_MIN_PER_MISROUTE
        for mm in MANUAL_MIN_PER_TICKET:
            saved = auto_per_week * mm
            rework = auto_per_week * (1 - acc) * REWORK_MIN_PER_MISROUTE
            rows.append(
                {
                    "scope": scope,
                    "threshold": t,
                    "tickets_per_week": round(per_week),
                    "auto_routed_pct": round(auto_share * 100, 1),
                    "accuracy_on_auto_routed_pct": round(acc * 100, 1),
                    "assumed_manual_min_per_ticket": mm,
                    "assumed_rework_min_per_misroute": REWORK_MIN_PER_MISROUTE,
                    "net_hours_saved_per_week": round((saved - rework) / 60, 1),
                    "breakeven_manual_min_per_ticket": round(breakeven, 2),
                }
            )

out = pd.DataFrame(rows)
Path("reports").mkdir(exist_ok=True)
out.to_csv("reports/impact_analysis.csv", index=False)

print("=== SIMULATED NET HOURS SAVED PER WEEK (all 2025+ test set) ===")
print(f"Assumed rework per misrouted ticket: {REWORK_MIN_PER_MISROUTE} min")
base = out[out["scope"] == "all 2025+ test set"]
pivot = base.pivot(index="threshold", columns="assumed_manual_min_per_ticket", values="net_hours_saved_per_week")
pivot.columns = [f"{c:g} min/ticket" for c in pivot.columns]
print(f"Ticket volume used: about {int(base['tickets_per_week'].iloc[0]):,} per week (dataset volume, not a real queue)\n")
print(pivot.to_string())

print("\n=== BREAK-EVEN (manual minutes per ticket needed for savings to be positive) ===")
print(base.drop_duplicates("threshold")[["threshold", "auto_routed_pct", "accuracy_on_auto_routed_pct", "breakeven_manual_min_per_ticket"]].to_string(index=False))

print("\n=== SENSITIVITY: EXCLUDING JAN 2025 ===")
ex = out[out["scope"] == "excluding Jan 2025"]
pv2 = ex.pivot(index="threshold", columns="assumed_manual_min_per_ticket", values="net_hours_saved_per_week")
pv2.columns = [f"{c:g} min/ticket" for c in pv2.columns]
print(pv2.to_string())

print("\n=== WEEKLY REPORTING AUTOMATION ===")
saved = MANUAL_REPORT_HOURS - PIPELINE_MINUTES / 60
print(f"MEASURED   pipeline runtime : {PIPELINE_MINUTES} min (Airflow, 2026-10-15)")
print(f"ASSUMED    manual effort    : {MANUAL_REPORT_HOURS} h per weekly report")
print(f"SIMULATED  hours saved/week : {saved:.1f} (before dashboard review time)")
print("\nSaved: reports/impact_analysis.csv")
