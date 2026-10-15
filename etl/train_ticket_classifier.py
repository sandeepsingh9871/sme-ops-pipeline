"""
Phase 5: NLP ticket routing - predict the complaint ISSUE category from the
narrative text, with a confidence threshold for auto-routing vs manual review.

Design choices (documented for the README):
  * Target  = top-10 Issue values (by training frequency) + "Other issues"
  * Split   = by time (train < 2025-01-01, test >= 2025-01-01) to avoid leakage
  * Dedupe  = identical cleaned texts removed (template complaints) before split
  * Baseline = majority-class dummy model, to show real uplift

Run from the repo root:
    pip install pyarrow
    python etl/train_ticket_classifier.py --sample 100000   # quick run
    python etl/train_ticket_classifier.py                   # full run
"""
import argparse
import re
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
)

SRC = "data/processed/tickets_enriched"
SPLIT_DATE = pd.Timestamp("2025-01-01")
TOP_N = 10
MAX_CHARS = 3000
THRESHOLDS = [0.5, 0.6, 0.7, 0.8, 0.9]


def clean(text: str) -> str:
    text = text.lower()
    text = re.sub(r"\bx{2,}\b(?:/x{2,}\b)*", " ", text)  # redaction tokens: xxxx, xx/xx/xxxx
    text = re.sub(r"\d+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:MAX_CHARS]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=None, help="max training rows (quick run)")
    args = ap.parse_args()

    for d in ("reports", "models", "data/processed"):
        Path(d).mkdir(parents=True, exist_ok=True)

    # --- Load ------------------------------------------------------------------
    df = pd.read_parquet(
        SRC, columns=["Complaint ID", "Date received", "Product", "Issue", "narrative"]
    )
    df = df[df["narrative"].notna() & df["Issue"].notna()].copy()
    df["date"] = pd.to_datetime(df["Date received"])
    print(f"complaints with narrative + issue: {len(df):,}")

    # --- Clean + dedupe ----------------------------------------------------------
    df["text"] = df["narrative"].map(clean)
    df = df[df["text"].str.len() > 20]
    n0 = len(df)
    df = df.drop_duplicates(subset="text")
    print(f"identical-text duplicates removed: {n0 - len(df):,} -> {len(df):,} rows")

    # --- Time-based split ----------------------------------------------------------
    train = df[df["date"] < SPLIT_DATE]
    test = df[df["date"] >= SPLIT_DATE]
    if args.sample and len(train) > args.sample:
        train = train.sample(args.sample, random_state=42)
    print(f"train: {len(train):,} (< {SPLIT_DATE.date()})   test: {len(test):,}")

    top = train["Issue"].value_counts().head(TOP_N).index
    to_label = lambda s: s.where(s.isin(top), "Other issues")
    y_train, y_test = to_label(train["Issue"]), to_label(test["Issue"])

    print("\n=== CLASS DISTRIBUTION (train) ===")
    print((y_train.value_counts(normalize=True) * 100).round(1).to_string())

    # --- Features + models -----------------------------------------------------------
    vec = TfidfVectorizer(
        ngram_range=(1, 2), min_df=5, max_features=150_000,
        sublinear_tf=True, stop_words="english", dtype=np.float32,
    )
    X_train = vec.fit_transform(train["text"])
    X_test = vec.transform(test["text"])
    print(f"\nTF-IDF matrix: {X_train.shape[0]:,} x {X_train.shape[1]:,}")

    dummy = DummyClassifier(strategy="most_frequent").fit(X_train, y_train)
    base_f1 = f1_score(y_test, dummy.predict(X_test), average="macro")
    base_acc = accuracy_score(y_test, dummy.predict(X_test))

    clf = LogisticRegression(max_iter=300, C=3.0, class_weight="balanced")
    clf.fit(X_train, y_train)

    proba = clf.predict_proba(X_test)
    pred = clf.classes_[proba.argmax(axis=1)]
    conf = proba.max(axis=1)
    y_true = y_test.to_numpy()

    acc = accuracy_score(y_true, pred)
    macro_f1 = f1_score(y_true, pred, average="macro")

    header = (
        f"Baseline (majority class): accuracy={base_acc:.3f} macro_f1={base_f1:.3f}\n"
        f"Logistic regression     : accuracy={acc:.3f} macro_f1={macro_f1:.3f}\n"
    )
    report = classification_report(y_true, pred, digits=3)
    print("\n=== RESULTS (test = 2025+) ===")
    print(header)
    print(report)
    Path("reports/classifier_report.txt").write_text(header + "\n" + report)

    cm = pd.DataFrame(
        confusion_matrix(y_true, pred, labels=clf.classes_),
        index=clf.classes_, columns=clf.classes_,
    )
    cm.to_csv("reports/confusion_matrix.csv")

    # --- Auto-route vs manual-review trade-off -----------------------------------------
    rows = []
    for t in THRESHOLDS:
        m = conf >= t
        rows.append(
            {
                "threshold": t,
                "auto_routed_pct": round(m.mean() * 100, 1),
                "manual_review_pct": round((1 - m.mean()) * 100, 1),
                "accuracy_on_auto_routed_pct": round((pred[m] == y_true[m]).mean() * 100, 1) if m.any() else None,
            }
        )
    trade = pd.DataFrame(rows)
    trade.to_csv("reports/threshold_tradeoff.csv", index=False)
    print("=== CONFIDENCE THRESHOLD TRADE-OFF ===")
    print(trade.to_string(index=False))

    # --- Explainability: top terms per class ---------------------------------------------
    terms = np.array(vec.get_feature_names_out())
    print("\n=== TOP TERMS PER CLASS ===")
    for i, cls in enumerate(clf.classes_):
        idx = np.argsort(clf.coef_[i])[-8:][::-1]
        print(f"{cls[:60]:<60} {', '.join(terms[idx])}")

    # --- Persist model + predictions --------------------------------------------------
    joblib.dump(
        {"vectorizer": vec, "model": clf, "labels": list(clf.classes_), "top_issues": list(top)},
        "models/ticket_classifier.joblib", compress=3,
    )
    out = pd.DataFrame(
        {
            "complaint_id": test["Complaint ID"].to_numpy(),
            "date_received": test["date"].to_numpy(),
            "product": test["Product"].astype(str).to_numpy(),
            "issue_true": y_true,
            "issue_pred": pred,
            "confidence": conf.round(4),
        }
    )
    out.to_parquet("data/processed/ticket_predictions.parquet", index=False)
    print("\nSaved: models/ticket_classifier.joblib, data/processed/ticket_predictions.parquet,")
    print("       reports/classifier_report.txt, confusion_matrix.csv, threshold_tradeoff.csv")


if __name__ == "__main__":
    main()
