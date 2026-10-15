#!/usr/bin/env bash
# Download the CFPB Consumer Complaint Database Narratives Archive
# (FOIA Reading Room) exports #2-#21 = May 2018 .. Aug 2026, then unzip them.
# Resumable: finished zips are skipped on re-run.
#
# Usage (from repo root):  bash scripts/download_narratives.sh
set -euo pipefail

BASE="https://files.consumerfinance.gov/f/documents"
ZIP_DIR="data/raw/narratives_zips"
OUT_DIR="data/raw/narratives"
mkdir -p "$ZIP_DIR" "$OUT_DIR"

FILES=(
  CCDB_Export_2_May_2018_through_April_2021.zip
  CCDB_Export_3_May_2021_through_October_2022.zip
  CCDB_Export_4_November_2022_through_August_2023.zip
  CCDB_Export_5_September_2023_through_March_2024.zip
  CCDB_Export_6_April_2024_through_July_2024.zip
  CCDB_Export_7_August_2024_through_October_2024.zip
  CCDB_Export_8_November_2024_through_December_2024.zip
  CCDB_Export_9_January_2025_through_February_2025.zip
  CCDB_Export_10_March_2025_through_April_2025.zip
  CCDB_Export_11_May_2025_through_June_2025.zip
  CCDB_Export_12_July_2025_through_August_2025.zip
  CCDB_Export_13_September_2025_through_October_2025.zip
  CCDB_Export_14_November_2025_through_December_2025.zip
  CCDB_Export_15_January_2026_through_February_2026.zip
  CCDB_Export_16_March_2026.zip
  CCDB_Export_17_April_2026.zip
  CCDB_Export_18_May_2026.zip
  CCDB_Export_19_June_2026.zip
  CCDB_Export_20_July_2026.zip
  CCDB_Export_21_August_2026.zip
)

for f in "${FILES[@]}"; do
  if [ -f "$ZIP_DIR/$f" ]; then
    echo "skip (already downloaded): $f"
    continue
  fi
  echo "downloading: $f"
  wget -q --show-progress -O "$ZIP_DIR/$f.part" "$BASE/$f"
  mv "$ZIP_DIR/$f.part" "$ZIP_DIR/$f"
done

echo
echo "Unzipping..."
for z in "$ZIP_DIR"/*.zip; do
  unzip -n -q "$z" -d "$OUT_DIR"
done

echo
echo "Done. Contents of $OUT_DIR:"
ls -lh "$OUT_DIR"
