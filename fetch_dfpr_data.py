"""
Fetch DFPR license data from the Illinois Open Data Portal (data.illinois.gov).

Uses the SODA API to query the Professional Licensing dataset (pzzh-kp68),
looking up agents by their license numbers for exact matching.

Usage:
    python fetch_dfpr_data.py
"""

import io
import sys

import requests
import pandas as pd

from config import DFPR_DATASET_ID, ENTITY_NAME

SODA_BASE_URL = f"https://data.illinois.gov/resource/{DFPR_DATASET_ID}.csv"
BATCH_SIZE = 100  # SODA IN clause limit


def fetch_dfpr_by_license_numbers(license_numbers):
    """
    Query DFPR by a list of license numbers. Much faster and more accurate
    than pulling all 600K+ records and fuzzy matching by name.
    Returns a DataFrame with DFPR license data for the matched numbers.
    """
    # Filter out empty/invalid license numbers
    valid_numbers = [n.strip() for n in license_numbers if n and str(n).strip()]
    if not valid_numbers:
        print("  WARNING: No valid license numbers provided.")
        return pd.DataFrame()

    print(f"  Looking up {len(valid_numbers)} license numbers in DFPR...")

    all_rows = []

    # Query in batches (SODA has limits on IN clause size)
    for i in range(0, len(valid_numbers), BATCH_SIZE):
        batch = valid_numbers[i:i + BATCH_SIZE]
        in_list = ", ".join(f"'{n}'" for n in batch)
        where_clause = f"license_number IN ({in_list})"

        params = {
            "$where": where_clause,
            "$select": "first_name,middle,last_name,license_number,license_status,"
                       "expiration_date,business_name,businessdba,description",
            "$limit": 50000,
        }

        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(valid_numbers) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} license numbers)...")

        resp = requests.get(SODA_BASE_URL, params=params, timeout=120)
        resp.raise_for_status()

        chunk = pd.read_csv(io.StringIO(resp.text))
        if not chunk.empty:
            all_rows.append(chunk)

    if not all_rows:
        print("  WARNING: No DFPR records found for the given license numbers.")
        return pd.DataFrame()

    df = pd.concat(all_rows, ignore_index=True)

    # Rename columns to match the format the existing matching logic expects
    df["Supverisee"] = df.apply(
        lambda r: f"{r.get('last_name', '')} {r.get('first_name', '')}".strip(),
        axis=1,
    )
    df["Expiration Date"] = df["expiration_date"]

    return df


# Keep the old broad fetch as a fallback
def fetch_dfpr_records():
    """Fetch all IL Real Estate licensees (fallback for agents without license numbers)."""
    where_clause = (
        "license_type = 'REAL ESTATE' "
        "AND business = 'N' "
        "AND first_name IS NOT NULL AND first_name != ''"
    )

    all_rows = []
    offset = 0

    while True:
        params = {
            "$where": where_clause,
            "$select": "first_name,middle,last_name,license_number,license_status,"
                       "expiration_date,business_name,businessdba,description",
            "$limit": 50000,
            "$offset": offset,
            "$order": "last_name,first_name",
        }

        print(f"  Querying data.illinois.gov (offset={offset})...")
        resp = requests.get(SODA_BASE_URL, params=params, timeout=120)
        resp.raise_for_status()

        chunk = pd.read_csv(io.StringIO(resp.text))

        if chunk.empty:
            break

        all_rows.append(chunk)
        if len(chunk) < 50000:
            break
        offset += 50000

    if not all_rows:
        print("  WARNING: No DFPR records found for the given filters.")
        return pd.DataFrame()

    df = pd.concat(all_rows, ignore_index=True)

    df["Supverisee"] = df.apply(
        lambda r: f"{r.get('last_name', '')} {r.get('first_name', '')}".strip(),
        axis=1,
    )
    df["Expiration Date"] = df["expiration_date"]

    return df


if __name__ == "__main__":
    # Quick test with a few known license numbers
    test_numbers = ["475209491"]  # Desiree Phipps from earlier test
    print(f"Testing DFPR lookup for {ENTITY_NAME} with {len(test_numbers)} license number(s)...")
    df = fetch_dfpr_by_license_numbers(test_numbers)
    print(f"\n{len(df)} DFPR records found:")
    for _, row in df.iterrows():
        print(f"  {row['Supverisee']:40s} | Lic#: {row['license_number']} | Exp: {row['Expiration Date']} | {row.get('license_status', '')}")
