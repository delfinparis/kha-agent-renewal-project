"""
Fetch DFPR license data from the Illinois Open Data Portal (data.illinois.gov).

Uses the SODA API to query the Professional Licensing dataset (pzzh-kp68),
filtering by business DBA name and real estate license types.

Usage:
    python fetch_dfpr_data.py
"""

import io
import sys

import requests
import pandas as pd

from config import DFPR_DATASET_ID, DFPR_BUSINESS_DBA, ENTITY_NAME

SODA_BASE_URL = f"https://data.illinois.gov/resource/{DFPR_DATASET_ID}.csv"
PAGE_SIZE = 50000


def fetch_dfpr_records():
    """
    Query DFPR data filtered by business DBA and real estate license type.
    Returns a DataFrame with columns matching the existing script's expectations.
    """
    where_clause = (
        f"businessdba LIKE '%{DFPR_BUSINESS_DBA}%' "
        f"AND description LIKE '%Real Estate%'"
    )

    all_rows = []
    offset = 0

    while True:
        params = {
            "$where": where_clause,
            "$select": "first_name,middle,last_name,license_number,license_status,"
                       "expiration_date,business_name,businessdba,description",
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": "last_name,first_name",
        }

        print(f"  Querying data.illinois.gov (offset={offset})...")
        resp = requests.get(SODA_BASE_URL, params=params, timeout=60)
        resp.raise_for_status()

        chunk = pd.read_csv(io.StringIO(resp.text))

        if chunk.empty:
            break

        all_rows.append(chunk)
        if len(chunk) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    if not all_rows:
        print("  WARNING: No DFPR records found for the given filters.")
        return pd.DataFrame()

    df = pd.concat(all_rows, ignore_index=True)

    # Rename columns to match the format the existing matching logic expects
    # The existing script reads a "Supverisee" column (DFPR eLicense export format)
    # and an "Expiration Date" column
    df["Supverisee"] = df.apply(
        lambda r: f"{r.get('last_name', '')} {r.get('first_name', '')}".strip(),
        axis=1,
    )
    df["Expiration Date"] = df["expiration_date"]

    return df


if __name__ == "__main__":
    print(f"Fetching DFPR data for {ENTITY_NAME} (DBA: {DFPR_BUSINESS_DBA})...")
    df = fetch_dfpr_records()
    print(f"\n{len(df)} DFPR records found:")
    for _, row in df.head(20).iterrows():
        print(f"  {row['Supverisee']:40s} | Exp: {row['Expiration Date']} | {row.get('license_status', '')}")
    if len(df) > 20:
        print(f"  ... and {len(df) - 20} more")
