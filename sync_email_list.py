"""
Sync Google Sheets email list with Monday.com agent roster.

Checks the Monday.com board for terminated agents and removes them
from the Google Sheet email list, so only active agents remain.

Requires:
    GOOGLE_SERVICE_ACCOUNT_JSON — env var containing the JSON key contents
    MONDAY_API_TOKEN — env var for Monday.com API access

Usage:
    export MONDAY_API_TOKEN="your_token"
    export GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
    python sync_email_list.py
    python sync_email_list.py --dry-run
"""

import argparse
import json
import os
import sys
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from config import (
    GOOGLE_SHEET_ID,
    MONDAY_BOARD_ID,
    MONDAY_TERMINATED_GROUP_ID,
    ENTITY_NAME,
)
from fetch_monday_agents import fetch_board_items


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_gspread_client():
    """Authenticate with Google Sheets using service account credentials."""
    creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not creds_json:
        print("ERROR: Set GOOGLE_SERVICE_ACCOUNT_JSON environment variable")
        sys.exit(1)

    creds_data = json.loads(creds_json)
    credentials = Credentials.from_service_account_info(creds_data, scopes=SCOPES)
    return gspread.authorize(credentials)


def normalize_for_comparison(name):
    """Simple name normalization for matching sheet rows to Monday.com."""
    return " ".join(name.lower().strip().split())


def get_terminated_agents():
    """Fetch agents in the terminated group from Monday.com."""
    items = fetch_board_items(MONDAY_BOARD_ID)
    terminated = [i for i in items if i["group"]["id"] == MONDAY_TERMINATED_GROUP_ID]

    names = []
    for item in terminated:
        # Try to get first/last from columns
        col_lookup = {}
        for col in item["column_values"]:
            col_lookup[col["title"].lower()] = col["text"]

        first = None
        last = None

        for key in ["first name", "first_name", "firstname", "first"]:
            if key in col_lookup and col_lookup[key]:
                first = col_lookup[key].strip()
                break

        for key in ["last name", "last_name", "lastname", "last"]:
            if key in col_lookup and col_lookup[key]:
                last = col_lookup[key].strip()
                break

        # Fallback: parse item name
        if not first and not last:
            parts = item["name"].strip().split(None, 1)
            first = parts[0] if len(parts) >= 1 else ""
            last = parts[1] if len(parts) >= 2 else ""

        names.append({
            "first": first or "",
            "last": last or "",
            "item_name": item["name"],
        })

    return names


def sync_sheet(dry_run=False):
    """Remove terminated agents from the Google Sheet email list."""
    print(f"[{ENTITY_NAME}] Email List Sync — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # Step 1: Get terminated agents from Monday.com
    print("\n--- Monday.com: Fetching terminated agents ---")
    terminated = get_terminated_agents()
    print(f"  Found {len(terminated)} terminated agent(s)")

    if not terminated:
        print("  No terminated agents found. Sheet unchanged.")
        return

    for agent in terminated:
        print(f"  - {agent['first']} {agent['last']}")

    # Build a set of normalized names for matching
    terminated_names = set()
    for agent in terminated:
        first_norm = normalize_for_comparison(agent["first"])
        last_norm = normalize_for_comparison(agent["last"])
        terminated_names.add((first_norm, last_norm))

    # Step 2: Open Google Sheet
    print(f"\n--- Google Sheets: Opening {ENTITY_NAME} email list ---")
    gc = get_gspread_client()
    sheet = gc.open_by_key(GOOGLE_SHEET_ID)
    worksheet = sheet.get_worksheet(0)

    all_rows = worksheet.get_all_records()
    print(f"  Total rows in sheet: {len(all_rows)}")

    # Step 3: Find rows to remove
    rows_to_remove = []
    for i, row in enumerate(all_rows):
        first = normalize_for_comparison(str(row.get("First Name", "")))
        last = normalize_for_comparison(str(row.get("Last Name", "")))

        if (first, last) in terminated_names:
            # Row index in the sheet (1-indexed header + 1-indexed data)
            sheet_row = i + 2  # +1 for header, +1 for 0-index
            rows_to_remove.append({
                "sheet_row": sheet_row,
                "first": row.get("First Name", ""),
                "last": row.get("Last Name", ""),
                "email": row.get("Email", ""),
            })

    print(f"  Matches to remove: {len(rows_to_remove)}")

    if not rows_to_remove:
        print("  No matching rows found in sheet. Nothing to remove.")
        return

    for match in rows_to_remove:
        print(f"  - Row {match['sheet_row']}: {match['first']} {match['last']} ({match['email']})")

    # Step 4: Remove rows (bottom-up to preserve row indices)
    if dry_run:
        print(f"\n*** DRY RUN — No changes made to Google Sheet ***")
    else:
        rows_to_remove.sort(key=lambda x: x["sheet_row"], reverse=True)
        for match in rows_to_remove:
            worksheet.delete_rows(match["sheet_row"])
            print(f"  Removed row {match['sheet_row']}: {match['first']} {match['last']}")

        print(f"\n  Removed {len(rows_to_remove)} agent(s) from the email list.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync email list with Monday.com")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without modifying sheet")
    args = parser.parse_args()

    sync_sheet(dry_run=args.dry_run)
