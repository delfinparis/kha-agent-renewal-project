"""
Fetch active agents from a Monday.com board, excluding terminated agents.

Usage:
    export MONDAY_API_TOKEN="your_token"
    python fetch_monday_agents.py
"""

import os
import sys
import json

import requests
import pandas as pd

from config import MONDAY_BOARD_ID, MONDAY_TERMINATED_GROUP_ID, ENTITY_NAME

MONDAY_API_URL = "https://api.monday.com/v2"
PAGE_LIMIT = 500


def _get_headers():
    token = os.environ.get("MONDAY_API_TOKEN")
    if not token:
        print("ERROR: Set MONDAY_API_TOKEN environment variable")
        sys.exit(1)
    return {
        "Authorization": token,
        "Content-Type": "application/json",
        "API-Version": "2024-10",
    }


def _run_query(query, variables=None):
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    resp = requests.post(MONDAY_API_URL, json=payload, headers=_get_headers(), timeout=120)
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data:
        print(f"Monday.com API errors: {json.dumps(data['errors'], indent=2)}")
        sys.exit(1)

    return data["data"]


def fetch_board_items(board_id):
    """Fetch all items from a board using cursor-based pagination."""
    first_query = """
    query ($boardId: [ID!]!, $limit: Int!) {
      boards(ids: $boardId) {
        name
        items_page(limit: $limit) {
          cursor
          items {
            id
            name
            group { id title }
            column_values { id text }
          }
        }
      }
    }
    """

    data = _run_query(first_query, {"boardId": [str(board_id)], "limit": PAGE_LIMIT})
    board = data["boards"][0]
    print(f"Board: {board['name']}")

    page = board["items_page"]
    all_items = list(page["items"])
    cursor = page["cursor"]

    next_query = """
    query ($limit: Int!, $cursor: String!) {
      next_items_page(limit: $limit, cursor: $cursor) {
        cursor
        items {
          id
          name
          group { id title }
          column_values { id text }
        }
      }
    }
    """

    page_num = 1
    while cursor is not None:
        page_num += 1
        data = _run_query(next_query, {"limit": PAGE_LIMIT, "cursor": cursor})
        next_page = data["next_items_page"]
        all_items.extend(next_page["items"])
        cursor = next_page["cursor"]

    print(f"  Total items: {len(all_items)}")
    return all_items


def extract_agent_info(item):
    """Extract agent first/last name from a Monday.com item."""
    col_lookup = {}
    for col in item["column_values"]:
        col_lookup[col["id"].lower()] = col["text"]

    first_name = None
    last_name = None

    for key in ["text95", "first name", "first_name", "firstname", "first"]:
        if key in col_lookup and col_lookup[key]:
            first_name = col_lookup[key].strip()
            break

    for key in ["text_19", "last name", "last_name", "lastname", "last"]:
        if key in col_lookup and col_lookup[key]:
            last_name = col_lookup[key].strip()
            break

    # Fallback: parse item name as "First Last"
    if not first_name and not last_name:
        parts = item["name"].strip().split(None, 1)
        first_name = parts[0] if len(parts) >= 1 else ""
        last_name = parts[1] if len(parts) >= 2 else ""

    # License number: "license_number" for KHA, "license_number3" for HC
    license_num = ""
    for key in ["license_number", "license_number3"]:
        if key in col_lookup and col_lookup[key]:
            license_num = col_lookup[key].strip()
            break

    return {
        "First Name": first_name or "",
        "Last Name": last_name or "",
        "License Number": license_num,
        "monday_item_id": item["id"],
        "monday_group": item["group"]["title"],
    }


def fetch_active_agents():
    """Fetch active (non-terminated) agents and return as a DataFrame."""
    items = fetch_board_items(MONDAY_BOARD_ID)

    active = [i for i in items if i["group"]["id"] != MONDAY_TERMINATED_GROUP_ID]
    terminated = len(items) - len(active)
    print(f"  Active: {len(active)}, Terminated (filtered out): {terminated}")

    agents = [extract_agent_info(item) for item in active]
    df = pd.DataFrame(agents)
    return df


if __name__ == "__main__":
    print(f"Fetching {ENTITY_NAME} agents from Monday.com...")
    df = fetch_active_agents()
    print(f"\n{len(df)} active agents:")
    for _, row in df.iterrows():
        print(f"  {row['First Name']} {row['Last Name']}")
