#!/usr/bin/env python3
"""
DFPR License Renewal Tracker

Automated or manual script that:
1. Pulls active agents from Monday.com (or reads a local CSV)
2. Pulls DFPR license data from data.illinois.gov (or reads a local CSV)
3. Matches agents using fuzzy name matching
4. Removes agents who have already renewed (expiration after 4/30/2026)
5. Keeps agents who still need to renew or aren't found in the DFPR data

Usage:
    # Automated mode (pulls from Monday.com + data.illinois.gov):
    export MONDAY_API_TOKEN="your_token"
    python update_license_renewals.py --auto --dry-run
    python update_license_renewals.py --auto

    # Manual mode (local CSVs):
    python update_license_renewals.py --dfpr report.csv --agents agents.csv --dry-run
    python update_license_renewals.py --dfpr report.csv --agents agents.csv
"""

import argparse
import os
import re
import shutil
import sys
from datetime import date, datetime

from config import RENEWAL_CUTOFF, ENTITY_NAME

# Fuzzy match threshold (0-100)
DEFAULT_THRESHOLD = 85

# Default output CSV name for automated runs
AUTO_OUTPUT_CSV = "agents_need_renewal.csv"

# ===================================================================
# Name Matching
# ===================================================================

def normalize_name(name):
    """Normalize a name for comparison."""
    name = name.lower().strip()
    # Remove common suffixes
    for suffix in [' jr', ' sr', ' ii', ' iii', ' iv', ' md']:
        name = name.rstrip('.').replace(suffix, '')
    # Normalize punctuation and whitespace
    name = name.replace("'", "").replace("-", " ").replace(".", "").replace(",", " ")
    name = ' '.join(name.split())
    return name


def build_agents_csv_name_variants(row):
    """Generate multiple name variants from the agents CSV row for matching."""
    first = str(row.get('First Name', '')).strip()
    lasts = []
    for col in ['Last Name', 'Last Name 2', 'Last Name 3']:
        val = str(row.get(col, '')).strip()
        if val and val.lower() != 'nan' and val != '':
            lasts.append(val)

    variants = set()

    # Full name: "First Last Last2 Last3"
    full = ' '.join([first] + lasts)
    variants.add(normalize_name(full))

    if lasts:
        # "Last First" format
        variants.add(normalize_name(f"{' '.join(lasts)} {first}"))
        variants.add(normalize_name(f"{lasts[0]} {first}"))
        variants.add(normalize_name(f"{first} {lasts[0]}"))

    # With hyphenated last names
    if len(lasts) >= 2:
        variants.add(normalize_name(f"{first} {lasts[0]}-{lasts[1]}"))
        variants.add(normalize_name(f"{first} {lasts[0]} {lasts[1]}"))
        variants.add(normalize_name(f"{lasts[0]}-{lasts[1]} {first}"))
        variants.add(normalize_name(f"{first} {lasts[1]}"))

    if len(lasts) >= 3:
        variants.add(normalize_name(f"{first} {lasts[0]} {lasts[1]} {lasts[2]}"))
        variants.add(normalize_name(f"{first} {lasts[0]}-{lasts[1]}-{lasts[2]}"))

    variants.discard('')
    return variants


def match_dfpr_to_agents(dfpr_df, agents_df, threshold=DEFAULT_THRESHOLD):
    """Match DFPR records to agents CSV using fuzzy name matching."""
    from rapidfuzz import fuzz, process

    # Build agents CSV lookup: {normalized_variant: csv_index}
    agents_name_map = {}
    for idx, row in agents_df.iterrows():
        variants = build_agents_csv_name_variants(row)
        for variant in variants:
            if variant:
                agents_name_map[variant] = idx

    agents_variant_list = list(agents_name_map.keys())

    matches = []
    unmatched_dfpr = []
    matched_agents_indices = set()

    for _, dfpr_row in dfpr_df.iterrows():
        dfpr_name_raw = str(dfpr_row.get('Supverisee', '')).strip()
        if not dfpr_name_raw or dfpr_name_raw.lower() == 'nan':
            continue

        dfpr_name = normalize_name(dfpr_name_raw)
        if not dfpr_name:
            continue

        exp_text = str(dfpr_row.get('Expiration Date', '')).strip()

        # Pass 1: Exact match
        if dfpr_name in agents_name_map:
            csv_idx = agents_name_map[dfpr_name]
            if csv_idx not in matched_agents_indices:
                matches.append({
                    'dfpr_name': dfpr_name_raw,
                    'exp_date': exp_text,
                    'csv_index': csv_idx,
                    'score': 100,
                    'match_type': 'exact'
                })
                matched_agents_indices.add(csv_idx)
                continue

        # Pass 2: Fuzzy match
        result = process.extractOne(
            dfpr_name,
            agents_variant_list,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold
        )

        if result:
            matched_variant, score, _ = result
            csv_idx = agents_name_map[matched_variant]
            if csv_idx not in matched_agents_indices:
                matches.append({
                    'dfpr_name': dfpr_name_raw,
                    'exp_date': exp_text,
                    'csv_index': csv_idx,
                    'score': int(score),
                    'match_type': 'fuzzy'
                })
                matched_agents_indices.add(csv_idx)
            else:
                unmatched_dfpr.append({'name': dfpr_name_raw, 'exp': exp_text, 'reason': 'duplicate match'})
        else:
            unmatched_dfpr.append({'name': dfpr_name_raw, 'exp': exp_text, 'reason': 'no match'})

    unmatched_agents_indices = set(agents_df.index) - matched_agents_indices
    return matches, unmatched_dfpr, unmatched_agents_indices


# ===================================================================
# Date Parsing
# ===================================================================

def parse_expiration_date(raw_text):
    """Parse expiration date from DFPR CSV (M/D/YY or M/D/YYYY format)."""
    text = str(raw_text).strip()
    if not text or text.lower() == 'nan':
        return None

    for fmt in ['%m/%d/%y', '%m/%d/%Y', '%m-%d-%y', '%m-%d-%Y']:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    return None


# ===================================================================
# Business Logic
# ===================================================================

def determine_action(match):
    """Determine whether to keep or remove an agent."""
    exp_date = parse_expiration_date(match['exp_date'])

    if exp_date is None:
        return 'keep', f'unparseable date: "{match["exp_date"]}"'

    if exp_date <= RENEWAL_CUTOFF:
        return 'keep', f'expires {exp_date.strftime("%m/%d/%Y")} (needs renewal)'
    else:
        return 'remove', f'expires {exp_date.strftime("%m/%d/%Y")} (already renewed)'


# ===================================================================
# CSV Operations
# ===================================================================

def load_csv(csv_path):
    """Load a CSV with encoding detection."""
    import pandas as pd
    for encoding in ['utf-8', 'latin-1', 'cp1252']:
        try:
            df = pd.read_csv(csv_path, encoding=encoding)
            return df
        except (UnicodeDecodeError, Exception):
            continue
    print(f"ERROR: Could not read CSV: {csv_path}")
    sys.exit(1)


def backup_csv(csv_path):
    """Create timestamped backup of original CSV."""
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    backup_path = f"{csv_path}.bak.{timestamp}"
    shutil.copy2(csv_path, backup_path)
    return backup_path


# ===================================================================
# Main Pipeline
# ===================================================================

def run_pipeline(dfpr_path=None, agents_path=None, output_path=None,
                  dry_run=False, threshold=DEFAULT_THRESHOLD,
                  dfpr_df=None, agents_df=None, source_label="manual"):
    """Main processing pipeline. Accepts CSVs paths or pre-loaded DataFrames."""
    import pandas as pd
    from collections import Counter

    print("=" * 60)
    print(f"[{ENTITY_NAME}] License Renewal Update — {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 60)
    print(f"Source:     {source_label}")
    print(f"Cutoff:     {RENEWAL_CUTOFF.strftime('%m/%d/%Y')}")
    print(f"Threshold:  {threshold}")
    print(f"Dry run:    {dry_run}")
    print()

    # --- Step 1: Load data ---
    if dfpr_df is None:
        print(f"Loading DFPR CSV: {dfpr_path}")
        dfpr_df = load_csv(dfpr_path)
    if agents_df is None:
        print(f"Loading Agents CSV: {agents_path}")
        agents_df = load_csv(agents_path)
    print(f"  DFPR records:  {len(dfpr_df)}")
    print(f"  Agents in CSV: {len(agents_df)}")

    # --- Step 2: Show DFPR expiration date distribution ---
    exp_counts = Counter()
    for _, row in dfpr_df.iterrows():
        exp = parse_expiration_date(row.get('Expiration Date', ''))
        if exp:
            exp_counts[exp.strftime('%m/%d/%Y')] += 1

    print(f"\nDFPR expiration date distribution:")
    for exp_str, count in sorted(exp_counts.items()):
        exp = parse_expiration_date(exp_str)
        status = "KEEP (needs renewal)" if exp and exp <= RENEWAL_CUTOFF else "REMOVE (renewed)"
        print(f"  {exp_str}: {count:4d} agents → {status}")

    # --- Step 3: Match DFPR to agents CSV ---
    print(f"\nMatching {len(dfpr_df)} DFPR records to {len(agents_df)} agents...")
    matches, unmatched_dfpr, unmatched_agents = match_dfpr_to_agents(
        dfpr_df, agents_df, threshold=threshold
    )

    exact = sum(1 for m in matches if m['match_type'] == 'exact')
    fuzzy = sum(1 for m in matches if m['match_type'] == 'fuzzy')
    fuzzy_scores = [m['score'] for m in matches if m['match_type'] == 'fuzzy']
    avg_fuzzy = sum(fuzzy_scores) / len(fuzzy_scores) if fuzzy_scores else 0

    print(f"  Exact matches:        {exact}")
    print(f"  Fuzzy matches:        {fuzzy}  (avg score: {avg_fuzzy:.1f})")
    print(f"  Unmatched DFPR:       {len(unmatched_dfpr)}")
    print(f"  Unmatched agents CSV: {len(unmatched_agents)} (will be kept)")

    # --- Step 4: Determine keep/remove ---
    indices_to_remove = set()
    keep_reasons = []
    remove_list = []

    for match in matches:
        action, reason = determine_action(match)
        csv_idx = match['csv_index']
        csv_row = agents_df.loc[csv_idx]
        name = f"{csv_row.get('First Name', '')} {csv_row.get('Last Name', '')}".strip()

        if action == 'remove':
            indices_to_remove.add(csv_idx)
            remove_list.append({
                'name': name,
                'reason': reason,
                'score': match['score'],
                'match_type': match['match_type'],
                'dfpr_name': match['dfpr_name']
            })
        else:
            keep_reasons.append({'name': name, 'reason': reason})

    # --- Step 5: Print summary ---
    print(f"\n{'=' * 60}")
    print("RESULTS")
    print(f"{'=' * 60}")
    print(f"REMOVE (already renewed):     {len(indices_to_remove)}")
    print(f"KEEP (needs renewal):         {len(keep_reasons)}")
    print(f"KEEP (no match in DFPR):      {len(unmatched_agents)}")
    print(f"Total in updated CSV:         {len(agents_df) - len(indices_to_remove)}")

    # Show removal list
    if remove_list:
        print(f"\n--- Agents to Remove ({len(remove_list)}) ---")
        remove_list.sort(key=lambda x: x['name'])
        for item in remove_list:
            score_str = f"{item['score']:3d} ({item['match_type']})"
            print(f"  {item['name']:35s} | {item['reason']} | Score: {score_str}")
            if item['match_type'] == 'fuzzy':
                print(f"    DFPR name: {item['dfpr_name']}")

    # Show unmatched DFPR agents that have renewed (might need manual review)
    renewed_unmatched = [a for a in unmatched_dfpr
                         if parse_expiration_date(a['exp']) and parse_expiration_date(a['exp']) > RENEWAL_CUTOFF]
    if renewed_unmatched:
        print(f"\n--- Unmatched DFPR Agents Who Renewed ({len(renewed_unmatched)}) — manual review ---")
        for agent in renewed_unmatched:
            print(f"  {agent['name']:35s} | Exp: {agent['exp']} | {agent['reason']}")

    # --- Step 6: Build report text ---
    report_lines = []
    report_lines.append(f"[{ENTITY_NAME}] License Renewal Report — {datetime.now().strftime('%Y-%m-%d')}")
    report_lines.append(f"Source: {source_label}")
    report_lines.append(f"Cutoff: {RENEWAL_CUTOFF.strftime('%m/%d/%Y')}")
    report_lines.append("")
    report_lines.append(f"REMOVE (already renewed):     {len(indices_to_remove)}")
    report_lines.append(f"KEEP (needs renewal):         {len(keep_reasons)}")
    report_lines.append(f"KEEP (no match in DFPR):      {len(unmatched_agents)}")
    report_lines.append(f"Total needing renewal:        {len(agents_df) - len(indices_to_remove)}")

    if remove_list:
        report_lines.append(f"\n--- Agents Who Renewed ({len(remove_list)}) ---")
        for item in sorted(remove_list, key=lambda x: x['name']):
            report_lines.append(f"  {item['name']:35s} | {item['reason']}")

    if keep_reasons:
        report_lines.append(f"\n--- Agents Still Needing Renewal ({len(keep_reasons)}) ---")
        for item in sorted(keep_reasons, key=lambda x: x['name']):
            report_lines.append(f"  {item['name']:35s} | {item['reason']}")

    if renewed_unmatched:
        report_lines.append(f"\n--- Unmatched DFPR (Manual Review) ({len(renewed_unmatched)}) ---")
        for agent in renewed_unmatched:
            report_lines.append(f"  {agent['name']:35s} | Exp: {agent['exp']}")

    report_text = "\n".join(report_lines)

    # --- Step 7: Save updated CSV ---
    if dry_run:
        print(f"\n*** DRY RUN — No changes made ***")
    else:
        updated_df = agents_df.drop(index=list(indices_to_remove)).reset_index(drop=True)
        save_path = output_path or agents_path or AUTO_OUTPUT_CSV

        if agents_path and os.path.exists(agents_path) and indices_to_remove:
            backup_path = backup_csv(agents_path)
            print(f"\nBackup saved: {backup_path}")

        updated_df.to_csv(save_path, index=False)
        print(f"Updated CSV saved: {save_path}")
        print(f"Agents needing renewal: {len(updated_df)}")

    return report_text

# ===================================================================
# CLI
# ===================================================================

def run_auto(dry_run=False, threshold=DEFAULT_THRESHOLD):
    """Automated pipeline: pull from Monday.com + data.illinois.gov."""
    from fetch_monday_agents import fetch_active_agents
    from fetch_dfpr_data import fetch_dfpr_records

    print(f"[{ENTITY_NAME}] Automated renewal check starting...\n")

    # Step 1: Get active agents from Monday.com
    print("--- Monday.com ---")
    agents_df = fetch_active_agents()

    # Step 2: Get DFPR license data
    print("\n--- DFPR (data.illinois.gov) ---")
    dfpr_df = fetch_dfpr_records()

    if dfpr_df.empty:
        print("No DFPR records found. Exiting.")
        return

    # Step 3: Run the matching pipeline
    report_text = run_pipeline(
        dfpr_df=dfpr_df,
        agents_df=agents_df,
        output_path=AUTO_OUTPUT_CSV,
        dry_run=dry_run,
        threshold=threshold,
        source_label="Monday.com + data.illinois.gov",
    )

    print("\n" + report_text)

    # Step 4: Save report for Apps Script to email
    if not dry_run:
        with open("latest_report.txt", "w") as f:
            f.write(report_text)
        print(f"Report saved to latest_report.txt")


def main():
    parser = argparse.ArgumentParser(
        description='License renewal tracker — automated or manual',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Automated (Monday.com + DFPR API):
  python update_license_renewals.py --auto --dry-run
  python update_license_renewals.py --auto

  # Manual (local CSVs):
  python update_license_renewals.py --dfpr dfpr_export.csv --agents agents.csv --dry-run
  python update_license_renewals.py --dfpr dfpr_export.csv --agents agents.csv
        """
    )

    parser.add_argument('--auto', action='store_true',
                        help='Automated mode: pull agents from Monday.com, DFPR from data.illinois.gov')
    parser.add_argument('--dfpr', default=None, help='Path to DFPR eLicense CSV export (manual mode)')
    parser.add_argument('--agents', default=None, help='Path to agents CSV (manual mode)')
    parser.add_argument('--output', default=None, help='Output CSV path (default: overwrite agents CSV)')
    parser.add_argument('--dry-run', action='store_true', help='Show changes without modifying CSV')
    parser.add_argument('--threshold', type=int, default=DEFAULT_THRESHOLD,
                        help=f'Fuzzy match threshold 0-100 (default: {DEFAULT_THRESHOLD})')

    args = parser.parse_args()

    if args.auto:
        run_auto(
            dry_run=args.dry_run,
            threshold=args.threshold,
        )
    else:
        if not args.dfpr or not args.agents:
            parser.error("Manual mode requires --dfpr and --agents (or use --auto)")

        if not os.path.exists(args.dfpr):
            print(f"ERROR: DFPR CSV not found: {args.dfpr}")
            sys.exit(1)

        if not os.path.exists(args.agents):
            print(f"ERROR: Agents CSV not found: {args.agents}")
            sys.exit(1)

        run_pipeline(
            dfpr_path=args.dfpr,
            agents_path=args.agents,
            output_path=args.output,
            dry_run=args.dry_run,
            threshold=args.threshold,
        )


if __name__ == '__main__':
    main()
