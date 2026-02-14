"""
Rebuild 2025 historical data from CampMinder API (season_id=2025).
Uses the same consolidation logic as rebuild_2025_data.py but reads from API instead of CSV.

This produces the same data structure in historical_enrollment.json:
  - summary
  - daily (cumulative enrollments by date)
  - programs (per-program week breakdowns)
  - enrollments_by_date (for date-filtered Old View Stats)
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from campminder_api import CampMinderAPIClient

JSON_PATH = os.path.join(os.path.dirname(__file__), 'data', 'historical_enrollment.json')

# =============================================================================
# SESSION NAME -> PROGRAM NAME MAPPING (for "Unknown" programs from API)
# When a session exists without a linked program, the API returns program_name="Unknown"
# We resolve these using the session name
# =============================================================================

SESSION_NAME_TO_PROGRAM = {
    # Theater Camp variants
    'M & M Performing Arts Week 6-9': 'Theater Camp',
    'Theater Camp Weeks 2-5': 'Theater Camp',
    # Teeny Tiny Tnuah variants
    "Teeny Tiny T'nuah Week 1-4": 'Teeny Tiny Tnuah',
    "Teeny Tiny T'nuah Weeks 1&2": 'Teeny Tiny Tnuah',
    "Teeny Tiny T'nuah Weeks 3&4": 'Teeny Tiny Tnuah',
    # Tiny Tumblers variants
    'Tiny Tumblers Gym Week 5-8': 'Tiny Tumblers Gymnastics',
    'Tiny Tumblers Gymnastics Weeks 1&2': 'Tiny Tumblers Gymnastics',
    'Tiny Tumblers Gymnastics Weeks 2&3': 'Tiny Tumblers Gymnastics',
    'Tiny Tumblers Gymnastics Weeks 3&4': 'Tiny Tumblers Gymnastics',
    'Tiny Tumblers Gymnastics Weeks 4&5': 'Tiny Tumblers Gymnastics',
    'Tiny Tumblers Gymnastics Weeks 5&6': 'Tiny Tumblers Gymnastics',
    'Tiny Tumblers Gymnastics Weeks 6&7': 'Tiny Tumblers Gymnastics',
    'Tiny Tumblers Gymnastics Weeks 7&8': 'Tiny Tumblers Gymnastics',
    'Tiny Tumblers Gymnastics Weeks 8&9': 'Tiny Tumblers Gymnastics',
    # Madatzim CIT
    'Madatzim CIT Week 1-4': 'Madatzim CIT',
    # Koach programs
    'Koach Chaverim Week 1-4': 'Koach Chaverim',
    'Koach Chaverim Week 5-8': 'Koach Chaverim',
    'Koach Giborim Week 5-8': 'Koach Giborim',
    'Koach Yeladim Week 5-8': 'Koach Yeladim',
    'Koach Madliteen Week 3': 'Koach Madli-Teen',
    'Koach Madliteen Week 4': 'Koach Madli-Teen',
    'Koach Madliteen Week 5-8': 'Koach Madli-Teen',
}

# =============================================================================
# PROGRAM NAME CONSOLIDATION (for API program names that need merging)
# =============================================================================

PROGRAM_NAME_MAP = {
    # Children's Trust - API returns "Children's Trust X", we want "X Children's Trust"
    # to match the name_map in dashboard.html
    "Children's Trust Chaverim": "Chaverim Children's Trust",
    "Children's Trust Giborim": "Giborim Children's Trust",
    "Children's Trust Madli-Teen": "Madli-Teen Children's Trust",
    "Children's Trust Tsofim": "Tsofim Children's Trust",
    "Children's Trust Yeladim": "Yeladim Children's Trust",
    # Koach Madliteen variants (API returns session-level names)
    'Koach Madliteen Week 3': 'Koach Madli-Teen',
    'Koach Madliteen Week 4': 'Koach Madli-Teen',
}


def canonicalize_program(name):
    """Map a program name to its canonical form."""
    if name in PROGRAM_NAME_MAP:
        return PROGRAM_NAME_MAP[name]
    return name


def resolve_unknown_program(session_name):
    """For 'Unknown' programs, determine the actual program from session name."""
    if session_name in SESSION_NAME_TO_PROGRAM:
        return SESSION_NAME_TO_PROGRAM[session_name]
    # If not in map, return the session name itself (will be caught later)
    return None


def main():
    print("=" * 60)
    print("Rebuilding 2025 data from CampMinder API")
    print("=" * 60)
    print()

    # =====================================================================
    # STEP 1: Fetch data from API
    # =====================================================================

    client = CampMinderAPIClient()
    print("Authenticating with CampMinder API...")
    if not client.authenticate():
        print("ERROR: Authentication failed!")
        return

    print(f"Authenticated. Client ID: {client.client_id}")
    print()

    print("Fetching enrollment report for season 2025...")
    raw_data = client.get_enrollment_report(2025)
    api_enrollments = raw_data['enrollments']
    print(f"  API returned {len(api_enrollments)} enrollment records")
    print(f"  Unique person IDs: {len(set(e['person_id'] for e in api_enrollments))}")
    print()

    # =====================================================================
    # STEP 2: Resolve Unknown programs and consolidate names
    # =====================================================================

    all_enrollments = []  # (person_id, week, canonical_program, date)
    all_camper_ids = set()
    unresolved = []

    for e in api_enrollments:
        person_id = e['person_id']
        week = e['week']
        program_name = e['program_name']
        session_name = e['session_name']
        enrollment_date = e.get('enrollment_date', '')[:10] if e.get('enrollment_date') else None

        # Skip invalid weeks
        if not (1 <= week <= 9):
            continue

        # Resolve "Unknown" programs using session name
        if program_name == 'Unknown':
            resolved = resolve_unknown_program(session_name)
            if resolved:
                program_name = resolved
            else:
                unresolved.append((person_id, session_name, week))
                continue

        # Canonicalize program name
        canonical = canonicalize_program(program_name)

        all_camper_ids.add(person_id)
        all_enrollments.append((person_id, week, canonical, enrollment_date))

    print(f"Processed {len(all_enrollments)} enrollment records from {len(all_camper_ids)} unique campers")
    if unresolved:
        from collections import Counter
        print(f"WARNING: {len(unresolved)} records could not be resolved:")
        session_counts = Counter(s for _, s, _ in unresolved)
        for name, count in sorted(session_counts.items()):
            print(f"  '{name}': {count} records")
    print()

    # =====================================================================
    # STEP 3: Build programs data
    # =====================================================================

    programs_data = defaultdict(lambda: {
        'weeks': defaultdict(set),  # week_num -> set of person_ids
    })

    for person_id, week, program, date in all_enrollments:
        programs_data[program]['weeks'][week].add(person_id)

    programs_list = []
    total_camper_weeks = 0

    for program_name in sorted(programs_data.keys()):
        data = programs_data[program_name]
        week_counts = {}
        total = 0
        for w in range(1, 10):
            count = len(data['weeks'].get(w, set()))
            week_counts[f'week_{w}'] = count
            total += count

        fte = round(total / 9, 2)
        total_camper_weeks += total

        prog_entry = {
            'program': program_name,
            **week_counts,
            'total': total,
            'fte': fte
        }
        programs_list.append(prog_entry)

    print(f"Built {len(programs_list)} programs:")
    for p in programs_list:
        print(f"  {p['program']}: total={p['total']}, fte={p['fte']}")
    print(f"\nTotal camper-weeks (sum of programs): {total_camper_weeks}")
    print()

    # =====================================================================
    # STEP 4: Build daily cumulative data
    # =====================================================================

    date_data = defaultdict(lambda: {'camper_ids': set(), 'week_count': 0})
    no_date_count = 0

    for person_id, week, program, date in all_enrollments:
        if date:
            date_data[date]['camper_ids'].add(person_id)
            date_data[date]['week_count'] += 1
        else:
            no_date_count += 1

    if no_date_count:
        print(f"Note: {no_date_count} enrollment records had no date")

    daily_list = []
    cumulative_campers = set()
    cumulative_weeks = 0

    for date in sorted(date_data.keys()):
        data = date_data[date]
        new_campers = data['camper_ids'] - cumulative_campers
        cumulative_campers.update(data['camper_ids'])
        cumulative_weeks += data['week_count']

        daily_list.append({
            'date': date,
            'new_registrations': len(new_campers),
            'camper_weeks_added': data['week_count'],
            'cumulative_campers': len(cumulative_campers),
            'cumulative_weeks': cumulative_weeks
        })

    print(f"Built {len(daily_list)} daily entries")
    if daily_list:
        print(f"  Date range: {daily_list[0]['date']} to {daily_list[-1]['date']}")
        print(f"  Final cumulative: {daily_list[-1]['cumulative_campers']} campers, {daily_list[-1]['cumulative_weeks']} weeks")
    print()

    # =====================================================================
    # STEP 5: Build enrollments_by_date (for date-filtered Old View Stats)
    # =====================================================================

    ebd = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    for person_id, week, program, date in all_enrollments:
        if date and 1 <= week <= 9:
            ebd[date][program][f'week_{week}'] += 1

    enrollments_by_date_list = []
    for dt in sorted(ebd.keys()):
        day_entry = {'date': dt, 'programs': {}}
        for prog_name, weeks_dict in sorted(ebd[dt].items()):
            day_entry['programs'][prog_name] = dict(weeks_dict)
        enrollments_by_date_list.append(day_entry)

    print(f"Built enrollments_by_date with {len(enrollments_by_date_list)} date entries")
    print()

    # =====================================================================
    # STEP 6: Build summary
    # =====================================================================

    summary = {
        'year': 2025,
        'total_campers': len(all_camper_ids),
        'total_camper_weeks': total_camper_weeks,
        'total_programs': len(programs_list)
    }

    print(f"Summary: {json.dumps(summary, indent=2)}")

    if daily_list:
        daily_final_weeks = daily_list[-1]['cumulative_weeks']
        if daily_final_weeks != total_camper_weeks:
            diff = total_camper_weeks - daily_final_weeks
            print(f"  Note: Daily cumulative ({daily_final_weeks}) differs from program total ({total_camper_weeks}) by {diff}")
            print(f"  This is because {no_date_count} enrollment records had no date")
    print()

    # =====================================================================
    # STEP 7: Update historical_enrollment.json
    # =====================================================================

    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        historical = json.load(f)

    # Replace 2025 section
    historical['2025'] = {
        'summary': summary,
        'daily': daily_list,
        'programs': programs_list,
        'enrollments_by_date': enrollments_by_date_list
    }

    with open(JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(historical, f, indent=2, ensure_ascii=False)

    print(f"SUCCESS: Updated {JSON_PATH}")
    print(f"  2025 section replaced with {len(programs_list)} programs, {len(daily_list)} daily entries, {len(enrollments_by_date_list)} enrollments_by_date entries")

    # Final verification
    print("\n=== VERIFICATION ===")
    print(f"  Summary total_camper_weeks: {summary['total_camper_weeks']}")
    print(f"  Sum of program totals: {sum(p['total'] for p in programs_list)}")
    if daily_list:
        print(f"  Daily final cumulative_weeks: {daily_list[-1]['cumulative_weeks']}")
        print(f"  Daily final cumulative_campers: {daily_list[-1]['cumulative_campers']}")
    print(f"  Summary total_campers: {summary['total_campers']}")


if __name__ == '__main__':
    main()
