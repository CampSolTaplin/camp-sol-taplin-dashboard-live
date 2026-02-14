"""
One-time script to rebuild 2025 historical data from the Final Stats CSV export.
Replaces the incomplete 'programs' section in data/historical_enrollment.json
and rebuilds the 'daily' section from actual enrollment dates.

CSV: 2_14_2026_14_37_12.csv (Final Stats 2025 from CampMinder)
"""

import csv
import json
import re
import os
from collections import defaultdict
from datetime import datetime

# =====================================================================
# CONFIGURATION
# =====================================================================

CSV_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'downloads', '2_14_2026_14_37_12.csv')
# Try multiple possible paths
if not os.path.exists(CSV_PATH):
    CSV_PATH = r'C:\Users\arih\OneDrive - MARJCC\downloads\2_14_2026_14_37_12.csv'

JSON_PATH = os.path.join(os.path.dirname(__file__), 'data', 'historical_enrollment.json')

# Program name consolidation map: raw CSV name -> canonical name
PROGRAM_NAME_MAP = {
    # Theater Camp variants
    'M & M Performing Arts Week 6-9': 'Theater Camp',
    'Theater Camp Weeks 2-5': 'Theater Camp',
    # Teeny Tiny Tnuah variants
    "Teeny Tiny T'nuah Week 1-4": 'Teeny Tiny Tnuah',
    "Teeny Tiny T'nuah Weeks 1&2": 'Teeny Tiny Tnuah',
    "Teeny Tiny T'nuah Weeks 3&4": 'Teeny Tiny Tnuah',
    "Teeny Tiny Tnuah": 'Teeny Tiny Tnuah',
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
    'Tiny Tumblers Gymnastics': 'Tiny Tumblers Gymnastics',
    # Madatzim CIT variant
    'Madatzim CIT Week 1-4': 'Madatzim CIT',
    # Children's Trust - flip name order to match 2026 dashboard name_map
    "Children's Trust: Tsofim": "Tsofim Children's Trust",
    "Children's Trust: Yeladim": "Yeladim Children's Trust",
    "Children's Trust: Yeladim (Koach)": "Yeladim Children's Trust",
    "Children's Trust: Chaverim": "Chaverim Children's Trust",
    "Children's Trust: Chaverim (Koach)": "Chaverim Children's Trust",
    "Children's Trust: Giborim": "Giborim Children's Trust",
    "Children's Trust: Giborim (Koach)": "Giborim Children's Trust",
    "Children's Trust: Madli-Teen": "Madli-Teen Children's Trust",
    "Children's Trust: Madli-Teen (Koach)": "Madli-Teen Children's Trust",
    # Koach programs - keep separate
    'Koach Chaverim Week 1-4': 'Koach Chaverim',
    'Koach Chaverim Week 5-8': 'Koach Chaverim',
    'Koach Giborim Week 5-8': 'Koach Giborim',
    'Koach Yeladim Week 5-8': 'Koach Yeladim',
    'Koach Madliteen Week 3': 'Koach Madli-Teen',
    'Koach Madliteen Week 4': 'Koach Madli-Teen',
    'Koach Madliteen Week 5-8': 'Koach Madli-Teen',
}


def parse_week_range(text):
    """Extract week numbers from range-style text like 'Weeks 2-5', 'Week 5-8', 'Weeks 4&5'."""
    # Range: "Weeks 2-5" or "Week 5-8"
    m = re.search(r'[Ww]eeks?\s*(\d+)\s*[-–]\s*(\d+)', text)
    if m:
        return list(range(int(m.group(1)), int(m.group(2)) + 1))
    # Ampersand: "Weeks 4&5"
    m = re.search(r'[Ww]eeks?\s*(\d+)\s*&\s*(\d+)', text)
    if m:
        return [int(m.group(1)), int(m.group(2))]
    # Single: "Week 3"
    m = re.search(r'[Ww]eek\s*(\d+)', text)
    if m:
        return [int(m.group(1))]
    return []


def parse_single_enrollment(entry):
    """
    Parse a single enrollment string into a list of (week, program) tuples.

    Formats:
    - "Week 2 (1WK)/Recreational Gymnastics" -> [(2, 'Recreational Gymnastics')]
    - "ECA Week 2/PK3" -> [(2, 'PK3')]
    - "Theater Camp Weeks 2-5" -> [(2,'Theater Camp'),(3,'Theater Camp'),(4,'Theater Camp'),(5,'Theater Camp')]
    - "Tiny Tumblers Gymnastics Weeks 4&5" -> [(4,'Tiny Tumblers Gymnastics'),(5,'Tiny Tumblers Gymnastics')]
    - "Children's Trust: Giborim" -> [(1,'Children's Trust: Giborim'),...,(8,'Children's Trust: Giborim')]
    - "M & M Performing Arts Week 6-9" -> [(6,'M & M Performing Arts Week 6-9'),...,(9,...)]
    - "Koach Giborim Week 5-8" -> [(5,'Koach Giborim Week 5-8'),...,(8,...)]
    """
    entry = entry.strip()
    if not entry:
        return []

    # Pattern 1: "Week X (1WK)/Program" or "Week X/Program"
    m = re.match(r'[Ww]eek\s+(\d+)\s*(?:\(1WK\))?\s*/\s*(.+)', entry)
    if m:
        week = int(m.group(1))
        program = m.group(2).strip()
        return [(week, program)]

    # Pattern 2: "ECA Week X/Program"
    m = re.match(r'ECA\s+[Ww]eek\s+(\d+)\s*/\s*(.+)', entry)
    if m:
        week = int(m.group(1))
        program = m.group(2).strip()
        return [(week, program)]

    # Pattern 3: Multi-week program with "/" separator: "Program Weeks X-Y/Program"
    # Not common but handle it
    m = re.match(r'(.+?)\s+[Ww]eeks?\s+[\d&\-–]+\s*/\s*(.+)', entry)
    if m:
        weeks = parse_week_range(entry)
        program = m.group(2).strip()
        return [(w, program) for w in weeks]

    # Pattern 4: Multi-week without "/" separator: "Theater Camp Weeks 2-5", "Tiny Tumblers Gymnastics Weeks 4&5"
    # Also handles: "M & M Performing Arts Week 6-9", "Teeny Tiny T'nuah Week 1-4"
    weeks = parse_week_range(entry)
    if weeks:
        # The program name is the entry itself (will be mapped later via PROGRAM_NAME_MAP)
        return [(w, entry) for w in weeks]

    # Pattern 5: Children's Trust programs with no week info (full enrollment = weeks 1-8)
    if "children's trust" in entry.lower() or "koach" in entry.lower():
        # Check if weeks are embedded
        if not weeks:
            # Full CT enrollment = weeks 1-8
            return [(w, entry) for w in range(1, 9)]

    # Pattern 6: Program with no week (shouldn't happen often for enrolled)
    # If there's a "/" try to split
    if '/' in entry:
        parts = entry.split('/')
        program = parts[-1].strip()
        session = parts[0].strip()
        weeks = parse_week_range(session)
        if weeks:
            return [(w, program) for w in weeks]

    # Fallback: couldn't parse
    print(f"  WARNING: Could not parse enrollment: '{entry}'")
    return []


def parse_enrollment_string(enrolled_str):
    """
    Parse the full "Enrolled Sessions/Programs" string which may contain
    multiple enrollments separated by ", " and " and ".
    Returns list of (week, program) tuples.
    """
    if not enrolled_str or not enrolled_str.strip():
        return []

    results = []

    # Split on " and " first, then on ", " but be careful with program names containing commas
    # The pattern is: entries separated by ", " or " and "
    # But "M & M Performing Arts" contains " & " which is different from " and "

    # Strategy: Replace " and " with a delimiter, but not inside known program names
    # Simple approach: split by ", " first, then check for " and " within remaining parts

    # Replace the last " and " which typically joins the final enrollment
    # "Week 1/A, Week 2/B and Week 3/C" -> split correctly

    # Use regex to split on ", " and " and " but only when followed by a week/ECA/program pattern
    parts = re.split(r',\s+(?=[Ww]eek|ECA|Tiny|Teeny|Theater|Children|Koach|M &|Madatzim|LIT)', enrolled_str)

    expanded_parts = []
    for part in parts:
        # Further split on " and " when followed by week pattern
        subparts = re.split(r'\s+and\s+(?=[Ww]eek|ECA|Tiny|Teeny|Theater|Children|Koach|M &|Madatzim|LIT)', part)
        expanded_parts.extend(subparts)

    for part in expanded_parts:
        part = part.strip()
        if part:
            enrollments = parse_single_enrollment(part)
            results.extend(enrollments)

    return results


def parse_applied_sessions(sessions_str, programs_str):
    """
    Parse Applied Sessions + Applied Programs columns.
    Sessions: "Week 6 (1WK)" or "Week 2 (1WK), Week 3 (1WK) and Week 4 (1WK)"
    Programs: "Madli-Teen" or "LIT Volunteer Staff, LIT Volunteer Staff and LIT Volunteer Staff"
    """
    if not sessions_str or not sessions_str.strip():
        return []

    # Split sessions
    sessions_parts = re.split(r',\s*|\s+and\s+', sessions_str)
    programs_parts = re.split(r',\s*|\s+and\s+', programs_str) if programs_str else []

    results = []
    for i, sess in enumerate(sessions_parts):
        sess = sess.strip()
        if not sess:
            continue

        # Get week number
        weeks = parse_week_range(sess)
        if not weeks:
            m = re.search(r'\((\d+)WK\)', sess, re.IGNORECASE)
            if m:
                # It's a single week session but week number is in the name
                wm = re.search(r'[Ww]eek\s+(\d+)', sess)
                if wm:
                    weeks = [int(wm.group(1))]

        # Get program name
        program = ''
        if '/' in sess:
            program = sess.split('/')[-1].strip()
        elif i < len(programs_parts):
            program = programs_parts[i].strip()

        if weeks and program:
            for w in weeks:
                results.append((w, program))
        elif not weeks and program:
            # Children's Trust or Koach without week
            if "children's trust" in sess.lower() or "koach" in sess.lower():
                for w in range(1, 9):
                    results.append((w, sess.strip()))

    return results


def canonicalize_program(raw_name):
    """Map a raw program name to its canonical form."""
    # Check direct mapping first
    if raw_name in PROGRAM_NAME_MAP:
        return PROGRAM_NAME_MAP[raw_name]

    # Check case-insensitive
    for key, val in PROGRAM_NAME_MAP.items():
        if key.lower() == raw_name.lower():
            return val

    return raw_name


def parse_date(date_str):
    """Parse date from M/D/YYYY format to YYYY-MM-DD."""
    if not date_str or not date_str.strip():
        return None
    try:
        dt = datetime.strptime(date_str.strip(), '%m/%d/%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        try:
            dt = datetime.strptime(date_str.strip(), '%m/%d/%y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            return None


def main():
    print(f"Reading CSV: {CSV_PATH}")
    print(f"Output JSON: {JSON_PATH}")
    print()

    # =====================================================================
    # STEP 1: Parse CSV
    # =====================================================================

    all_enrollments = []  # List of (person_id, week, canonical_program, date)
    all_camper_ids = set()
    unparsed = []

    with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            person_id = row.get('PersonID', '').strip()
            date_str = row.get('Enrollment Effective Date', '').strip()
            enrolled_str = row.get('Enrolled Sessions/Programs', '').strip()
            applied_sessions = row.get('Applied Sessions', '').strip()
            applied_programs = row.get('Applied Programs', '').strip()

            if not person_id:
                continue

            date = parse_date(date_str)

            # Parse enrolled sessions first
            enrollments = parse_enrollment_string(enrolled_str)

            # If no enrolled sessions, try applied
            if not enrollments and (applied_sessions or applied_programs):
                enrollments = parse_applied_sessions(applied_sessions, applied_programs)

            if not enrollments:
                if enrolled_str or applied_sessions:
                    unparsed.append((person_id, enrolled_str or applied_sessions))
                continue

            all_camper_ids.add(person_id)

            for week, raw_program in enrollments:
                canonical = canonicalize_program(raw_program)
                if 1 <= week <= 9:
                    all_enrollments.append((person_id, week, canonical, date))

    print(f"Parsed {len(all_enrollments)} enrollment records from {len(all_camper_ids)} unique campers")
    if unparsed:
        print(f"WARNING: {len(unparsed)} rows could not be parsed:")
        for pid, text in unparsed[:10]:
            print(f"  PersonID {pid}: '{text[:80]}'")
    print()

    # =====================================================================
    # STEP 2: Build programs data
    # =====================================================================

    programs_data = defaultdict(lambda: {
        'weeks': defaultdict(set),  # week_num -> set of person_ids
        'total_enrollments': 0
    })

    for person_id, week, program, date in all_enrollments:
        programs_data[program]['weeks'][week].add(person_id)
        programs_data[program]['total_enrollments'] += 1

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
    # STEP 3: Build daily cumulative data
    # =====================================================================

    # Group enrollments by date
    date_data = defaultdict(lambda: {'camper_ids': set(), 'week_count': 0})
    no_date_count = 0

    for person_id, week, program, date in all_enrollments:
        if date:
            date_data[date]['camper_ids'].add(person_id)
            date_data[date]['week_count'] += 1
        else:
            no_date_count += 1

    if no_date_count:
        print(f"Note: {no_date_count} enrollment records had no date (applied but not enrolled)")

    # Build cumulative daily list
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
    # STEP 4: Build enrollments_by_date (for date-filtered Old View Stats)
    # =====================================================================
    # Structure: { "YYYY-MM-DD": { "program_name": { "week_1": count, ... } } }
    # This allows us to sum up to any cutoff date to get per-program week counts

    ebd = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    for person_id, week, program, date in all_enrollments:
        if date and 1 <= week <= 9:
            ebd[date][program][f'week_{week}'] += 1

    # Convert to serializable list format: [ { date, programs: { name: { week_1: n, ... } } } ]
    enrollments_by_date_list = []
    for dt in sorted(ebd.keys()):
        day_entry = {'date': dt, 'programs': {}}
        for prog_name, weeks_dict in sorted(ebd[dt].items()):
            day_entry['programs'][prog_name] = dict(weeks_dict)
        enrollments_by_date_list.append(day_entry)

    print(f"Built enrollments_by_date with {len(enrollments_by_date_list)} date entries")
    print()

    # =====================================================================
    # STEP 5: Build summary
    # =====================================================================

    summary = {
        'year': 2025,
        'total_campers': len(all_camper_ids),
        'total_camper_weeks': total_camper_weeks,
        'total_programs': len(programs_list)
    }

    print(f"Summary: {json.dumps(summary, indent=2)}")

    # Consistency check
    if daily_list:
        daily_final_weeks = daily_list[-1]['cumulative_weeks']
        if daily_final_weeks != total_camper_weeks:
            diff = total_camper_weeks - daily_final_weeks
            print(f"  Note: Daily cumulative ({daily_final_weeks}) differs from program total ({total_camper_weeks}) by {diff}")
            print(f"  This is because {no_date_count} enrollment records had no date")
    print()

    # =====================================================================
    # STEP 6: Update historical_enrollment.json
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
