"""
CampMinder CSV Parser
Based on Ari's campminder_matrix_optimized script
Processes enrollment data from CampMinder exports
"""

import pandas as pd
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union
from collections import defaultdict

# Define exact program order
PROGRAM_ORDER = [
    # ECA Camps
    'Infants', 'Toddler', 'PK2', 'PK3', 'PK4',
    # Variety Camps
    'Tsofim', "Tsofim Children's Trust",
    'Yeladim', "Yeladim Children's Trust",
    'Chaverim', "Chaverim Children's Trust",
    'Giborim', "Giborim Children's Trust",
    'Madli-Teen', "Madli-Teen Children's Trust",
    'Teen Travel', 'Teen Travel: Epic Trip to Orlando',
    # Sports Camps
    'Basketball', 'Flag Football', 'Soccer',
    'Sports Academy 1', 'Sports Academy 2',
    'Tennis Academy', 'Tennis Academy - Half Day',
    'Swim Academy', 'Tiny Tumblers Gymnastics',
    'Recreational Gymnastics', 'Competitive Gymnastics Team',
    'Volleyball', 'MMA Camp',
    # Performing Arts
    'Teeny Tiny Tnuah', 'Tiny Tnuah 1', 'Tiny Tnuah 2',
    'Tnuah 1', 'Tnuah 2', 'Extreme Tnuah',
    'Art Exploration', 'Music Camp', 'Theater Camp',
    # Teens
    'Madatzim 9th Grade', 'Madatzim 10th Grade',
    # Special Needs
    'OMETZ'
]

VALID_PROGRAMS = set(PROGRAM_ORDER)

# Category definitions
CATEGORIES = {
    'ECA Camps': {
        'color': '#FFB347',
        'color_light': '#FFF3E0',
        'emoji': '🌅',
        'programs': ['Infants', 'Toddler', 'PK2', 'PK3', 'PK4']
    },
    'Variety Camps': {
        'color': '#7CB342',
        'color_light': '#F1F8E9',
        'emoji': '⛺',
        'programs': [
            'Tsofim', "Tsofim Children's Trust",
            'Yeladim', "Yeladim Children's Trust",
            'Chaverim', "Chaverim Children's Trust",
            'Giborim', "Giborim Children's Trust",
            'Madli-Teen', "Madli-Teen Children's Trust",
            'Teen Travel', 'Teen Travel: Epic Trip to Orlando'
        ]
    },
    'Sports Camps': {
        'color': '#42A5F5',
        'color_light': '#E3F2FD',
        'emoji': '🏀',
        'programs': [
            'Basketball', 'Flag Football', 'Soccer',
            'Sports Academy 1', 'Sports Academy 2',
            'Tennis Academy', 'Tennis Academy - Half Day',
            'Swim Academy', 'Tiny Tumblers Gymnastics',
            'Recreational Gymnastics', 'Competitive Gymnastics Team',
            'Volleyball', 'MMA Camp'
        ]
    },
    'Performing Arts Camps': {
        'color': '#AB47BC',
        'color_light': '#F3E5F5',
        'emoji': '🎭',
        'programs': [
            'Teeny Tiny Tnuah', 'Tiny Tnuah 1', 'Tiny Tnuah 2',
            'Tnuah 1', 'Tnuah 2', 'Extreme Tnuah',
            'Art Exploration', 'Music Camp', 'Theater Camp'
        ]
    },
    'Teens Camps': {
        'color': '#5C6BC0',
        'color_light': '#E8EAF6',
        'emoji': '🎒',
        'programs': ['Madatzim 9th Grade', 'Madatzim 10th Grade']
    },
    'Special Needs Camps': {
        'color': '#FF7043',
        'color_light': '#FBE9E7',
        'emoji': '💙',
        'programs': ['OMETZ']
    }
}

# Program name mapping
PROGRAM_NAME_MAP = {
    'Teeny Tiny Tnuah': 'Teeny Tiny Tnuah',
    "Teeny Tiny T'nuah": 'Teeny Tiny Tnuah',
    'Tiny Tnuah 1': 'Tiny Tnuah 1',
    "Tiny T'nuah 1": 'Tiny Tnuah 1',
    'Tiny Tnuah 2': 'Tiny Tnuah 2',
    "Tiny T'nuah 2": 'Tiny Tnuah 2',
    'Tnuah 1': 'Tnuah 1',
    "T'nuah 1": 'Tnuah 1',
    'Tnuah 2': 'Tnuah 2',
    "T'nuah 2": 'Tnuah 2',
    'Extreme Tnuah': 'Extreme Tnuah',
    "Extreme T'nuah": 'Extreme Tnuah',
    'Ometz': 'OMETZ',
    'OMETZ': 'OMETZ',
}

# Goals by program (updated Feb 2026)
PROGRAM_GOALS = {
    'Infants': 6,
    'Toddler': 12,
    'PK2': 26,
    'PK3': 36,
    'PK4': 40,
    'Tsofim': 20,
    "Tsofim Children's Trust": 8,
    'Yeladim': 28,
    "Yeladim Children's Trust": 29,
    'Chaverim': 24,
    "Chaverim Children's Trust": 55,
    'Giborim': 10,
    "Giborim Children's Trust": 45,
    'Madli-Teen': 10,
    "Madli-Teen Children's Trust": 28,
    'Teen Travel': 15,
    'Teen Travel: Epic Trip to Orlando': 15,
    'Basketball': 20,
    'Flag Football': 12,
    'Soccer': 30,
    'Sports Academy 1': 15,
    'Sports Academy 2': 15,
    'Tennis Academy': 10,
    'Tennis Academy - Half Day': 10,
    'Swim Academy': 12,
    'Tiny Tumblers Gymnastics': 15,
    'Recreational Gymnastics': 30,
    'Competitive Gymnastics Team': 10,
    'Volleyball': 10,
    'MMA Camp': 30,
    'Teeny Tiny Tnuah': 10,
    'Tiny Tnuah 1': 15,
    'Tiny Tnuah 2': 15,
    'Tnuah 1': 12,
    'Tnuah 2': 12,
    'Extreme Tnuah': 10,
    'Art Exploration': 30,
    'Music Camp': 10,
    'Theater Camp': 20,
    'Madatzim 9th Grade': 10,
    'Madatzim 10th Grade': 10,
    'OMETZ': 0
}

# Programs to EXCLUDE from goal total
PROGRAMS_EXCLUDE_FROM_GOAL_TOTAL = {
    'MMA Camp', 'Infants', 'Toddler', 'PK2', 'PK3', 'PK4'
}

# Number of weeks each program runs (for FTE calculation)
PROGRAM_WEEKS = {
    'Infants': 9, 'Toddler': 9, 'PK2': 9, 'PK3': 9, 'PK4': 9,
    'Tsofim': 9, "Tsofim Children's Trust": 8,
    'Yeladim': 9, "Yeladim Children's Trust": 8,
    'Chaverim': 9, "Chaverim Children's Trust": 8,
    'Giborim': 9, "Giborim Children's Trust": 8,
    'Madli-Teen': 9, "Madli-Teen Children's Trust": 8,
    'Teen Travel': 7, 'Teen Travel: Epic Trip to Orlando': 1,
    'Basketball': 9, 'Flag Football': 9, 'Soccer': 9,
    'Sports Academy 1': 8, 'Sports Academy 2': 8,
    'Tennis Academy': 9, 'Tennis Academy - Half Day': 9,
    'Swim Academy': 8, 'Tiny Tumblers Gymnastics': 9,
    'Recreational Gymnastics': 9, 'Competitive Gymnastics Team': 9,
    'Volleyball': 9, 'MMA Camp': 9,
    'Teeny Tiny Tnuah': 4, 'Tiny Tnuah 1': 8, 'Tiny Tnuah 2': 8,
    'Tnuah 1': 8, 'Tnuah 2': 8, 'Extreme Tnuah': 8,
    'Art Exploration': 8, 'Music Camp': 1, 'Theater Camp': 8,
    'Madatzim 9th Grade': 9, 'Madatzim 10th Grade': 9,
    'OMETZ': 9
}


def normalize_program_name(program: str) -> Optional[str]:
    """Normalize program name to standard format"""
    if not program:
        return None
    program = program.strip()
    
    if program in PROGRAM_NAME_MAP:
        return PROGRAM_NAME_MAP[program]
    
    if program in VALID_PROGRAMS:
        return program
    
    return None


def parse_single_enrollment(enrollment_str: str) -> Union[Tuple[int, str], List[Tuple[int, str]], None]:
    """Parse a single enrollment string and return (week, program) or list of tuples"""
    enrollment_str = enrollment_str.strip()
    
    # Pattern 1: "Week X (1WK)/Program"
    match = re.match(r'Week\s+(\d+)\s*\(1WK\)\s*/\s*(.+)', enrollment_str, re.IGNORECASE)
    if match:
        week = int(match.group(1))
        program = normalize_program_name(match.group(2))
        if program and 1 <= week <= 9:
            return (week, program)
        return None
    
    # Pattern 2: "Program Weeks X-Y/Program"
    match = re.match(r'(.+)\s+Weeks\s+(\d+)-(\d+)\s*/\s*(.+)', enrollment_str, re.IGNORECASE)
    if match:
        program_name = match.group(4).strip()
        start_week = int(match.group(2))
        end_week = int(match.group(3))
        program = normalize_program_name(program_name)
        if program:
            results = []
            for week in range(start_week, min(end_week + 1, 10)):
                if 1 <= week <= 9:
                    results.append((week, program))
            return results if results else None
        return None
    
    # Pattern 3: "ECA Week X/Program"
    match = re.match(r'ECA\s+Week\s+(\d+)\s*/\s*(.+)', enrollment_str, re.IGNORECASE)
    if match:
        week = int(match.group(1))
        program = normalize_program_name(match.group(2))
        if program and 1 <= week <= 9:
            return (week, program)
        return None
    
    # Pattern 4: "Week X/Program" (without 1WK)
    match = re.match(r'Week\s+(\d+)\s*/\s*(.+)', enrollment_str, re.IGNORECASE)
    if match:
        week = int(match.group(1))
        program_name = match.group(2).strip()
        program = normalize_program_name(program_name)
        if program and 1 <= week <= 9:
            return (week, program)
        return None
    
    # Pattern 5: "Teeny Tiny Tnuah - Full Session/Teeny Tiny Tnuah"
    match = re.match(r'Teeny\s+Tiny\s+T[\'n]uah\s*-\s*Full\s+Session\s*/\s*Teeny\s+Tiny\s+T[\'n]uah', enrollment_str, re.IGNORECASE)
    if match:
        results = []
        for week in range(1, 5):
            results.append((week, 'Teeny Tiny Tnuah'))
        return results
    
    return None


def process_enrollment_string(enrollment_str: str) -> List[Tuple[int, str]]:
    """Process the 'Enrolled Sessions/Programs' column"""
    if pd.isna(enrollment_str) or not str(enrollment_str).strip():
        return []
    
    enrollment_str = str(enrollment_str)
    
    parts = []
    comma_parts = enrollment_str.split(',')
    for comma_part in comma_parts:
        and_parts = comma_part.split(' and ')
        parts.extend([p.strip() for p in and_parts if p.strip()])
    
    all_enrollments = []
    for part in parts:
        result = parse_single_enrollment(part)
        if result:
            if isinstance(result, list):
                all_enrollments.extend(result)
            else:
                all_enrollments.append(result)
    
    return all_enrollments


def process_applied_enrollments(sessions_str: str, programs_str: str) -> List[Tuple[int, str]]:
    """Process Applied Sessions and Applied Programs columns"""
    if pd.isna(sessions_str) or pd.isna(programs_str):
        return []
    
    sessions_str = str(sessions_str)
    programs_str = str(programs_str)
    
    if not sessions_str.strip() or sessions_str == 'nan':
        return []
    if not programs_str.strip() or programs_str == 'nan':
        return []
    
    sessions_parts = []
    for part in sessions_str.split(','):
        sessions_parts.extend([p.strip() for p in part.split(' and ') if p.strip()])
    
    programs_parts = []
    for part in programs_str.split(','):
        programs_parts.extend([p.strip() for p in part.split(' and ') if p.strip()])
    
    if len(sessions_parts) != len(programs_parts):
        min_len = min(len(sessions_parts), len(programs_parts))
        sessions_parts = sessions_parts[:min_len]
        programs_parts = programs_parts[:min_len]
    
    all_enrollments = []
    for session, program in zip(sessions_parts, programs_parts):
        normalized_program = normalize_program_name(program)
        if not normalized_program:
            continue
        
        week_match = re.search(r'Week\s+(\d+)', session, re.IGNORECASE)
        if week_match:
            week = int(week_match.group(1))
            if 1 <= week <= 9:
                all_enrollments.append((week, normalized_program))
    
    return all_enrollments


def parse_date(date_str) -> Optional[datetime]:
    """Parse enrollment effective date"""
    if pd.isna(date_str):
        return None
    
    date_str = str(date_str).strip()
    if not date_str or date_str == 'nan':
        return None
    
    # Try multiple date formats
    formats = [
        '%m/%d/%Y',
        '%Y-%m-%d',
        '%m-%d-%Y',
        '%d/%m/%Y',
        '%Y/%m/%d',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    # Try pandas parser as fallback
    try:
        return pd.to_datetime(date_str).to_pydatetime()
    except:
        return None


class CampMinderParser:
    """Parser for CampMinder CSV enrollment exports"""
    
    def __init__(self):
        self.categories = CATEGORIES
        self.goals = PROGRAM_GOALS
        self.program_weeks = PROGRAM_WEEKS
        self.excluded = PROGRAMS_EXCLUDE_FROM_GOAL_TOTAL
    
    def get_category_for_program(self, program_name: str) -> str:
        """Get the category name for a given program"""
        for cat_name, cat_info in self.categories.items():
            if program_name in cat_info['programs']:
                return cat_name
        return 'Other'
    
    def get_category_info(self, program_name: str) -> Dict:
        """Get full category info for a program"""
        cat_name = self.get_category_for_program(program_name)
        if cat_name in self.categories:
            return {
                'name': cat_name,
                **self.categories[cat_name]
            }
        return {
            'name': 'Other',
            'color': '#9E9E9E',
            'color_light': '#F5F5F5',
            'emoji': '📋',
            'programs': []
        }
    
    def parse_csv(self, filepath: str) -> Dict[str, Any]:
        """Parse a CampMinder CSV file and return structured report data"""
        
        # Read the CSV with proper encoding
        df = pd.read_csv(filepath, encoding='utf-8-sig', on_bad_lines='skip', engine='python')
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        print(f"Columns found: {list(df.columns)}")
        print(f"Total rows: {len(df)}")
        
        # Process all rows
        expanded_rows = []
        # Track participants by program and week
        participants_data = defaultdict(lambda: defaultdict(list))
        # Track registrations by date
        registrations_by_date = defaultdict(lambda: {
            'count': 0,
            'campers': set(),
            'camper_weeks': 0
        })
        
        for idx, row in df.iterrows():
            # Get enrollment date
            enrollment_date = parse_date(row.get('Enrollment Effective Date', ''))
            enrollment_date_str = enrollment_date.strftime('%Y-%m-%d') if enrollment_date else None
            
            # 1. Process "Enrolled Sessions/Programs" column
            enrolled_str = row.get('Enrolled Sessions/Programs', '')
            enrollments_from_enrolled = process_enrollment_string(enrolled_str)
            
            # 2. Process "Applied Sessions" + "Applied Programs" columns
            sessions_str = row.get('Applied Sessions', '')
            programs_str = row.get('Applied Programs', '')
            enrollments_from_applied = process_applied_enrollments(sessions_str, programs_str)
            
            # Combine both sources
            all_enrollments = enrollments_from_enrolled + enrollments_from_applied
            
            if not all_enrollments:
                continue
            
            first_name = str(row.get('First Name', '')).strip()
            last_name = str(row.get('Last Name', '')).strip()
            camper_id = f"{first_name}_{last_name}".lower().replace(' ', '_')
            
            # Track registration by date
            if enrollment_date_str:
                registrations_by_date[enrollment_date_str]['campers'].add(camper_id)
                registrations_by_date[enrollment_date_str]['camper_weeks'] += len(all_enrollments)
            
            for week, program in all_enrollments:
                expanded_rows.append({
                    'first_name': first_name,
                    'last_name': last_name,
                    'camper_id': camper_id,
                    'program': program,
                    'week': week,
                    'enrollment_date': enrollment_date_str
                })
                
                # Track participant for this program/week
                participants_data[program][week].append({
                    'first_name': first_name,
                    'last_name': last_name,
                    'camper_id': camper_id,
                    'enrollment_date': enrollment_date_str
                })
        
        print(f"Expanded to {len(expanded_rows)} enrollment records")
        
        if not expanded_rows:
            raise ValueError("No valid enrollment records found in CSV")
        
        enrollment_df = pd.DataFrame(expanded_rows)
        
        # Calculate date statistics
        date_stats = self._calculate_date_stats(registrations_by_date)
        
        # Convert participants_data to regular dict for JSON
        participants_dict = {}
        for program, weeks in participants_data.items():
            participants_dict[program] = {}
            for week, campers in weeks.items():
                # Remove duplicates while preserving order
                seen = set()
                unique_campers = []
                for c in campers:
                    if c['camper_id'] not in seen:
                        seen.add(c['camper_id'])
                        unique_campers.append(c)
                participants_dict[program][str(week)] = unique_campers
        
        result = self._calculate_stats(enrollment_df)
        result['participants'] = participants_dict
        result['date_stats'] = date_stats
        
        return result
    
    def _calculate_date_stats(self, registrations_by_date: Dict) -> Dict[str, Any]:
        """Calculate statistics by enrollment date"""
        
        # Convert to list sorted by date
        daily_data = []
        cumulative_campers = set()
        cumulative_weeks = 0
        
        for date_str in sorted(registrations_by_date.keys()):
            data = registrations_by_date[date_str]
            cumulative_campers.update(data['campers'])
            cumulative_weeks += data['camper_weeks']
            
            daily_data.append({
                'date': date_str,
                'new_registrations': len(data['campers']),
                'camper_weeks_added': data['camper_weeks'],
                'cumulative_campers': len(cumulative_campers),
                'cumulative_weeks': cumulative_weeks
            })
        
        # Group by week
        weekly_data = defaultdict(lambda: {'new_registrations': 0, 'camper_weeks': 0})
        for item in daily_data:
            try:
                date_obj = datetime.strptime(item['date'], '%Y-%m-%d')
                week_start = date_obj - pd.Timedelta(days=date_obj.weekday())
                week_key = week_start.strftime('%Y-%m-%d')
                weekly_data[week_key]['new_registrations'] += item['new_registrations']
                weekly_data[week_key]['camper_weeks'] += item['camper_weeks_added']
            except:
                continue
        
        weekly_list = [{'week_start': k, **v} for k, v in sorted(weekly_data.items())]
        
        # Group by month
        monthly_data = defaultdict(lambda: {'new_registrations': 0, 'camper_weeks': 0})
        for item in daily_data:
            try:
                month_key = item['date'][:7]  # YYYY-MM
                monthly_data[month_key]['new_registrations'] += item['new_registrations']
                monthly_data[month_key]['camper_weeks'] += item['camper_weeks_added']
            except:
                continue
        
        monthly_list = [{'month': k, **v} for k, v in sorted(monthly_data.items())]
        
        return {
            'daily': daily_data,
            'weekly': weekly_list,
            'monthly': monthly_list
        }
    
    def _calculate_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate all statistics from parsed data"""
        
        unique_campers = df['camper_id'].nunique()
        total_camper_weeks = len(df)
        
        programs_data = []
        
        for program in df['program'].unique():
            program_df = df[df['program'] == program]
            
            # Week distribution
            week_counts = {}
            for week in range(1, 10):
                week_counts[f'week_{week}'] = len(program_df[program_df['week'] == week])
            
            total = len(program_df)
            
            # FTE calculation
            weeks_offered = self.program_weeks.get(program, 9)
            fte = total / weeks_offered if weeks_offered > 0 else 0
            
            goal = self.goals.get(program, 0)
            percent_to_goal = (fte / goal * 100) if goal > 0 else 0
            
            cat_info = self.get_category_info(program)
            
            programs_data.append({
                'program': program,
                'category': cat_info['name'],
                'category_color': cat_info['color'],
                'category_color_light': cat_info['color_light'],
                'total': total,
                'fte': round(fte, 1),
                'goal': goal,
                'percent_to_goal': round(percent_to_goal, 1),
                **week_counts
            })
        
        # Sort by category order then program order
        category_order = ['ECA Camps', 'Variety Camps', 'Sports Camps', 'Performing Arts Camps', 'Teens Camps', 'Special Needs Camps', 'Other']
        program_order_map = {prog: idx for idx, prog in enumerate(PROGRAM_ORDER)}
        
        programs_data.sort(key=lambda x: (
            category_order.index(x['category']) if x['category'] in category_order else 99,
            program_order_map.get(x['program'], 999)
        ))
        
        # Calculate by category
        categories_data = []
        for cat_name, cat_info in self.categories.items():
            cat_programs = [p for p in programs_data if p['category'] == cat_name]
            
            if not cat_programs:
                continue
            
            total = sum(p['total'] for p in cat_programs)
            fte = sum(p['fte'] for p in cat_programs)
            goal = sum(p['goal'] for p in cat_programs)
            percent = (fte / goal * 100) if goal > 0 else 0
            
            if percent >= 70:
                status = 'success'
            elif percent >= 50:
                status = 'warning'
            else:
                status = 'danger'
            
            categories_data.append({
                'category': cat_name,
                'emoji': cat_info['emoji'],
                'color': cat_info['color'],
                'color_light': cat_info['color_light'],
                'total': total,
                'fte': round(fte, 1),
                'goal': goal,
                'percent_to_goal': round(percent, 1),
                'status': status,
                'programs': [p['program'] for p in cat_programs]
            })
        
        # Handle "Other" category
        other_programs = [p for p in programs_data if p['category'] == 'Other']
        if other_programs:
            total = sum(p['total'] for p in other_programs)
            fte = sum(p['fte'] for p in other_programs)
            goal = sum(p['goal'] for p in other_programs) or 1
            percent = (fte / goal * 100) if goal > 0 else 0
            
            categories_data.append({
                'category': 'Other',
                'emoji': '📋',
                'color': '#9E9E9E',
                'color_light': '#F5F5F5',
                'total': total,
                'fte': round(fte, 1),
                'goal': goal,
                'percent_to_goal': round(percent, 1),
                'status': 'warning',
                'programs': [p['program'] for p in other_programs]
            })
        
        # Calculate overall goal (excluding certain programs)
        total_goal = sum(
            g for p, g in self.goals.items() 
            if p not in self.excluded and g > 0
        )
        
        # Total FTE for all programs (excluding ECA)
        total_fte = sum(p['fte'] for p in programs_data if p['program'] not in self.excluded)
        
        summary = {
            'total_enrollment': unique_campers,
            'total_camper_weeks': total_camper_weeks,
            'total_fte': round(total_fte, 1),
            'goal': total_goal,
            'percent_to_goal': round(total_fte / total_goal * 100, 1) if total_goal > 0 else 0
        }
        
        return {
            'summary': summary,
            'programs': programs_data,
            'categories': categories_data,
            'raw_count': len(df)
        }


if __name__ == '__main__':
    import sys
    import json
    if len(sys.argv) > 1:
        parser = CampMinderParser()
        result = parser.parse_csv(sys.argv[1])
        print(f"\nSummary:")
        print(f"  Total Enrollment (unique campers): {result['summary']['total_enrollment']}")
        print(f"  Total Camper Weeks: {result['summary']['total_camper_weeks']}")
        print(f"  Total FTE: {result['summary']['total_fte']}")
        print(f"  Goal: {result['summary']['goal']}")
        print(f"  % to Goal: {result['summary']['percent_to_goal']}%")
