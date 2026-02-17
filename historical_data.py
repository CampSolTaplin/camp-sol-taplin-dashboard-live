"""
Historical Data Manager for Camp Sol Taplin
Manages enrollment data from 2024 and 2025 for comparisons
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

DATA_FILE = 'data/historical_enrollment.json'

class HistoricalDataManager:
    """Manages historical enrollment data for year-over-year comparisons"""
    
    def __init__(self):
        self.data = self._load_data()
    
    def _load_data(self) -> Dict:
        """Load historical data from JSON file"""
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading historical data: {e}")
        return {}
    
    def get_year_data(self, year: int) -> Optional[Dict]:
        """Get all data for a specific year"""
        return self.data.get(str(year))
    
    def get_enrollment_as_of_date(self, year: int, month: int, day: int) -> Optional[Dict]:
        """Get enrollment totals as of a specific date for a given year"""
        year_data = self.get_year_data(year)
        if not year_data or 'daily' not in year_data:
            return None
        
        target_date = f"{year}-{month:02d}-{day:02d}"
        
        # Find the closest date <= target_date
        daily = year_data['daily']
        result = None
        
        for day_data in daily:
            if day_data['date'] <= target_date:
                result = {
                    'date': day_data['date'],
                    'total_enrollment': day_data['cumulative_campers'],
                    'total_camper_weeks': day_data['cumulative_weeks']
                }
            else:
                break
        
        return result
    
    def get_daily_data(self, year: int, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get daily registration data, optionally filtered by date range"""
        year_data = self.get_year_data(year)
        if not year_data or 'daily' not in year_data:
            return []
        
        daily = year_data['daily']
        
        if start_date:
            daily = [d for d in daily if d['date'] >= start_date]
        if end_date:
            daily = [d for d in daily if d['date'] <= end_date]
        
        return daily
    
    def get_comparison_data(self, current_year: int = 2026, current_daily: List = None) -> Dict:
        """Generate comprehensive comparison data between years"""

        years_to_compare = [2024, 2025]
        comparison = {
            'years': {},
            'milestones': [],
            'growth_rates': {}
        }

        for year in years_to_compare:
            year_data = self.get_year_data(year)
            if year_data:
                comparison['years'][year] = {
                    'summary': year_data.get('summary', {}),
                    'daily': year_data.get('daily', [])
                }

        # Calculate growth rate 2024 -> 2025
        if '2024' in self.data and '2025' in self.data:
            campers_2024 = self.data['2024']['summary']['total_campers']
            campers_2025 = self.data['2025']['summary']['total_campers']
            weeks_2024 = self.data['2024']['summary']['total_camper_weeks']
            weeks_2025 = self.data['2025']['summary']['total_camper_weeks']

            comparison['growth_rates'] = {
                'campers_growth': round((campers_2025 - campers_2024) / campers_2024 * 100, 1),
                'weeks_growth': round((weeks_2025 - weeks_2024) / weeks_2024 * 100, 1)
            }

            # Key milestones (including 2026 if data available)
            comparison['milestones'] = self._calculate_milestones(current_daily)

        return comparison

    def _calculate_milestones(self, current_daily: List = None) -> List[Dict]:
        """Calculate when each year hit certain enrollment milestones"""
        milestones_to_track = [100, 250, 500, 750, 1000]
        results = []

        for milestone in milestones_to_track:
            milestone_data = {'milestone': milestone}

            for year in ['2024', '2025']:
                if year in self.data and 'daily' in self.data[year]:
                    for day in self.data[year]['daily']:
                        if day['cumulative_campers'] >= milestone:
                            milestone_data[f'year_{year}'] = {
                                'date': day['date'],
                                'days_from_start': self._days_from_year_start(day['date'])
                            }
                            break

            # Check 2026 current data
            if current_daily:
                for day in current_daily:
                    if day.get('cumulative_campers', 0) >= milestone:
                        milestone_data['year_2026'] = {
                            'date': day['date'],
                            'days_from_start': self._days_from_year_start(day['date'])
                        }
                        break

            if len(milestone_data) > 1:  # Has at least one year's data
                results.append(milestone_data)

        return results
    
    def _days_from_year_start(self, date_str: str) -> int:
        """Calculate days from January 1st of that year"""
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            year_start = datetime(dt.year, 1, 1)
            return (dt - year_start).days
        except:
            return 0
    
    def get_pace_comparison(self, current_data: Dict, as_of_date: str = None) -> Dict:
        """
        Compare current year pace against historical years
        Returns how current year compares to same point in previous years
        """
        if not as_of_date:
            as_of_date = datetime.now().strftime('%Y-%m-%d')
        
        # Get current month/day
        current_dt = datetime.strptime(as_of_date, '%Y-%m-%d')
        month, day = current_dt.month, current_dt.day
        
        comparison = {
            'as_of_date': as_of_date,
            'current': {
                'campers': current_data.get('summary', {}).get('total_enrollment', 0),
                'camper_weeks': current_data.get('summary', {}).get('total_camper_weeks', 0)
            },
            'vs_2025': None,
            'vs_2024': None
        }
        
        # Compare to 2025 same date
        data_2025 = self.get_enrollment_as_of_date(2025, month, day)
        if data_2025:
            comparison['vs_2025'] = {
                'campers_then': data_2025['total_enrollment'],
                'campers_diff': comparison['current']['campers'] - data_2025['total_enrollment'],
                'campers_pct': round((comparison['current']['campers'] / data_2025['total_enrollment'] - 1) * 100, 1) if data_2025['total_enrollment'] > 0 else 0,
                'weeks_then': data_2025['total_camper_weeks'],
                'weeks_diff': comparison['current']['camper_weeks'] - data_2025['total_camper_weeks'],
                'weeks_pct': round((comparison['current']['camper_weeks'] / data_2025['total_camper_weeks'] - 1) * 100, 1) if data_2025['total_camper_weeks'] > 0 else 0
            }
        
        # Compare to 2024 same date
        data_2024 = self.get_enrollment_as_of_date(2024, month, day)
        if data_2024:
            comparison['vs_2024'] = {
                'campers_then': data_2024['total_enrollment'],
                'campers_diff': comparison['current']['campers'] - data_2024['total_enrollment'],
                'campers_pct': round((comparison['current']['campers'] / data_2024['total_enrollment'] - 1) * 100, 1) if data_2024['total_enrollment'] > 0 else 0,
                'weeks_then': data_2024['total_camper_weeks'],
                'weeks_diff': comparison['current']['camper_weeks'] - data_2024['total_camper_weeks'],
                'weeks_pct': round((comparison['current']['camper_weeks'] / data_2024['total_camper_weeks'] - 1) * 100, 1) if data_2024['total_camper_weeks'] > 0 else 0
            }
        
        return comparison
    
    def get_weekly_comparison_chart_data(self) -> Dict:
        """
        Get data formatted for a multi-year comparison chart
        Uses day/month format for labels (ignoring year for comparison)
        """
        from datetime import date, timedelta
        
        chart_data = {
            'labels': [],  # Day/Month format (e.g., "Jan 15", "Feb 7")
            'days': [],    # Days from Jan 1 (for internal use)
            '2024': [],
            '2025': [],
            '2026': []  # Will be filled with current data if available
        }
        
        # Get today's day of year to limit 2026 data
        today = date.today()
        today_day_of_year = (today - date(today.year, 1, 1)).days
        
        # Get max days we have data for
        max_days = 0
        
        for year in ['2024', '2025']:
            if year in self.data and 'daily' in self.data[year]:
                for day in self.data[year]['daily']:
                    days = self._days_from_year_start(day['date'])
                    max_days = max(max_days, days)
        
        # Create aligned data points with date labels
        for days_offset in range(0, max_days + 1, 7):  # Weekly intervals
            # Create date label (using 2024 as reference year, only showing day/month)
            ref_date = date(2024, 1, 1) + timedelta(days=days_offset)
            label = ref_date.strftime('%b %d')  # e.g., "Jan 15", "Feb 07"
            
            chart_data['labels'].append(label)
            chart_data['days'].append(days_offset)
            
            for year in ['2024', '2025']:
                if year in self.data and 'daily' in self.data[year]:
                    # Find cumulative at this point
                    cumulative = 0
                    for day in self.data[year]['daily']:
                        if self._days_from_year_start(day['date']) <= days_offset:
                            cumulative = day['cumulative_weeks']
                        else:
                            break
                    chart_data[year].append(cumulative)
                else:
                    chart_data[year].append(0)
            
            # For 2026, only include data up to today
            # (Will be filled by frontend with current data)
            if days_offset <= today_day_of_year:
                chart_data['2026'].append(None)  # Placeholder, filled by frontend
            else:
                chart_data['2026'].append(None)
        
        # Add today's day of year for frontend to know where to cut 2026 line
        chart_data['today_day_of_year'] = today_day_of_year
        
        return chart_data
    
    def get_childrens_trust_stats(self, year: int) -> Dict:
        """
        Calculate Children's Trust statistics for a given historical year.
        Returns total camper-weeks and estimated unique campers from CT programs.
        """
        year_data = self.get_year_data(year)
        if not year_data or 'programs' not in year_data:
            return {'camper_weeks': 0, 'unique_campers': 0}

        programs = year_data['programs']
        ct_total_weeks = 0
        ct_unique_campers = 0

        # CT program name patterns
        ct_keywords = ["children's trust", "childrens trust"]

        if isinstance(programs, list):
            # 2024 format: list of dicts with 'program' key
            for prog in programs:
                if not isinstance(prog, dict):
                    continue
                prog_name = (prog.get('program') or prog.get('name', '')).lower()
                if any(kw in prog_name for kw in ct_keywords):
                    ct_total_weeks += prog.get('total', 0)
                    # Estimate unique campers: max weekly enrollment
                    max_weekly = max(prog.get(f'week_{w}', 0) for w in range(1, 10))
                    ct_unique_campers += max_weekly
        elif isinstance(programs, dict):
            # 2025 format: dict with program name as key
            for prog_name, prog_data in programs.items():
                if any(kw in prog_name.lower() for kw in ct_keywords):
                    if isinstance(prog_data, dict):
                        ct_total_weeks += prog_data.get('total', 0)
                        max_weekly = max(prog_data.get(f'week_{w}', 0) for w in range(1, 10))
                        ct_unique_campers += max_weekly

        return {
            'camper_weeks': ct_total_weeks,
            'unique_campers': ct_unique_campers
        }

    def get_ct_daily_data(self, year: int) -> List[Dict]:
        """
        Build cumulative Children's Trust unique campers per date using enrollments_by_date.
        Returns list of {date, ct_campers} entries — one per date where CT data changes.
        Uses same logic as get_childrens_trust_stats: unique campers = sum of max weekly
        enrollment per CT program.
        """
        year_data = self.get_year_data(year)
        if not year_data or 'enrollments_by_date' not in year_data:
            return []

        ct_keywords = ["children's trust", "childrens trust"]
        from collections import defaultdict

        # Accumulate CT program weekly counts progressively across dates
        ct_program_weeks = defaultdict(lambda: defaultdict(int))
        result = []

        for day_entry in year_data['enrollments_by_date']:
            date_str = day_entry['date']
            changed = False

            for prog_name, weeks_dict in day_entry.get('programs', {}).items():
                if any(kw in prog_name.lower() for kw in ct_keywords):
                    for week_key, count in weeks_dict.items():
                        ct_program_weeks[prog_name][week_key] += count
                        changed = True

            if changed or not result:
                # Compute CT unique campers: sum of max weekly enrollment per CT program
                ct_unique = 0
                for prog_name, weeks in ct_program_weeks.items():
                    max_weekly = max((weeks.get(f'week_{w}', 0) for w in range(1, 10)), default=0)
                    ct_unique += max_weekly
                result.append({'date': date_str, 'ct_campers': ct_unique})

        return result

    def get_programs_as_of_date(self, year: int, month: int, day: int) -> List[Dict]:
        """
        Get program-level enrollment data for a given year, filtered to only include
        enrollments that occurred on or before the given month/day of that year.

        Uses the 'enrollments_by_date' section which stores per-date, per-program, per-week counts.
        Returns a list of program dicts in the same format as 'programs' section.
        """
        year_data = self.get_year_data(year)
        if not year_data or 'enrollments_by_date' not in year_data:
            # Fallback: return full programs data if no date-level data available
            return year_data.get('programs', []) if year_data else []

        target_date = f"{year}-{month:02d}-{day:02d}"

        # Accumulate per-program, per-week counts up to target_date
        from collections import defaultdict
        program_weeks = defaultdict(lambda: defaultdict(int))

        for day_entry in year_data['enrollments_by_date']:
            if day_entry['date'] > target_date:
                break  # dates are sorted, stop early
            for prog_name, weeks_dict in day_entry.get('programs', {}).items():
                for week_key, count in weeks_dict.items():
                    program_weeks[prog_name][week_key] += count

        # Build programs list in same format as 'programs' section
        programs_list = []
        for prog_name in sorted(program_weeks.keys()):
            weeks = program_weeks[prog_name]
            total = sum(weeks.get(f'week_{w}', 0) for w in range(1, 10))
            fte = round(total / 9, 2)
            entry = {
                'program': prog_name,
                'week_1': weeks.get('week_1', 0),
                'week_2': weeks.get('week_2', 0),
                'week_3': weeks.get('week_3', 0),
                'week_4': weeks.get('week_4', 0),
                'week_5': weeks.get('week_5', 0),
                'week_6': weeks.get('week_6', 0),
                'week_7': weeks.get('week_7', 0),
                'week_8': weeks.get('week_8', 0),
                'week_9': weeks.get('week_9', 0),
                'total': total,
                'fte': fte
            }
            programs_list.append(entry)

        return programs_list

    def get_program_data(self, year: int, program_name: str) -> Optional[Dict]:
        """
        Get enrollment data for a specific program in a specific year
        
        Returns week-by-week enrollment for the program
        """
        year_data = self.get_year_data(year)
        if not year_data or 'programs' not in year_data:
            return None
        
        programs = year_data['programs']
        
        # Handle case where programs might be in different formats
        if not programs:
            return None
        
        # Program name mapping for year-over-year comparison
        # Maps 2026 names to their 2025 equivalents
        name_mapping_2025 = {
            'MMA Camp': 'Karate',
            'Madatzim 9th Grade': 'Madatzim CIT',
            'Madatzim 10th Grade': 'Madatzim CIT',  # May need adjustment
        }
        
        # Get the name to search for
        search_name = program_name
        if year == 2025 and program_name in name_mapping_2025:
            search_name = name_mapping_2025[program_name]
        
        # Find the program
        for prog in programs:
            # Skip if prog is not a dict (defensive check)
            if not isinstance(prog, dict):
                continue
            
            prog_name = prog.get('program') or prog.get('name', '')
            if prog_name == search_name or prog_name == program_name:
                result = {
                    'program': program_name,
                    'year': year,
                    'week_1': prog.get('week_1', 0),
                    'week_2': prog.get('week_2', 0),
                    'week_3': prog.get('week_3', 0),
                    'week_4': prog.get('week_4', 0),
                    'week_5': prog.get('week_5', 0),
                    'week_6': prog.get('week_6', 0),
                    'week_7': prog.get('week_7', 0),
                    'week_8': prog.get('week_8', 0),
                    'week_9': prog.get('week_9', 0),
                    'total': prog.get('total', 0),
                    'fte': prog.get('fte', 0)
                }
                
                # Special handling for Theater Camp - extend weeks
                if prog_name == 'Theater Camp' and year == 2025:
                    # Week 2 data extends to weeks 3, 4, 5
                    week_2_val = result['week_2']
                    result['week_3'] = week_2_val
                    result['week_4'] = week_2_val
                    result['week_5'] = week_2_val
                    # Week 6 data extends to weeks 7, 8, 9
                    week_6_val = result['week_6']
                    result['week_7'] = week_6_val
                    result['week_8'] = week_6_val
                    result['week_9'] = week_6_val
                    # Recalculate total
                    result['total'] = sum([result[f'week_{i}'] for i in range(1, 10)])
                    result['fte'] = round(result['total'] / 9, 2)
                
                return result
        
        return None
