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
    
    def get_comparison_data(self, current_year: int = 2026) -> Dict:
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
            
            # Key milestones
            comparison['milestones'] = self._calculate_milestones()
        
        return comparison
    
    def _calculate_milestones(self) -> List[Dict]:
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
        Aligns years by days from Jan 1 for fair comparison
        """
        chart_data = {
            'labels': [],  # Days from Jan 1
            '2024': [],
            '2025': [],
            '2026': []  # Will be filled with current data if available
        }
        
        # Get max days we have data for
        max_days = 0
        
        for year in ['2024', '2025']:
            if year in self.data and 'daily' in self.data[year]:
                for day in self.data[year]['daily']:
                    days = self._days_from_year_start(day['date'])
                    max_days = max(max_days, days)
        
        # Create aligned data points
        for days_offset in range(0, max_days + 1, 7):  # Weekly intervals
            chart_data['labels'].append(days_offset)
            
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
        
        return chart_data
    
    def get_program_data(self, year: int, program_name: str) -> Optional[Dict]:
        """
        Get enrollment data for a specific program in a specific year
        
        Returns week-by-week enrollment for the program
        """
        year_data = self.get_year_data(year)
        if not year_data or 'programs' not in year_data:
            return None
        
        # Find the program
        for prog in year_data['programs']:
            if prog.get('program') == program_name or prog.get('name') == program_name:
                return {
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
        
        return None
