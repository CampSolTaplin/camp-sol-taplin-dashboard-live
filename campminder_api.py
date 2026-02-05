"""
CampMinder API Client
Handles authentication and data fetching from CampMinder API
"""

import requests
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CampMinderAPIClient:
    """Client for interacting with CampMinder API"""
    
    BASE_URL = "https://api.campminder.com"
    
    def __init__(self, api_key: str = None, subscription_key: str = None):
        """
        Initialize the API client
        
        Args:
            api_key: CampMinder API Key (or set CAMPMINDER_API_KEY env var)
            subscription_key: Azure subscription key (or set CAMPMINDER_SUBSCRIPTION_KEY env var)
        """
        self.api_key = api_key or os.environ.get('CAMPMINDER_API_KEY')
        self.subscription_key = subscription_key or os.environ.get('CAMPMINDER_SUBSCRIPTION_KEY')
        
        if not self.api_key or not self.subscription_key:
            raise ValueError("API Key and Subscription Key are required")
        
        self.jwt_token = None
        self.jwt_expires_at = None
        self.client_ids = []
        self.client_id = None  # Primary client ID
        
        # Cache for API responses
        self._cache = {}
        self._cache_ttl = 300  # 5 minutes
    
    def _get_headers(self, include_auth: bool = True) -> Dict[str, str]:
        """Get headers for API requests"""
        headers = {
            "Content-Type": "application/json",
            "Ocp-Apim-Subscription-Key": self.subscription_key
        }
        
        if include_auth and self.jwt_token:
            headers["Authorization"] = f"Bearer {self.jwt_token}"
        
        return headers
    
    def authenticate(self) -> bool:
        """
        Authenticate with CampMinder API and get JWT token
        
        Returns:
            True if authentication successful
        """
        try:
            url = f"{self.BASE_URL}/auth/apikey"
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Ocp-Apim-Subscription-Key": self.subscription_key
            }
            
            logger.info("Authenticating with CampMinder API...")
            logger.info(f"URL: {url}")
            logger.info(f"API Key (first 20 chars): {self.api_key[:20] if self.api_key else 'None'}...")
            logger.info(f"Subscription Key (first 10 chars): {self.subscription_key[:10] if self.subscription_key else 'None'}...")
            
            response = requests.get(url, headers=headers, timeout=30)
            
            logger.info(f"Response Status: {response.status_code}")
            logger.info(f"Response Headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                data = response.json()
                self.jwt_token = data.get('Token')
                self.jwt_expires_at = datetime.now() + timedelta(hours=1)
                
                # Parse client IDs
                client_ids_str = data.get('ClientIDs', '')
                if client_ids_str:
                    self.client_ids = [int(cid.strip()) for cid in client_ids_str.split(',') if cid.strip()]
                    self.client_id = self.client_ids[0] if self.client_ids else None
                
                logger.info(f"Authentication successful! ClientIDs: {self.client_ids}")
                return True
            else:
                logger.error(f"Authentication failed: {response.status_code}")
                logger.error(f"Response body: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _ensure_authenticated(self):
        """Ensure we have a valid JWT token"""
        if not self.jwt_token or (self.jwt_expires_at and datetime.now() >= self.jwt_expires_at):
            if not self.authenticate():
                raise Exception("Failed to authenticate with CampMinder API")
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """
        Make authenticated request to API
        
        Args:
            endpoint: API endpoint (e.g., '/sessions')
            params: Query parameters
            
        Returns:
            JSON response or None if error
        """
        self._ensure_authenticated()
        
        url = f"{self.BASE_URL}{endpoint}"
        headers = self._get_headers()
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"API request failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"API request error: {e}")
            return None
    
    def _paginated_request(self, endpoint: str, params: Dict = None, max_pages: int = 100) -> List[Dict]:
        """
        Make paginated request and collect all results
        
        Args:
            endpoint: API endpoint
            params: Base query parameters
            max_pages: Maximum pages to fetch
            
        Returns:
            List of all results
        """
        all_results = []
        params = params or {}
        params['pagenumber'] = 1
        params['pagesize'] = 1000  # Max page size
        
        for page in range(1, max_pages + 1):
            params['pagenumber'] = page
            
            data = self._make_request(endpoint, params)
            if not data:
                break
            
            results = data.get('Results', [])
            all_results.extend(results)
            
            # Check if there are more pages
            total_count = data.get('TotalCount', 0)
            if len(all_results) >= total_count or not results:
                break
            
            logger.info(f"Fetched page {page}, got {len(results)} results ({len(all_results)}/{total_count} total)")
        
        return all_results
    
    # ==================== SESSION ENDPOINTS ====================
    
    def get_sessions(self, season_id: int, client_id: int = None) -> List[Dict]:
        """
        Get all sessions for a season
        
        Args:
            season_id: Season year (e.g., 2026)
            client_id: Client ID (uses default if not provided)
            
        Returns:
            List of session objects
        """
        client_id = client_id or self.client_id
        
        params = {
            'clientid': client_id,
            'seasonid': season_id
        }
        
        return self._paginated_request('/sessions', params)
    
    def get_programs(self, season_id: int, client_id: int = None) -> List[Dict]:
        """
        Get all programs for a season
        
        Args:
            season_id: Season year (e.g., 2026)
            client_id: Client ID (uses default if not provided)
            
        Returns:
            List of program objects
        """
        client_id = client_id or self.client_id
        
        params = {
            'clientid': client_id,
            'seasonid': season_id
        }
        
        return self._paginated_request('/sessions/programs', params)
    
    def get_session_groups(self, season_id: int, client_id: int = None) -> List[Dict]:
        """
        Get session groups (categories)
        
        Args:
            season_id: Season year
            client_id: Client ID
            
        Returns:
            List of group objects
        """
        client_id = client_id or self.client_id
        
        params = {
            'clientid': client_id,
            'seasonid': season_id
        }
        
        return self._paginated_request('/sessions/groups', params)
    
    def get_attendees(self, season_id: int, client_id: int = None, 
                      status: int = 2, session_ids: List[int] = None,
                      program_ids: List[int] = None) -> List[Dict]:
        """
        Get all attendees for a season
        
        Args:
            season_id: Season year (e.g., 2026)
            client_id: Client ID
            status: Status filter (2=Enrolled, 4=Applied, 6=Enrolled+Applied)
            session_ids: Optional list of session IDs to filter
            program_ids: Optional list of program IDs to filter
            
        Returns:
            List of attendee objects with session/program status
        """
        client_id = client_id or self.client_id
        
        params = {
            'clientid': client_id,
            'seasonid': season_id,
            'status': status  # 2=Enrolled, 4=Applied, 6=Both
        }
        
        if session_ids:
            params['sessionids'] = session_ids
        if program_ids:
            params['programids'] = program_ids
        
        return self._paginated_request('/sessions/attendees', params)
    
    # ==================== PERSON ENDPOINTS ====================
    
    def get_person(self, person_id: int, client_id: int = None) -> Optional[Dict]:
        """
        Get a single person by ID
        
        Args:
            person_id: Person ID
            client_id: Client ID
            
        Returns:
            Person object or None
        """
        client_id = client_id or self.client_id
        
        params = {'clientid': client_id}
        return self._make_request(f'/persons/{person_id}', params)
    
    def get_persons_batch(self, person_ids: List[int], client_id: int = None) -> Dict[int, Dict]:
        """
        Get multiple persons by ID
        
        Args:
            person_ids: List of person IDs
            client_id: Client ID
            
        Returns:
            Dict mapping person_id to person data
        """
        results = {}
        
        for pid in person_ids:
            person = self.get_person(pid, client_id)
            if person:
                results[pid] = person
        
        return results
    
    # ==================== ENROLLMENT DATA ====================
    
    def get_enrollment_report(self, season_id: int, client_id: int = None) -> Dict:
        """
        Get comprehensive enrollment report for dashboard
        
        This combines data from sessions, programs, and attendees to create
        a report similar to the CSV export.
        
        Args:
            season_id: Season year (e.g., 2026)
            client_id: Client ID
            
        Returns:
            Dict with enrollment data structured for dashboard
        """
        client_id = client_id or self.client_id
        
        logger.info(f"Fetching enrollment report for season {season_id}, client {client_id}")
        
        # Fetch all required data
        sessions = self.get_sessions(season_id, client_id)
        programs = self.get_programs(season_id, client_id)
        attendees = self.get_attendees(season_id, client_id, status=6)  # Enrolled + Applied
        
        logger.info(f"Fetched: {len(sessions)} sessions, {len(programs)} programs, {len(attendees)} attendees")
        
        # Build lookup maps
        session_map = {s['ID']: s for s in sessions}
        program_map = {p['ID']: p for p in programs}
        
        # Build session to weeks mapping (handling multi-week sessions)
        session_to_weeks = {}
        for session in sessions:
            name = session.get('Name', '')
            week_info = self._extract_week_info(name, session.get('SortOrder', 0))
            session_to_weeks[session['ID']] = week_info['weeks']
            logger.debug(f"Session '{name}' -> Weeks {week_info['weeks']}")
        
        # Process attendees into enrollment data
        # For multi-week sessions, create one enrollment record per week
        enrollments = []
        
        for attendee in attendees:
            person_id = attendee.get('PersonID')
            
            for sps in attendee.get('SessionProgramStatus', []):
                session_id = sps.get('SessionID')
                program_id = sps.get('ProgramID')
                status_id = sps.get('StatusID')
                status_name = sps.get('StatusName', '')
                effective_date = sps.get('EffectiveDate', '')
                post_date = sps.get('PostDate', '')
                
                # Only include Enrolled (2) or Applied (4)
                if status_id not in [2, 4]:
                    continue
                
                session = session_map.get(session_id, {})
                program = program_map.get(program_id, {})
                
                weeks = session_to_weeks.get(session_id, [])
                
                # Create one enrollment record per week in the session
                for week_num in weeks:
                    if 1 <= week_num <= 9:
                        enrollments.append({
                            'person_id': person_id,
                            'program_id': program_id,
                            'program_name': program.get('Name', 'Unknown'),
                            'session_id': session_id,
                            'session_name': session.get('Name', 'Unknown'),
                            'week': week_num,
                            'status_id': status_id,
                            'status_name': status_name,
                            'enrollment_date': effective_date or (post_date[:10] if post_date else ''),
                            'post_date': post_date
                        })
        
        logger.info(f"Processed {len(enrollments)} enrollment records (expanded for multi-week sessions)")
        
        return {
            'enrollments': enrollments,
            'sessions': sessions,
            'programs': programs,
            'season_id': season_id,
            'client_id': client_id,
            'fetched_at': datetime.now().isoformat()
        }
    
    def _extract_week_info(self, session_name: str, sort_order: int = 0) -> Dict:
        """
        Extract week information from session name
        
        Handles formats like:
        - "ECA Week 1"
        - "Week 1 (1WK)"
        - "Week 1"
        - "Teeny Tiny Tnuah - Full Session" (weeks 1-4)
        - "Theater Camp Weeks 2-5"
        - "Theater Camp Weeks 6-9"
        
        Args:
            session_name: Session name from API
            sort_order: Fallback sort order
            
        Returns:
            Dict with 'weeks' list (e.g., [1], [1,2,3,4], [2,3,4,5])
        """
        import re
        
        name_lower = session_name.lower()
        
        # Check for "Full Session" patterns (typically weeks 1-4)
        if 'full session' in name_lower:
            return {'weeks': [1, 2, 3, 4], 'type': 'full_session'}
        
        # Check for range patterns like "Weeks 2-5" or "Weeks 6-9"
        range_match = re.search(r'weeks?\s*(\d+)\s*[-–to]+\s*(\d+)', name_lower)
        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            return {'weeks': list(range(start, end + 1)), 'type': 'range'}
        
        # Check for single week patterns
        patterns = [
            r'week\s*(\d+)',      # "Week 1", "ECA Week 1"
            r'wk\s*(\d+)',        # "Wk 1"
            r'\((\d+)wk\)',       # "(1WK)"
            r'session\s*(\d+)',   # "Session 1"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, name_lower)
            if match:
                week = int(match.group(1))
                if 1 <= week <= 10:
                    return {'weeks': [week], 'type': 'single'}
        
        # Fallback to sort order
        if 1 <= sort_order <= 10:
            return {'weeks': [sort_order], 'type': 'sort_order'}
        
        return {'weeks': [], 'type': 'unknown'}
    
    def _extract_week_number(self, session_name: str, sort_order: int = 0) -> int:
        """
        Extract single week number (backward compatible)
        
        Returns:
            Single week number or 0
        """
        info = self._extract_week_info(session_name, sort_order)
        return info['weeks'][0] if info['weeks'] else 0


class EnrollmentDataProcessor:
    """Process enrollment data into dashboard-ready format"""
    
    # Program category mapping (same as your parser.py)
    PROGRAM_CATEGORIES = {
        'Early Childhood': ['Infants', 'Toddler', 'PK2', 'PK3', 'PK4'],
        'Variety': ['Tsofim', 'Yeladim', 'Chaverim', 'Giborim', 'Ozrim', 'Madliteen', 'Madli-teen', 'Madli-Teen'],
        'Sports': ['Basketball', 'Soccer', 'Tennis', 'Flag Football', 'Gymnastics', 'Karate', 'Multi-Sport', 'Baseball', 'Volleyball'],
        'Performing Arts': ["T'nuah", 'Tnuah', 'Theater', 'Theatre', 'Dance', 'Music'],
        'Teen Leadership': ['LIT', 'CIT', 'Leadership', 'Teen'],
        "Children's Trust": ["Children's Trust", 'Koach'],
        'Specialty': ['Art', 'Science', 'STEM', 'Coding', 'Robotics', 'Nature']
    }
    
    # Program goals (same as parser.py)
    PROGRAM_GOALS = {
        'Infants': 6, 'Toddler': 12, 'PK2': 26, 'PK3': 36, 'PK4': 40,
        'Tsofim': 20, 'Yeladim': 28, 'Chaverim': 24, 'Giborim': 10,
        # ... add all goals from your parser.py
    }
    
    def __init__(self):
        self.category_colors = {
            'Early Childhood': '#FFB347',
            'Variety': '#7CB342',
            'Sports': '#42A5F5',
            'Performing Arts': '#AB47BC',
            'Teen Leadership': '#5C6BC0',
            "Children's Trust": '#FF7043',
            'Specialty': '#26A69A'
        }
        
        self.category_emojis = {
            'Early Childhood': '👶',
            'Variety': '🏕️',
            'Sports': '⚽',
            'Performing Arts': '🎭',
            'Teen Leadership': '🌟',
            "Children's Trust": '🤝',
            'Specialty': '🎨'
        }
    
    def process_enrollment_data(self, raw_data: Dict) -> Dict:
        """
        Process raw API data into dashboard-ready format
        
        Args:
            raw_data: Output from CampMinderAPIClient.get_enrollment_report()
            
        Returns:
            Dict formatted for dashboard template
        """
        enrollments = raw_data.get('enrollments', [])
        
        if not enrollments:
            return self._empty_report()
        
        # Group by program
        programs_data = {}
        person_programs = {}  # Track unique campers
        
        for e in enrollments:
            program = e['program_name']
            week = e['week']
            person_id = e['person_id']
            enrollment_date = e['enrollment_date']
            
            if program not in programs_data:
                programs_data[program] = {
                    'weeks': {i: [] for i in range(1, 10)},
                    'total': 0,
                    'unique_campers': set()
                }
            
            if 1 <= week <= 9:
                programs_data[program]['weeks'][week].append({
                    'person_id': person_id,
                    'enrollment_date': enrollment_date
                })
                programs_data[program]['total'] += 1
            
            programs_data[program]['unique_campers'].add(person_id)
            
            # Track person across programs
            if person_id not in person_programs:
                person_programs[person_id] = set()
            person_programs[person_id].add(program)
        
        # Build program reports
        programs = []
        categories_data = {}
        total_weeks = 0
        
        for program_name, data in programs_data.items():
            category = self._get_category(program_name)
            goal = self._get_goal(program_name)
            
            week_counts = {f'week_{i}': len(data['weeks'][i]) for i in range(1, 10)}
            total = sum(week_counts.values())
            fte = round(total / 9, 2)
            percent = round((fte / goal * 100) if goal > 0 else 0, 1)
            
            programs.append({
                'program': program_name,
                'category': category,
                **week_counts,
                'total': total,
                'fte': fte,
                'goal': goal,
                'percent_to_goal': percent,
                'category_color': self.category_colors.get(category, '#999'),
                'category_color_light': self._lighten_color(self.category_colors.get(category, '#999'))
            })
            
            # Aggregate by category
            if category not in categories_data:
                categories_data[category] = {'fte': 0, 'goal': 0, 'programs': []}
            categories_data[category]['fte'] += fte
            categories_data[category]['goal'] += goal
            categories_data[category]['programs'].append(program_name)
            
            total_weeks += total
        
        # Build category summary
        categories = []
        for cat_name, cat_data in categories_data.items():
            pct = round((cat_data['fte'] / cat_data['goal'] * 100) if cat_data['goal'] > 0 else 0, 1)
            categories.append({
                'category': cat_name,
                'emoji': self.category_emojis.get(cat_name, '📋'),
                'fte': round(cat_data['fte'], 1),
                'goal': cat_data['goal'],
                'percent_to_goal': pct,
                'color': self.category_colors.get(cat_name, '#999'),
                'status': 'success' if pct >= 70 else 'warning' if pct >= 50 else 'danger'
            })
        
        # Calculate summary
        total_enrollment = len(person_programs)
        total_fte = round(total_weeks / 9, 2)
        total_goal = sum(self.PROGRAM_GOALS.values())
        
        # Build date stats
        date_stats = self._build_date_stats(enrollments)
        
        # Build participants data for modal
        participants = self._build_participants_data(programs_data)
        
        return {
            'summary': {
                'total_enrollment': total_enrollment,
                'total_camper_weeks': total_weeks,
                'total_fte': total_fte,
                'goal': total_goal,
                'percent_to_goal': round((total_fte / total_goal * 100) if total_goal > 0 else 0, 1)
            },
            'programs': sorted(programs, key=lambda x: (x['category'], x['program'])),
            'categories': sorted(categories, key=lambda x: x['category']),
            'date_stats': date_stats,
            'participants': participants,
            'fetched_at': raw_data.get('fetched_at', datetime.now().isoformat())
        }
    
    def _get_category(self, program_name: str) -> str:
        """Get category for a program"""
        name_lower = program_name.lower()
        
        for category, keywords in self.PROGRAM_CATEGORIES.items():
            for keyword in keywords:
                if keyword.lower() in name_lower:
                    return category
        
        return 'Other'
    
    def _get_goal(self, program_name: str) -> int:
        """Get goal for a program"""
        # First try exact match
        if program_name in self.PROGRAM_GOALS:
            return self.PROGRAM_GOALS[program_name]
        
        # Try partial match
        name_lower = program_name.lower()
        for prog, goal in self.PROGRAM_GOALS.items():
            if prog.lower() in name_lower:
                return goal
        
        return 20  # Default goal
    
    def _lighten_color(self, hex_color: str) -> str:
        """Create a lighter version of a color"""
        # Simple approach: add transparency
        return hex_color + '20'
    
    def _build_date_stats(self, enrollments: List[Dict]) -> Dict:
        """Build date statistics from enrollments"""
        date_counts = {}
        
        for e in enrollments:
            date = e.get('enrollment_date', '')[:10]
            if not date:
                continue
            
            if date not in date_counts:
                date_counts[date] = {'registrations': 0, 'campers': set()}
            
            date_counts[date]['registrations'] += 1
            date_counts[date]['campers'].add(e['person_id'])
        
        # Build daily data
        daily = []
        cumulative_weeks = 0
        all_campers = set()
        
        for date in sorted(date_counts.keys()):
            data = date_counts[date]
            new_campers = data['campers'] - all_campers
            all_campers.update(data['campers'])
            cumulative_weeks += data['registrations']
            
            daily.append({
                'date': date,
                'new_registrations': len(new_campers),
                'camper_weeks_added': data['registrations'],
                'cumulative_campers': len(all_campers),
                'cumulative_weeks': cumulative_weeks
            })
        
        return {'daily': daily}
    
    def _build_participants_data(self, programs_data: Dict) -> Dict:
        """Build participants data for modal popups"""
        participants = {}
        
        for program_name, data in programs_data.items():
            participants[program_name] = {}
            for week, campers in data['weeks'].items():
                participants[program_name][str(week)] = [
                    {
                        'person_id': c['person_id'],
                        'first_name': f"Camper",  # Would need person lookup for real names
                        'last_name': str(c['person_id']),
                        'enrollment_date': c['enrollment_date']
                    }
                    for c in campers
                ]
        
        return participants
    
    def _empty_report(self) -> Dict:
        """Return empty report structure"""
        return {
            'summary': {
                'total_enrollment': 0,
                'total_camper_weeks': 0,
                'total_fte': 0,
                'goal': 0,
                'percent_to_goal': 0
            },
            'programs': [],
            'categories': [],
            'date_stats': {'daily': []},
            'participants': {}
        }


# Convenience function
def fetch_live_enrollment(api_key: str, subscription_key: str, season_id: int = 2026) -> Dict:
    """
    Fetch live enrollment data from CampMinder API
    
    Args:
        api_key: CampMinder API key
        subscription_key: Azure subscription key
        season_id: Season year (default 2026)
        
    Returns:
        Dashboard-ready enrollment data
    """
    client = CampMinderAPIClient(api_key, subscription_key)
    raw_data = client.get_enrollment_report(season_id)
    
    processor = EnrollmentDataProcessor()
    return processor.process_enrollment_data(raw_data)
