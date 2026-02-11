"""
Camp Sol Taplin - Enrollment Dashboard
Flask Application with User Management and Live CampMinder API Integration
"""

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, session, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import pandas as pd
import os
import json
import traceback
from datetime import datetime, timedelta
from io import BytesIO
import threading

# Import our custom modules
from parser import CampMinderParser
from historical_data import HistoricalDataManager

# Try to import CampMinder API client (optional)
try:
    from campminder_api import CampMinderAPIClient, EnrollmentDataProcessor
    CAMPMINDER_API_AVAILABLE = True
except ImportError:
    CAMPMINDER_API_AVAILABLE = False

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'camp-sol-taplin-2026-secret-key')

# Configuration
UPLOAD_FOLDER = 'static/uploads'
DATA_FOLDER = 'data'
USERS_FILE = os.path.join(DATA_FOLDER, 'users.json')
CACHE_FILE = os.path.join(DATA_FOLDER, 'api_cache.json')
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max

# CampMinder API Configuration (from environment variables)
CAMPMINDER_API_KEY = os.environ.get('CAMPMINDER_API_KEY')
CAMPMINDER_SUBSCRIPTION_KEY = os.environ.get('CAMPMINDER_SUBSCRIPTION_KEY')
CAMPMINDER_SEASON_ID = int(os.environ.get('CAMPMINDER_SEASON_ID', '2026'))
CACHE_TTL_MINUTES = 15  # Cache data for 15 minutes

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# ==================== USER MANAGEMENT ====================

def load_users():
    """Load users from JSON file"""
    # Check if we need to recreate default users (version check)
    recreate = False
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, 'r') as f:
                existing = json.load(f)
                # Check if new admin user exists
                if 'campsoltaplin@marjcc.org' in existing:
                    return existing
                else:
                    recreate = True  # Need to update to new user structure
        except:
            recreate = True
    else:
        recreate = True

    if recreate:
        # Default users
        default_users = {
            'campsoltaplin@marjcc.org': {
                'password': generate_password_hash('M@rjcc2026'),
                'role': 'admin',
                'created_at': datetime.now().isoformat()
            },
            'onlyview': {
                'password': generate_password_hash('M@rjcc2026'),
                'role': 'viewer',
                'created_at': datetime.now().isoformat()
            }
        }
        save_users(default_users)
        return default_users

    return {}

def save_users(users):
    """Save users to JSON file"""
    os.makedirs(DATA_FOLDER, exist_ok=True)
    with open(USERS_FILE, 'w') as f:
        json.dump(users, f, indent=2)

# Load users on startup
USERS = load_users()

class User(UserMixin):
    def __init__(self, username, role):
        self.id = username
        self.role = role

@login_manager.user_loader
def load_user(username):
    users = load_users()
    if username in users:
        return User(username, users[username]['role'])
    return None

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Initialize data managers
parser = CampMinderParser()
historical_manager = HistoricalDataManager()

# Store current report data in memory
current_report = {
    'data': None,
    'generated_at': None,
    'filename': None,
    'source': None  # 'csv' or 'api'
}

# API cache
api_cache = {
    'data': None,
    'fetched_at': None,
    'is_fetching': False
}

# ==================== CAMPMINDER API FUNCTIONS ====================

def is_api_configured() -> bool:
    """Check if CampMinder API is configured"""
    return bool(CAMPMINDER_API_KEY and CAMPMINDER_SUBSCRIPTION_KEY and CAMPMINDER_API_AVAILABLE)

def load_api_cache() -> dict:
    """Load cached API data from file"""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                cache = json.load(f)
                # Check if cache is still valid
                if cache.get('fetched_at'):
                    fetched_at = datetime.fromisoformat(cache['fetched_at'])
                    if datetime.now() - fetched_at < timedelta(minutes=CACHE_TTL_MINUTES):
                        return cache
        except Exception as e:
            print(f"Error loading API cache: {e}")
    return None

def save_api_cache(data: dict):
    """Save API data to cache file"""
    os.makedirs(DATA_FOLDER, exist_ok=True)
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Error saving API cache: {e}")

def fetch_live_data(force_refresh: bool = False) -> dict:
    """
    Fetch live enrollment data from CampMinder API
    
    Args:
        force_refresh: If True, bypass cache
        
    Returns:
        Processed enrollment data or None
    """
    global api_cache
    
    if not is_api_configured():
        print("CampMinder API not configured")
        return None
    
    # Check cache first (unless force refresh)
    if not force_refresh:
        cached = load_api_cache()
        if cached and cached.get('data'):
            print("Using cached API data")
            api_cache['data'] = cached['data']
            api_cache['fetched_at'] = cached.get('fetched_at')
            return cached['data']
    
    # Prevent concurrent fetches
    if api_cache['is_fetching']:
        print("API fetch already in progress")
        return api_cache.get('data')
    
    try:
        api_cache['is_fetching'] = True
        print(f"Fetching live data from CampMinder API (Season {CAMPMINDER_SEASON_ID})...")
        
        # Initialize API client
        client = CampMinderAPIClient(CAMPMINDER_API_KEY, CAMPMINDER_SUBSCRIPTION_KEY)
        
        # Fetch raw data
        raw_data = client.get_enrollment_report(CAMPMINDER_SEASON_ID)
        
        # Process into dashboard format
        processor = EnrollmentDataProcessor()
        processed_data = processor.process_enrollment_data(raw_data)
        
        # Update cache
        fetched_at = datetime.now().isoformat()
        api_cache['data'] = processed_data
        api_cache['fetched_at'] = fetched_at
        
        # Save to file
        save_api_cache({
            'data': processed_data,
            'fetched_at': fetched_at
        })
        
        print(f"API data fetched successfully: {processed_data['summary']['total_enrollment']} campers")
        return processed_data
        
    except Exception as e:
        print(f"Error fetching API data: {e}")
        traceback.print_exc()
        return api_cache.get('data')  # Return cached data if available
        
    finally:
        api_cache['is_fetching'] = False

# ==================== ROUTES ====================

@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        
        users = load_users()
        
        # Find user (case-insensitive)
        matched_user = None
        matched_username = None
        for stored_username, user_data in users.items():
            if stored_username.lower() == username.lower():
                matched_user = user_data
                matched_username = stored_username
                break
        
        if matched_user and check_password_hash(matched_user['password'], password):
            user = User(matched_username, matched_user['role'])
            login_user(user)
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid username or password', 'error')
    
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    # Determine data source: API (live) > CSV upload > None
    report_data = None
    generated_at = None
    data_source = None
    
    # Try to get data from API first (if configured)
    if is_api_configured():
        api_data = fetch_live_data(force_refresh=False)
        if api_data:
            report_data = api_data
            generated_at = api_cache.get('fetched_at', datetime.now().isoformat())
            if generated_at:
                try:
                    dt = datetime.fromisoformat(generated_at)
                    generated_at = dt.strftime('%B %d, %Y at %I:%M %p')
                except:
                    pass
            data_source = 'api'
    
    # Fall back to CSV upload data
    if not report_data and current_report.get('data'):
        report_data = current_report.get('data')
        generated_at = current_report.get('generated_at')
        data_source = 'csv'
    
    today = datetime.now()
    comparison_2025 = historical_manager.get_enrollment_as_of_date(2025, today.month, today.day)
    comparison_2024 = historical_manager.get_enrollment_as_of_date(2024, today.month, today.day)
    
    # Get pace comparison if we have current data
    pace_comparison = None
    if report_data:
        pace_comparison = historical_manager.get_pace_comparison(report_data)
    
    # Get historical comparison data
    historical_comparison = historical_manager.get_comparison_data()
    
    # Get daily data for charts
    historical_data_2025 = historical_manager.get_daily_data(2025)
    historical_data_2024 = historical_manager.get_daily_data(2024)
    
    # Get comparison chart data
    comparison_chart_data = historical_manager.get_weekly_comparison_chart_data()
    
    return render_template('dashboard.html',
                         report=report_data,
                         generated_at=generated_at,
                         data_source=data_source,
                         api_configured=is_api_configured(),
                         comparison_2025=comparison_2025,
                         comparison_2024=comparison_2024,
                         pace_comparison=pace_comparison,
                         historical_comparison=historical_comparison,
                         historical_data_2025=historical_data_2025,
                         historical_data_2024=historical_data_2024,
                         comparison_chart_data=comparison_chart_data,
                         user=current_user)

# ==================== USER MANAGEMENT ROUTES ====================

@app.route('/admin/users')
@login_required
def admin_users():
    """User management page"""
    if current_user.role != 'admin':
        flash('Access denied. Admin only.', 'error')
        return redirect(url_for('dashboard'))
    
    users = load_users()
    user_list = []
    for username, data in users.items():
        user_list.append({
            'username': username,
            'role': data.get('role', 'viewer'),
            'created_at': data.get('created_at', 'Unknown')
        })
    
    return render_template('admin_users.html', users=user_list, user=current_user)

@app.route('/api/users', methods=['GET'])
@login_required
def api_get_users():
    """API: Get all users"""
    if current_user.role != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403
    
    users = load_users()
    user_list = []
    for username, data in users.items():
        user_list.append({
            'username': username,
            'role': data.get('role', 'viewer'),
            'created_at': data.get('created_at', 'Unknown')
        })
    
    return jsonify({'users': user_list})

@app.route('/api/users', methods=['POST'])
@login_required
def api_create_user():
    """API: Create new user"""
    if current_user.role != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403
    
    data = request.get_json()
    username = data.get('username', '').strip().lower()
    password = data.get('password', '')
    role = data.get('role', 'viewer')
    
    # Validation
    if not username or len(username) < 3:
        return jsonify({'error': 'Username must be at least 3 characters'}), 400
    
    if not password or len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    
    if role not in ['admin', 'viewer', 'unit_leader']:
        return jsonify({'error': 'Invalid role'}), 400
    
    # Check if username is alphanumeric
    if not username.replace('_', '').replace('.', '').isalnum():
        return jsonify({'error': 'Username can only contain letters, numbers, underscores and dots'}), 400
    
    users = load_users()
    
    if username in users:
        return jsonify({'error': 'Username already exists'}), 400
    
    # Create user
    users[username] = {
        'password': generate_password_hash(password),
        'role': role,
        'created_at': datetime.now().isoformat()
    }
    
    save_users(users)
    
    return jsonify({
        'success': True,
        'message': f'User "{username}" created successfully',
        'user': {
            'username': username,
            'role': role
        }
    })

@app.route('/api/users/<username>', methods=['DELETE'])
@login_required
def api_delete_user(username):
    """API: Delete user"""
    if current_user.role != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403
    
    username = username.lower()
    
    # Cannot delete yourself
    if username == current_user.id:
        return jsonify({'error': 'Cannot delete your own account'}), 400
    
    users = load_users()
    
    if username not in users:
        return jsonify({'error': 'User not found'}), 404
    
    del users[username]
    save_users(users)
    
    return jsonify({
        'success': True,
        'message': f'User "{username}" deleted successfully'
    })

@app.route('/api/users/<username>/password', methods=['PUT'])
@login_required
def api_change_password(username):
    """API: Change user password"""
    if current_user.role != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403
    
    username = username.lower()
    data = request.get_json()
    new_password = data.get('password', '')
    
    if not new_password or len(new_password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    
    users = load_users()
    
    if username not in users:
        return jsonify({'error': 'User not found'}), 404
    
    users[username]['password'] = generate_password_hash(new_password)
    save_users(users)
    
    return jsonify({
        'success': True,
        'message': f'Password for "{username}" changed successfully'
    })

@app.route('/api/users/<username>/role', methods=['PUT'])
@login_required
def api_change_role(username):
    """API: Change user role"""
    if current_user.role != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403
    
    username = username.lower()
    
    # Cannot change your own role
    if username == current_user.id:
        return jsonify({'error': 'Cannot change your own role'}), 400
    
    data = request.get_json()
    new_role = data.get('role', '')
    
    if new_role not in ['admin', 'viewer']:
        return jsonify({'error': 'Invalid role'}), 400
    
    users = load_users()
    
    if username not in users:
        return jsonify({'error': 'User not found'}), 404
    
    users[username]['role'] = new_role
    save_users(users)
    
    return jsonify({
        'success': True,
        'message': f'Role for "{username}" changed to {new_role}'
    })

# ==================== CAMPMINDER API ROUTES ====================

@app.route('/api/refresh', methods=['POST'])
@login_required
def api_refresh_data():
    """Refresh data from CampMinder API"""
    if not is_api_configured():
        return jsonify({
            'success': False,
            'error': 'CampMinder API not configured. Please set CAMPMINDER_API_KEY and CAMPMINDER_SUBSCRIPTION_KEY environment variables.'
        }), 400
    
    try:
        print("Manual refresh requested...")
        data = fetch_live_data(force_refresh=True)
        
        if data:
            return jsonify({
                'success': True,
                'message': 'Data refreshed successfully',
                'summary': {
                    'total_enrollment': data['summary']['total_enrollment'],
                    'total_camper_weeks': data['summary']['total_camper_weeks'],
                    'programs_count': len(data.get('programs', []))
                },
                'fetched_at': api_cache.get('fetched_at')
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Failed to fetch data from API'
            }), 500
            
    except Exception as e:
        print(f"Refresh error: {e}")
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/participants/<program>/<int:week>')
@login_required
def api_participants(program, week):
    """Fetch participant details (names + guardian emails) for a specific program/week"""
    # Get participants from cached report data
    data = api_cache.get('data')
    if not data or 'participants' not in data:
        return jsonify({'participants': [], 'error': 'No data available'}), 404

    program_data = data['participants'].get(program, {})
    participants = program_data.get(str(week), [])

    if not participants:
        return jsonify({'participants': []})

    # Get unique person_ids that need lookup
    person_ids = [p['person_id'] for p in participants]

    # Load persons cache from file
    persons_cache_file = os.path.join(DATA_FOLDER, 'persons_cache.json')
    persons_cache = {}
    try:
        if os.path.exists(persons_cache_file):
            with open(persons_cache_file, 'r') as f:
                persons_cache = json.load(f)
    except Exception:
        pass

    # Fetch any missing persons using batch endpoint
    pids_to_fetch = [pid for pid in person_ids if str(pid) not in persons_cache]

    if pids_to_fetch and is_api_configured():
        try:
            client = CampMinderAPIClient(CAMPMINDER_API_KEY, CAMPMINDER_SUBSCRIPTION_KEY)
            if not client.authenticate():
                print("Failed to authenticate with CampMinder API for persons lookup")
                raise Exception("Authentication failed")

            # Step 1: Batch fetch all campers (with relatives info)
            print(f"Fetching {len(pids_to_fetch)} persons via batch API...")
            camper_results = client.get_persons_batch(
                pids_to_fetch,
                include_contact_details=True,
                include_relatives=True
            )
            print(f"Got {len(camper_results)} camper results from API")

            # Build a map of camper ID -> camper data
            camper_map = {}
            guardian_ids_needed = set()
            for person in camper_results:
                pid = person.get('ID')
                if pid:
                    camper_map[pid] = person
                    # Collect guardian IDs we need to fetch for emails
                    for rel in person.get('Relatives', []):
                        if rel.get('IsGuardian'):
                            guardian_ids_needed.add(rel['ID'])

            # Step 2: Batch fetch all guardians (with contact details for emails)
            guardian_map = {}
            if guardian_ids_needed:
                guardian_ids_list = list(guardian_ids_needed)
                print(f"Fetching {len(guardian_ids_list)} guardians via batch API...")
                guardian_results = client.get_persons_batch(
                    guardian_ids_list,
                    include_contact_details=True,
                    include_relatives=False
                )
                for g in guardian_results:
                    gid = g.get('ID')
                    if gid:
                        guardian_map[gid] = g
                print(f"Got {len(guardian_map)} guardian results from API")

            # Step 3: Build person_info for each camper
            for pid in pids_to_fetch:
                camper = camper_map.get(pid)
                if camper:
                    person_info = {
                        'first_name': camper.get('Name', {}).get('First', ''),
                        'last_name': camper.get('Name', {}).get('Last', ''),
                        'f1p1_email': '', 'f1p1_email2': '',
                        'f1p2_email': '', 'f1p2_email2': ''
                    }

                    # Get guardians sorted by IsPrimary
                    relatives = camper.get('Relatives', [])
                    guardians = [r for r in relatives if r.get('IsGuardian')]
                    guardians.sort(key=lambda r: (not r.get('IsPrimary', False)))

                    for g_idx, guardian in enumerate(guardians[:2]):
                        g_id = guardian.get('ID')
                        g_person = guardian_map.get(g_id)
                        if g_person:
                            emails = g_person.get('ContactDetails', {}).get('Emails', [])
                            prefix = 'f1p1' if g_idx == 0 else 'f1p2'
                            if len(emails) > 0:
                                person_info[f'{prefix}_email'] = emails[0].get('Address', '')
                            if len(emails) > 1:
                                person_info[f'{prefix}_email2'] = emails[1].get('Address', '')

                    persons_cache[str(pid)] = person_info
                else:
                    persons_cache[str(pid)] = {
                        'first_name': 'Camper', 'last_name': str(pid),
                        'f1p1_email': '', 'f1p1_email2': '',
                        'f1p2_email': '', 'f1p2_email2': ''
                    }

            # Save updated cache
            try:
                with open(persons_cache_file, 'w') as f:
                    json.dump(persons_cache, f)
            except Exception:
                pass
        except Exception as e:
            print(f"Error fetching persons batch: {e}")
            traceback.print_exc()

    # Build enriched participants list
    enriched = []
    for p in participants:
        pid = str(p['person_id'])
        info = persons_cache.get(pid, {})
        enriched.append({
            'person_id': p['person_id'],
            'first_name': info.get('first_name', 'Camper'),
            'last_name': info.get('last_name', str(p['person_id'])),
            'enrollment_date': p.get('enrollment_date', ''),
            'f1p1_email': info.get('f1p1_email', ''),
            'f1p1_email2': info.get('f1p1_email2', ''),
            'f1p2_email': info.get('f1p2_email', ''),
            'f1p2_email2': info.get('f1p2_email2', '')
        })

    # Sort by last name
    enriched.sort(key=lambda x: (x['last_name'].lower(), x['first_name'].lower()))

    return jsonify({'participants': enriched})

@app.route('/api/status')
@login_required
def api_status():
    """Get API configuration status"""
    # Show partial keys for debugging (safe to show first/last few chars)
    api_key_preview = None
    sub_key_preview = None
    
    if CAMPMINDER_API_KEY:
        api_key_preview = f"{CAMPMINDER_API_KEY[:10]}...{CAMPMINDER_API_KEY[-5:]}" if len(CAMPMINDER_API_KEY) > 15 else "TOO_SHORT"
    
    if CAMPMINDER_SUBSCRIPTION_KEY:
        sub_key_preview = f"{CAMPMINDER_SUBSCRIPTION_KEY[:8]}...{CAMPMINDER_SUBSCRIPTION_KEY[-4:]}" if len(CAMPMINDER_SUBSCRIPTION_KEY) > 12 else "TOO_SHORT"
    
    return jsonify({
        'api_configured': is_api_configured(),
        'campminder_api_module_available': CAMPMINDER_API_AVAILABLE,
        'api_key_set': bool(CAMPMINDER_API_KEY),
        'api_key_length': len(CAMPMINDER_API_KEY) if CAMPMINDER_API_KEY else 0,
        'api_key_preview': api_key_preview,
        'subscription_key_set': bool(CAMPMINDER_SUBSCRIPTION_KEY),
        'subscription_key_length': len(CAMPMINDER_SUBSCRIPTION_KEY) if CAMPMINDER_SUBSCRIPTION_KEY else 0,
        'subscription_key_preview': sub_key_preview,
        'season_id': CAMPMINDER_SEASON_ID,
        'cache_ttl_minutes': CACHE_TTL_MINUTES,
        'last_fetch': api_cache.get('fetched_at'),
        'is_fetching': api_cache.get('is_fetching', False),
        'has_cached_data': bool(api_cache.get('data'))
    })

@app.route('/api/test-auth')
@login_required
def api_test_auth():
    """Test CampMinder API authentication - for debugging"""
    if not is_api_configured():
        return jsonify({
            'success': False,
            'error': 'API not configured',
            'api_key_set': bool(CAMPMINDER_API_KEY),
            'subscription_key_set': bool(CAMPMINDER_SUBSCRIPTION_KEY)
        })
    
    import requests
    
    url = "https://api.campminder.com/auth/apikey"
    # Note: CampMinder auth endpoint does NOT want 'Bearer ' prefix
    headers = {
        "Authorization": CAMPMINDER_API_KEY,
        "Ocp-Apim-Subscription-Key": CAMPMINDER_SUBSCRIPTION_KEY
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        return jsonify({
            'success': response.status_code == 200,
            'status_code': response.status_code,
            'response_text': response.text[:500] if response.text else None,
            'response_headers': dict(response.headers),
            'request_url': url,
            'api_key_used': f"{CAMPMINDER_API_KEY[:10]}...{CAMPMINDER_API_KEY[-5:]}" if CAMPMINDER_API_KEY else None,
            'subscription_key_used': f"{CAMPMINDER_SUBSCRIPTION_KEY[:8]}..." if CAMPMINDER_SUBSCRIPTION_KEY else None
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__
        })

@app.route('/api/debug-data')
@login_required
def api_debug_data():
    """Debug endpoint to see raw API data"""
    # Check if API is configured
    api_key = os.environ.get('CAMPMINDER_API_KEY')
    sub_key = os.environ.get('CAMPMINDER_SUBSCRIPTION_KEY')
    season_id = int(os.environ.get('CAMPMINDER_SEASON_ID', '2026'))
    
    # First check - are env vars set?
    if not api_key or not sub_key:
        return jsonify({
            'error': 'Environment variables not set',
            'api_key_exists': bool(api_key),
            'sub_key_exists': bool(sub_key)
        })
    
    # Second check - is campminder_api module available?
    if not CAMPMINDER_API_AVAILABLE:
        return jsonify({
            'error': 'campminder_api module not available - check if file exists and has no import errors'
        })
    
    try:
        from campminder_api import CampMinderAPIClient
        
        client = CampMinderAPIClient(api_key, sub_key)
        
        # Authenticate first
        auth_result = client.authenticate()
        if not auth_result:
            return jsonify({
                'error': 'Authentication failed',
                'client_id': client.client_id,
                'jwt_token_exists': bool(client.jwt_token)
            })
        
        client_id = client.client_id
        
        # Get sessions
        sessions = client.get_sessions(season_id, client_id)
        
        # Get programs  
        programs = client.get_programs(season_id, client_id)
        
        # Get attendees (status=6 means Enrolled+Applied)
        attendees = client.get_attendees(season_id, client_id, status=6)
        
        # Create a simple session map: ID -> Name
        session_map = {s['ID']: s['Name'] for s in sessions} if sessions else {}
        
        # Create program map: ID -> Name
        program_map = {p['ID']: p['Name'] for p in programs} if programs else {}
        
        # Get unique session IDs from attendees
        attendee_session_ids = set()
        for att in (attendees or []):
            for sps in att.get('SessionProgramStatus', []):
                attendee_session_ids.add(sps.get('SessionID'))
        
        # Check which session IDs from attendees are missing from sessions
        missing_session_ids = attendee_session_ids - set(session_map.keys())
        
        return jsonify({
            'success': True,
            'client_id': client_id,
            'season_id': season_id,
            'sessions_count': len(sessions) if sessions else 0,
            'sessions_all': [{'id': s['ID'], 'name': s['Name'], 'sort': s.get('SortOrder')} for s in (sessions or [])],
            'programs_count': len(programs) if programs else 0,
            'programs_all': [{'id': p['ID'], 'name': p['Name']} for p in (programs or [])],
            'attendees_count': len(attendees) if attendees else 0,
            'attendees_sample': attendees[:3] if attendees else [],
            'unique_session_ids_in_attendees': list(attendee_session_ids),
            'missing_session_ids': list(missing_session_ids),
            'session_map_sample': dict(list(session_map.items())[:10])
        })
        
    except Exception as e:
        import traceback
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        })

# ==================== UPLOAD & DATA ROUTES ====================

@app.route('/upload', methods=['POST'])
@login_required
def upload_file():
    """Handle CSV file upload and processing"""
    print("=" * 50)
    print("UPLOAD REQUEST RECEIVED")
    print("=" * 50)
    
    try:
        if current_user.role != 'admin':
            print("ERROR: User not admin")
            return jsonify({'success': False, 'error': 'Unauthorized - admin access required'}), 403
        
        if 'file' not in request.files:
            print("ERROR: No file in request")
            return jsonify({'success': False, 'error': 'No file provided'}), 400
        
        file = request.files['file']
        print(f"File received: {file.filename}")
        
        if file.filename == '':
            print("ERROR: Empty filename")
            return jsonify({'success': False, 'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            print(f"ERROR: Invalid file type: {file.filename}")
            return jsonify({'success': False, 'error': 'Invalid file type. Please upload a CSV file.'}), 400
        
        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
        
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_filename = f"{timestamp}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], saved_filename)
        
        print(f"Saving file to: {filepath}")
        file.save(filepath)
        print(f"File saved successfully")
        
        if not os.path.exists(filepath):
            print("ERROR: File not saved")
            return jsonify({'success': False, 'error': 'Failed to save file'}), 500
        
        file_size = os.path.getsize(filepath)
        print(f"File size: {file_size} bytes")
        
        if file_size == 0:
            print("ERROR: File is empty")
            return jsonify({'success': False, 'error': 'File is empty'}), 400
        
        print("Parsing CSV...")
        try:
            report_data = parser.parse_csv(filepath)
            print(f"Parse successful!")
            print(f"  - Total enrollment: {report_data['summary']['total_enrollment']}")
            print(f"  - Total camper weeks: {report_data['summary']['total_camper_weeks']}")
            print(f"  - Programs found: {len(report_data['programs'])}")
        except Exception as parse_error:
            print(f"PARSE ERROR: {str(parse_error)}")
            print(traceback.format_exc())
            return jsonify({
                'success': False, 
                'error': f'Error parsing CSV: {str(parse_error)}'
            }), 500
        
        current_report['data'] = report_data
        current_report['generated_at'] = datetime.now().strftime('%B %d, %Y at %I:%M %p')
        current_report['filename'] = saved_filename
        
        print("Report data stored successfully")
        print("=" * 50)
        
        return jsonify({
            'success': True,
            'message': 'File processed successfully',
            'redirect': url_for('dashboard'),
            'summary': {
                'total_enrollment': report_data['summary']['total_enrollment'],
                'total_camper_weeks': report_data['summary']['total_camper_weeks'],
                'programs_count': len(report_data['programs'])
            }
        })
        
    except Exception as e:
        print(f"UNEXPECTED ERROR: {str(e)}")
        print(traceback.format_exc())
        return jsonify({
            'success': False, 
            'error': f'Server error: {str(e)}'
        }), 500

@app.route('/api/report-data')
@login_required
def get_report_data():
    """API endpoint to get current report data as JSON"""
    if current_report['data'] is None:
        return jsonify({'error': 'No report data available'}), 404
    
    today = datetime.now()
    comparison_2025 = historical_manager.get_enrollment_as_of_date(2025, today.month, today.day)
    comparison_2024 = historical_manager.get_enrollment_as_of_date(2024, today.month, today.day)
    
    return jsonify({
        'report': current_report['data'],
        'generated_at': current_report['generated_at'],
        'comparison_2025': comparison_2025,
        'comparison_2024': comparison_2024
    })

@app.route('/api/program-comparison/<program_name>')
@login_required
def get_program_comparison(program_name):
    """Get historical comparison data for a specific program"""
    # Get 2026 data from API or current report
    data_2026 = None
    if is_api_configured() and api_cache.get('data'):
        data_2026 = api_cache['data']
    elif current_report.get('data'):
        data_2026 = current_report['data']
    
    # Find program in 2026 data
    program_2026 = None
    if data_2026 and data_2026.get('programs'):
        for prog in data_2026['programs']:
            if prog['program'] == program_name:
                program_2026 = prog
                break
    
    # Get 2025 historical data
    program_2025 = historical_manager.get_program_data(2025, program_name)
    
    # Get 2024 historical data  
    program_2024 = historical_manager.get_program_data(2024, program_name)
    
    return jsonify({
        'program_name': program_name,
        'data_2026': program_2026,
        'data_2025': program_2025,
        'data_2024': program_2024
    })

@app.route('/download-excel')
@login_required
def download_excel():
    """Generate and download Excel report"""
    if current_report['data'] is None:
        flash('No report data available', 'error')
        return redirect(url_for('dashboard'))
    
    try:
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            summary_data = {
                'Metric': ['Total Enrollment', 'Total Camper Weeks', 'Total FTE', 'Goal', '% to Goal'],
                'Value': [
                    current_report['data']['summary']['total_enrollment'],
                    current_report['data']['summary']['total_camper_weeks'],
                    current_report['data']['summary']['total_fte'],
                    current_report['data']['summary']['goal'],
                    f"{current_report['data']['summary']['percent_to_goal']:.1f}%"
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            programs_df = pd.DataFrame(current_report['data']['programs'])
            programs_df.to_excel(writer, sheet_name='Programs', index=False)
            
            categories_df = pd.DataFrame(current_report['data']['categories'])
            categories_df.to_excel(writer, sheet_name='By Category', index=False)
        
        output.seek(0)
        
        filename = f"camp_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        flash(f'Error generating Excel: {str(e)}', 'error')
        return redirect(url_for('dashboard'))

# Cache for retention data (expensive to calculate)
retention_cache = {
    'data': None,
    'fetched_at': None
}
RETENTION_CACHE_TTL_HOURS = 24  # Cache retention for 24 hours

@app.route('/api/retention')
@login_required
def get_retention():
    """Get retention rate data from CampMinder API"""
    global retention_cache
    
    if not is_api_configured():
        return jsonify({'error': 'API not configured'}), 400
    
    # Check cache
    if retention_cache['data'] and retention_cache['fetched_at']:
        cache_age = datetime.now() - datetime.fromisoformat(retention_cache['fetched_at'])
        if cache_age.total_seconds() < RETENTION_CACHE_TTL_HOURS * 3600:
            return jsonify(retention_cache['data'])
    
    try:
        client = CampMinderAPIClient(
            api_key=CAMPMINDER_API_KEY,
            subscription_key=CAMPMINDER_SUBSCRIPTION_KEY
        )
        
        if not client.authenticate():
            return jsonify({'error': 'Authentication failed'}), 401
        
        retention_data = client.get_retention_rate(
            current_season=2026,
            previous_season=2025
        )
        
        # Update cache
        retention_cache['data'] = retention_data
        retention_cache['fetched_at'] = datetime.now().isoformat()
        
        return jsonify(retention_data)
        
    except Exception as e:
        print(f"Error fetching retention data: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# ==================== ERROR HANDLERS ====================

@app.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def server_error(e):
    return render_template('500.html'), 500

# ==================== MAIN ====================

if __name__ == '__main__':
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(DATA_FOLDER, exist_ok=True)
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=os.environ.get('DEBUG', 'False') == 'True')
