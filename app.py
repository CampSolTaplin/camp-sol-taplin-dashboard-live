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
CACHE_FILE = os.path.join(DATA_FOLDER, 'api_cache.json')
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max

# ==================== DATABASE CONFIG ====================
from flask_sqlalchemy import SQLAlchemy

database_url = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
# Render PostgreSQL uses "postgres://" but SQLAlchemy needs "postgresql://"
if database_url.startswith('postgres://'):
    database_url = database_url.replace('postgres://', 'postgresql://', 1)
app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# CampMinder API Configuration (from environment variables)
CAMPMINDER_API_KEY = os.environ.get('CAMPMINDER_API_KEY')
CAMPMINDER_SUBSCRIPTION_KEY = os.environ.get('CAMPMINDER_SUBSCRIPTION_KEY')
CAMPMINDER_SEASON_ID = int(os.environ.get('CAMPMINDER_SEASON_ID', '2026'))
CACHE_TTL_MINUTES = 15  # Cache data for 15 minutes

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# ==================== DATABASE MODELS ====================

class UserAccount(db.Model):
    __tablename__ = 'users'
    username = db.Column(db.String(120), primary_key=True)
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(20), nullable=False, default='viewer')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class GroupAssignment(db.Model):
    __tablename__ = 'group_assignments'
    id = db.Column(db.Integer, primary_key=True)
    program = db.Column(db.String(100), nullable=False)
    week = db.Column(db.Integer, nullable=False)
    person_id = db.Column(db.String(20), nullable=False)
    group_number = db.Column(db.Integer, nullable=False)
    __table_args__ = (db.UniqueConstraint('program', 'week', 'person_id'),)

# ==================== INIT DB & DEFAULT USERS ====================

with app.app_context():
    db.create_all()
    # Create default users if they don't exist
    if not UserAccount.query.filter_by(username='campsoltaplin@marjcc.org').first():
        db.session.add(UserAccount(
            username='campsoltaplin@marjcc.org',
            password_hash=generate_password_hash('M@rjcc2026'),
            role='admin'
        ))
    if not UserAccount.query.filter_by(username='onlyview').first():
        db.session.add(UserAccount(
            username='onlyview',
            password_hash=generate_password_hash('M@rjcc2026'),
            role='viewer'
        ))
    db.session.commit()

class User(UserMixin):
    def __init__(self, username, role):
        self.id = username
        self.role = role

@login_manager.user_loader
def load_user(username):
    u = db.session.get(UserAccount, username)
    if u:
        return User(u.username, u.role)
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

        # Find user (case-insensitive)
        u = UserAccount.query.filter(db.func.lower(UserAccount.username) == username.lower()).first()

        if u and check_password_hash(u.password_hash, password):
            user = User(u.username, u.role)
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
    
    all_users = UserAccount.query.all()
    user_list = []
    for u in all_users:
        user_list.append({
            'username': u.username,
            'role': u.role,
            'created_at': u.created_at.isoformat() if u.created_at else 'Unknown'
        })

    return render_template('admin_users.html', users=user_list, user=current_user)

@app.route('/api/users', methods=['GET'])
@login_required
def api_get_users():
    """API: Get all users"""
    if current_user.role != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403
    
    all_users = UserAccount.query.all()
    user_list = []
    for u in all_users:
        user_list.append({
            'username': u.username,
            'role': u.role,
            'created_at': u.created_at.isoformat() if u.created_at else 'Unknown'
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
    
    existing = UserAccount.query.filter_by(username=username).first()
    if existing:
        return jsonify({'error': 'Username already exists'}), 400

    # Create user
    new_user = UserAccount(
        username=username,
        password_hash=generate_password_hash(password),
        role=role
    )
    db.session.add(new_user)
    db.session.commit()

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
    
    u = UserAccount.query.filter_by(username=username).first()
    if not u:
        return jsonify({'error': 'User not found'}), 404

    db.session.delete(u)
    db.session.commit()

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
    
    u = UserAccount.query.filter_by(username=username).first()
    if not u:
        return jsonify({'error': 'User not found'}), 404

    u.password_hash = generate_password_hash(new_password)
    db.session.commit()

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
    
    u = UserAccount.query.filter_by(username=username).first()
    if not u:
        return jsonify({'error': 'User not found'}), 404

    u.role = new_role
    db.session.commit()

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

            # Step 3: Get "Share Group With" custom field
            share_group_map = {}  # pid -> value
            try:
                # Find the custom field definition for "Share Group With"
                # Note: custom fields API returns camelCase keys (name, id)
                cf_defs = client.get_custom_field_definitions()
                share_group_field_id = None
                print(f"Found {len(cf_defs)} custom field definitions")
                for cf in cf_defs:
                    # Handle both camelCase and PascalCase
                    cf_name = cf.get('name', cf.get('Name', ''))
                    cf_id = cf.get('id', cf.get('Id'))
                    if 'share' in cf_name.lower() and 'group' in cf_name.lower():
                        share_group_field_id = cf_id
                        print(f"Found 'Share Group With' custom field: ID={share_group_field_id}, Name={cf_name}")
                        break

                if share_group_field_id:
                    cf_data = client.get_custom_fields_for_persons(
                        pids_to_fetch, field_id=share_group_field_id,
                        season_id=CAMPMINDER_SEASON_ID
                    )
                    for pid, fields in cf_data.items():
                        for f in fields:
                            # Handle both camelCase and PascalCase
                            val = f.get('value', f.get('Value', ''))
                            if val:
                                share_group_map[pid] = val
                else:
                    print("Custom field 'Share Group With' not found in definitions")
            except Exception as e:
                print(f"Error fetching custom fields: {e}")

            # Step 4: Build person_info for each camper
            for pid in pids_to_fetch:
                camper = camper_map.get(pid)
                if camper:
                    person_info = {
                        'first_name': camper.get('Name', {}).get('First', ''),
                        'last_name': camper.get('Name', {}).get('Last', ''),
                        'f1p1_email': '', 'f1p1_email2': '',
                        'f1p2_email': '', 'f1p2_email2': '',
                        'share_group_with': share_group_map.get(pid, ''),
                        'gender': camper.get('GenderName', ''),
                        'medical_notes': '',
                        'siblings': [],
                        'aftercare': '',
                        'carpool': ''
                    }

                    # Get guardians and siblings from relatives
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
                        'f1p2_email': '', 'f1p2_email2': '',
                        'share_group_with': '',
                        'gender': '',
                        'medical_notes': '',
                        'siblings': [],
                        'aftercare': '',
                        'carpool': ''
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

    # Load group assignments for this program/week from DB
    ga_rows = GroupAssignment.query.filter_by(program=program, week=week).all()
    group_map = {ga.person_id: ga.group_number for ga in ga_rows}

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
            'f1p2_email2': info.get('f1p2_email2', ''),
            'share_group_with': info.get('share_group_with', ''),
            'gender': info.get('gender', ''),
            'medical_notes': info.get('medical_notes', ''),
            'siblings': ', '.join(info.get('siblings', [])) if isinstance(info.get('siblings'), list) else str(info.get('siblings', '')),
            'aftercare': info.get('aftercare', ''),
            'carpool': info.get('carpool', ''),
            'group': group_map.get(pid, 0)
        })

    # Sort by last name
    enriched.sort(key=lambda x: (x['last_name'].lower(), x['first_name'].lower()))

    return jsonify({'participants': enriched})

@app.route('/api/group-assignment/<program>/<int:week>', methods=['POST'])
@login_required
def api_save_group_assignment(program, week):
    """Save a group assignment for a single camper"""
    if current_user.role not in ['admin', 'unit_leader']:
        return jsonify({'error': 'Unauthorized'}), 403

    data = request.get_json()
    person_id = str(data.get('person_id', ''))
    group = data.get('group')

    if not person_id:
        return jsonify({'error': 'person_id is required'}), 400

    existing = GroupAssignment.query.filter_by(program=program, week=week, person_id=person_id).first()

    if group is None or group == 0:
        # Remove assignment
        if existing:
            db.session.delete(existing)
            db.session.commit()
    else:
        if existing:
            existing.group_number = int(group)
        else:
            db.session.add(GroupAssignment(
                program=program, week=week, person_id=person_id, group_number=int(group)
            ))
        db.session.commit()

    return jsonify({'success': True, 'key': f"{program}_{week}", 'person_id': person_id, 'group': group})

@app.route('/api/download-by-groups/<program>/<int:week>')
@login_required
def download_by_groups(program, week):
    """Generate and download Excel file organized by groups with attendance columns"""
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, Border, Side, PatternFill

    # Get participants data
    data = api_cache.get('data')
    if not data or 'participants' not in data:
        return jsonify({'error': 'No data available'}), 404

    program_data = data['participants'].get(program, {})
    participants = program_data.get(str(week), [])

    # Load persons cache
    persons_cache_file = os.path.join(DATA_FOLDER, 'persons_cache.json')
    persons_cache = {}
    try:
        if os.path.exists(persons_cache_file):
            with open(persons_cache_file, 'r') as f:
                persons_cache = json.load(f)
    except Exception:
        pass

    # Load group assignments from DB
    ga_rows = GroupAssignment.query.filter_by(program=program, week=week).all()
    group_map = {ga.person_id: ga.group_number for ga in ga_rows}

    # Build camper list
    campers = []
    for p in participants:
        pid = str(p['person_id'])
        info = persons_cache.get(pid, {})
        campers.append({
            'first_name': info.get('first_name', 'Camper'),
            'last_name': info.get('last_name', ''),
            'gender': info.get('gender', ''),
            'medical_notes': info.get('medical_notes', ''),
            'siblings': ', '.join(info.get('siblings', [])) if isinstance(info.get('siblings'), list) else str(info.get('siblings', '')),
            'aftercare': info.get('aftercare', ''),
            'carpool': info.get('carpool', ''),
            'share_group_with': info.get('share_group_with', ''),
            'group': group_map.get(pid, 0)
        })

    # Separate into groups
    groups = {}
    unassigned = []
    for c in campers:
        g = c['group']
        if g and g > 0:
            groups.setdefault(g, []).append(c)
        else:
            unassigned.append(c)

    # Create workbook
    wb = Workbook()
    wb.remove(wb.active)

    title_font = Font(bold=True, size=14)
    header_font = Font(bold=True, size=11)
    header_fill = PatternFill(start_color='CCE5FF', end_color='CCE5FF', fill_type='solid')
    thin_border = Border(
        left=Side(style='thin'), right=Side(style='thin'),
        top=Side(style='thin'), bottom=Side(style='thin')
    )

    def create_group_sheet(wb, sheet_title, group_label, camper_list):
        ws = wb.create_sheet(title=sheet_title)
        camper_list.sort(key=lambda c: (c['last_name'].lower(), c['first_name'].lower()))

        ws.merge_cells('A1:M1')
        ws['A1'] = f'{program} - Week {week} | {group_label}'
        ws['A1'].font = title_font

        headers = ['#', 'First Name', 'Last Name', 'Gender',
                   'Medical Notes', 'Siblings', 'Share Group With',
                   'AfterCare', 'Carpool', 'M', 'T', 'W', 'T', 'F']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = thin_border
            cell.alignment = Alignment(horizontal='center', wrap_text=True)

        for idx, camper in enumerate(camper_list, 1):
            row = idx + 3
            values = [idx, camper['first_name'], camper['last_name'],
                      camper['gender'], camper['medical_notes'],
                      camper['siblings'], camper['share_group_with'],
                      camper['aftercare'], camper['carpool'],
                      '', '', '', '', '']
            for col, val in enumerate(values, 1):
                cell = ws.cell(row=row, column=col, value=val)
                cell.border = thin_border

        ws.column_dimensions['A'].width = 4
        ws.column_dimensions['B'].width = 14
        ws.column_dimensions['C'].width = 14
        ws.column_dimensions['D'].width = 8
        ws.column_dimensions['E'].width = 22
        ws.column_dimensions['F'].width = 18
        ws.column_dimensions['G'].width = 18
        ws.column_dimensions['H'].width = 10
        ws.column_dimensions['I'].width = 10
        for col_letter in ['J', 'K', 'L', 'M', 'N']:
            ws.column_dimensions[col_letter].width = 5

        ws.page_setup.orientation = 'landscape'
        ws.page_setup.fitToWidth = 1
        ws.page_setup.fitToHeight = 1
        ws.sheet_properties.pageSetUpPr.fitToPage = True

    for group_num in sorted(groups.keys()):
        create_group_sheet(wb, f'Group {group_num}', f'Group {group_num}', groups[group_num])

    if unassigned:
        create_group_sheet(wb, 'Unassigned', 'Unassigned', unassigned)

    if not wb.sheetnames:
        ws = wb.create_sheet(title='No Groups')
        ws['A1'] = 'No group assignments found. Please assign groups first.'

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"{program}_Week{week}_ByGroups.xlsx"
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )

@app.route('/print-by-groups/<program>/<int:week>')
@login_required
def print_by_groups(program, week):
    """Render print-friendly page showing groups with attendance columns"""
    data = api_cache.get('data')
    if not data or 'participants' not in data:
        return "No data available", 404

    program_data = data['participants'].get(program, {})
    participants = program_data.get(str(week), [])

    persons_cache_file = os.path.join(DATA_FOLDER, 'persons_cache.json')
    persons_cache = {}
    try:
        if os.path.exists(persons_cache_file):
            with open(persons_cache_file, 'r') as f:
                persons_cache = json.load(f)
    except Exception:
        pass

    ga_rows = GroupAssignment.query.filter_by(program=program, week=week).all()
    group_map = {ga.person_id: ga.group_number for ga in ga_rows}

    campers = []
    for p in participants:
        pid = str(p['person_id'])
        info = persons_cache.get(pid, {})
        campers.append({
            'first_name': info.get('first_name', 'Camper'),
            'last_name': info.get('last_name', ''),
            'gender': info.get('gender', ''),
            'medical_notes': info.get('medical_notes', ''),
            'siblings': ', '.join(info.get('siblings', [])) if isinstance(info.get('siblings'), list) else str(info.get('siblings', '')),
            'aftercare': info.get('aftercare', ''),
            'carpool': info.get('carpool', ''),
            'share_group_with': info.get('share_group_with', ''),
            'group': group_map.get(pid, 0)
        })

    groups = {}
    unassigned = []
    for c in campers:
        g = c['group']
        if g and g > 0:
            groups.setdefault(g, []).append(c)
        else:
            unassigned.append(c)

    # Sort each group
    for g in groups:
        groups[g].sort(key=lambda c: (c['last_name'].lower(), c['first_name'].lower()))
    unassigned.sort(key=lambda c: (c['last_name'].lower(), c['first_name'].lower()))

    return render_template('print_by_groups.html',
                         program=program,
                         week=week,
                         groups=groups,
                         unassigned=unassigned)

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
