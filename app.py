"""
Camp Sol Taplin - Enrollment Dashboard
Flask Application with User Management and Live CampMinder API Integration
"""

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import pandas as pd
import os
import json
import traceback
import uuid
from datetime import datetime, date, timedelta
from io import BytesIO
import threading

# Import our custom modules
from parser import CampMinderParser
from historical_data import HistoricalDataManager
from budget_data import BUDGET_FY2026, parse_po_file, build_budget_vs_actual

# Try to import CampMinder API client (optional)
try:
    from campminder_api import CampMinderAPIClient, EnrollmentDataProcessor, FinancialDataProcessor
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
app.config['TEMPLATES_AUTO_RELOAD'] = True

# ==================== DATABASE CONFIG ====================
DB_POOL_SIZE = 5
DB_MAX_OVERFLOW = 10
DB_POOL_RECYCLE_SECONDS = 300

from flask_sqlalchemy import SQLAlchemy

database_url = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
# Render PostgreSQL uses "postgres://" but SQLAlchemy needs "postgresql://"
if database_url.startswith('postgres://'):
    database_url = database_url.replace('postgres://', 'postgresql://', 1)
app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# Connection pool settings to handle stale/dropped PostgreSQL connections
if database_url.startswith('postgresql://'):
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_pre_ping': True,        # Test connections before use (fixes SSL EOF errors)
        'pool_recycle': DB_POOL_RECYCLE_SECONDS,
        'pool_size': DB_POOL_SIZE,
        'max_overflow': DB_MAX_OVERFLOW,
    }

db = SQLAlchemy(app)

# CampMinder API Configuration (from environment variables)
CAMPMINDER_API_KEY = os.environ.get('CAMPMINDER_API_KEY')
CAMPMINDER_SUBSCRIPTION_KEY = os.environ.get('CAMPMINDER_SUBSCRIPTION_KEY')
CAMPMINDER_SEASON_ID = int(os.environ.get('CAMPMINDER_SEASON_ID', '2026'))
CACHE_TTL_MINUTES = 15  # Cache data for 15 minutes
ATTENDANCE_LOCK_HOUR = 17  # 5:00 PM

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# ==================== PERMISSION DEFINITIONS ====================

ALL_PERMISSIONS = [
    'view_dashboard', 'view_bydate', 'view_comparison',
    'view_campcomparison', 'view_detailed', 'view_finance',
    'edit_groups', 'download_excel', 'upload_csv',
    'manage_users', 'manage_settings',
    'take_attendance', 'view_attendance',
]

PERMISSION_LABELS = {
    'view_dashboard': 'Dashboard (Executive Summary)',
    'view_bydate': 'By Date View',
    'view_comparison': 'Year Comparison',
    'view_campcomparison': 'Camp Comparison',
    'view_detailed': 'Detailed View',
    'view_finance': 'Finance Tab',
    'edit_groups': 'Edit Groups',
    'download_excel': 'Download Excel',
    'upload_csv': 'Upload CSV',
    'manage_users': 'Manage Users',
    'manage_settings': 'Settings',
    'take_attendance': 'Take Attendance',
    'view_attendance': 'View Attendance (Admin)',
}

ROLE_DEFAULT_PERMISSIONS = {
    'admin': list(ALL_PERMISSIONS),
    'viewer': [
        'view_dashboard', 'view_bydate', 'view_comparison',
        'view_campcomparison', 'view_detailed', 'download_excel',
    ],
    'unit_leader': [
        'view_campcomparison', 'view_detailed', 'edit_groups',
        'take_attendance',
    ],
}

# ==================== DATABASE MODELS ====================

class UserAccount(db.Model):
    __tablename__ = 'users'
    username = db.Column(db.String(120), primary_key=True)
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(20), nullable=False, default='viewer')
    permissions = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def get_permissions(self):
        """Return list of permissions from DB, or role defaults if not set."""
        if self.permissions:
            try:
                return json.loads(self.permissions)
            except (json.JSONDecodeError, TypeError):
                pass
        return list(ROLE_DEFAULT_PERMISSIONS.get(self.role, []))

    def has_permission(self, perm):
        """Check if user has a specific permission."""
        return perm in self.get_permissions()

    def set_permissions(self, perms_list):
        self.permissions = json.dumps(perms_list)

class GroupAssignment(db.Model):
    __tablename__ = 'group_assignments'
    id = db.Column(db.Integer, primary_key=True)
    program = db.Column(db.String(100), nullable=False)
    week = db.Column(db.Integer, nullable=False)
    person_id = db.Column(db.String(20), nullable=False)
    group_number = db.Column(db.Integer, nullable=False)
    __table_args__ = (db.UniqueConstraint('program', 'week', 'person_id'),)

class ProgramSetting(db.Model):
    __tablename__ = 'program_settings'
    program = db.Column(db.String(120), primary_key=True)
    goal = db.Column(db.Integer, nullable=False, default=0)
    weeks_offered = db.Column(db.Integer, nullable=False, default=9)
    weeks_active = db.Column(db.String(50), nullable=False, default='1,2,3,4,5,6,7,8,9')
    active = db.Column(db.Boolean, nullable=False, default=True)

class GlobalSetting(db.Model):
    __tablename__ = 'global_settings'
    key = db.Column(db.String(50), primary_key=True)
    value = db.Column(db.String(200), nullable=False)

# ==================== ATTENDANCE MODELS ====================

class UnitLeaderAssignment(db.Model):
    __tablename__ = 'unit_leader_assignments'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(120), db.ForeignKey('users.username'), nullable=False)
    program_name = db.Column(db.String(100), nullable=False)
    __table_args__ = (db.UniqueConstraint('username', 'program_name'),)

class AttendanceCheckpoint(db.Model):
    __tablename__ = 'attendance_checkpoints'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False, unique=True)
    sort_order = db.Column(db.Integer, nullable=False, default=0)
    time_label = db.Column(db.String(20), nullable=True)
    active = db.Column(db.Boolean, nullable=False, default=True)

class AttendanceRecord(db.Model):
    __tablename__ = 'attendance_records'
    id = db.Column(db.Integer, primary_key=True)
    person_id = db.Column(db.String(20), nullable=False)
    program_name = db.Column(db.String(100), nullable=False)
    week = db.Column(db.Integer, nullable=False)
    date = db.Column(db.Date, nullable=False)
    checkpoint_id = db.Column(db.Integer, db.ForeignKey('attendance_checkpoints.id'), nullable=False)
    status = db.Column(db.String(20), nullable=False, default='present')
    recorded_by = db.Column(db.String(120), nullable=False)
    recorded_at = db.Column(db.DateTime, default=datetime.utcnow)
    notes = db.Column(db.String(500), nullable=True)
    __table_args__ = (
        db.UniqueConstraint('person_id', 'program_name', 'date', 'checkpoint_id'),
        db.Index('idx_attendance_date_program', 'date', 'program_name'),
    )

# ==================== INIT DB & DEFAULT USERS ====================

with app.app_context():
    db.create_all()
    # Ensure 'permissions' column exists (for existing databases)
    from sqlalchemy import inspect as sa_inspect
    inspector = sa_inspect(db.engine)
    columns = [c['name'] for c in inspector.get_columns('users')]
    if 'permissions' not in columns:
        with db.engine.connect() as conn:
            conn.execute(db.text("ALTER TABLE users ADD COLUMN permissions TEXT"))
            conn.commit()
    # Migrate existing users: backfill permissions from role if not set
    for u in UserAccount.query.all():
        if u.permissions is None:
            default_perms = ROLE_DEFAULT_PERMISSIONS.get(u.role, [])
            u.permissions = json.dumps(default_perms)
    db.session.commit()
    # Ensure admin users always have ALL current permissions (catches newly added ones)
    for u in UserAccount.query.filter_by(role='admin').all():
        current_perms = u.get_permissions()
        if set(current_perms) != set(ALL_PERMISSIONS):
            u.permissions = json.dumps(list(ALL_PERMISSIONS))
    db.session.commit()
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
    # Seed program settings from hardcoded defaults if table is empty
    if ProgramSetting.query.count() == 0:
        DEFAULT_GOALS = {
            'Infants': 6, 'Toddler': 12, 'PK2': 26, 'PK3': 36, 'PK4': 40,
            'Tsofim': 100, "Children's Trust Tsofim": 10,
            'Yeladim': 100, "Children's Trust Yeladim": 10,
            'Chaverim': 75, "Children's Trust Chaverim": 10,
            'Giborim': 60, "Children's Trust Giborim": 10,
            'Madli-Teen': 40, "Children's Trust Madli-Teen": 5,
            'Teen Travel': 30, 'Teen Travel: Epic Trip to Orlando': 15,
            'Basketball': 25, 'Flag Football': 20, 'Soccer': 25,
            'Sports Academy 1': 20, 'Sports Academy 2': 20,
            'Tennis Academy': 20, 'Tennis Academy - Half Day': 15,
            'Swim Academy': 20,
            'Tiny Tumblers Gymnastics': 15, 'Recreational Gymnastics': 20,
            'Competitive Gymnastics Team': 15, 'Volleyball': 20, 'MMA Camp': 15,
            'Teeny Tiny Tnuah': 20, 'Tiny Tnuah 1': 25, 'Tiny Tnuah 2': 25,
            'Tnuah 1': 30, 'Tnuah 2': 30, 'Extreme Tnuah': 20,
            'Art Exploration': 20, 'Music Camp': 20, 'Theater Camp': 25,
            'Madatzim 9th Grade': 25, 'Madatzim 10th Grade': 20,
            'OMETZ': 15
        }
        for prog, goal in DEFAULT_GOALS.items():
            db.session.add(ProgramSetting(program=prog, goal=goal, weeks_offered=9, weeks_active='1,2,3,4,5,6,7,8,9', active=True))
    # Migrate existing ProgramSettings: set weeks_active if missing/empty
    for ps in ProgramSetting.query.all():
        if not ps.weeks_active:
            # Reconstruct weeks_active from weeks_offered (e.g. 7 → "1,2,3,4,5,6,7")
            n = ps.weeks_offered if ps.weeks_offered and 1 <= ps.weeks_offered <= 9 else 9
            ps.weeks_active = ','.join(str(i) for i in range(1, n + 1))
    # Seed global settings if empty
    if not GlobalSetting.query.filter_by(key='total_goal').first():
        db.session.add(GlobalSetting(key='total_goal', value='750'))
    if not GlobalSetting.query.filter_by(key='revenue_goal').first():
        db.session.add(GlobalSetting(key='revenue_goal', value='0'))
    # Seed default attendance checkpoints
    if AttendanceCheckpoint.query.count() == 0:
        db.session.add(AttendanceCheckpoint(name='Morning', sort_order=1, time_label='9:00 AM', active=True))
        db.session.add(AttendanceCheckpoint(name='After Lunch', sort_order=2, time_label='1:00 PM', active=True))
        db.session.add(AttendanceCheckpoint(name='Departure', sort_order=3, time_label='3:30 PM', active=True))
        db.session.add(AttendanceCheckpoint(name='KC Before', sort_order=4, time_label='7:30 AM', active=True))
        db.session.add(AttendanceCheckpoint(name='KC After', sort_order=5, time_label='4:00 PM', active=True))
    else:
        # Ensure KC checkpoints exist (for existing databases)
        if not AttendanceCheckpoint.query.filter_by(name='KC Before').first():
            db.session.add(AttendanceCheckpoint(name='KC Before', sort_order=4, time_label='7:30 AM', active=True))
        if not AttendanceCheckpoint.query.filter_by(name='KC After').first():
            db.session.add(AttendanceCheckpoint(name='KC After', sort_order=5, time_label='4:00 PM', active=True))
    # Ensure Early Pickup checkpoint exists (stores EP flag independently of main status)
    if not AttendanceCheckpoint.query.filter_by(name='Early Pickup').first():
        db.session.add(AttendanceCheckpoint(name='Early Pickup', sort_order=6, time_label='', active=True))

    # ---- Unit Leader accounts ----
    UNIT_LEADER_PERMISSIONS = ['view_dashboard', 'view_detailed', 'edit_groups',
                               'take_attendance', 'view_attendance']
    UNIT_LEADERS = {
        'ecacamp@marjcc.org': [
            'Infants', 'Toddler', 'PK2', 'PK3', 'PK4',
        ],
        'tsofim@marjcc.org': [
            'Tsofim', "Children's Trust Tsofim",
        ],
        'yeladim@marjcc.org': [
            'Yeladim', "Children's Trust Yeladim",
        ],
        'chaverim@marjcc.org': [
            'Chaverim', "Children's Trust Chaverim",
        ],
        'giborim@marjcc.org': [
            'Giborim', "Children's Trust Giborim",
        ],
        'madli-teen@marjcc.org': [
            'Madli-Teen', "Children's Trust Madli-Teen",
        ],
        'teentravel@marjcc.org': [
            'Teen Travel', 'Teen Travel: Epic Trip to Orlando',
        ],
        'basketballcamp@marjcc.org': [
            'Basketball',
        ],
        'flagfootballcamp@marjcc.org': [
            'Flag Football',
        ],
        'soccercamp@marjcc.org': [
            'Soccer',
        ],
        'sportsacademy@marjcc.org': [
            'Sports Academy 1', 'Sports Academy 2',
        ],
        'tennisacademy@marjcc.org': [
            'Tennis Academy', 'Tennis Academy - Half Day',
        ],
        'swimacademy@marjcc.org': [
            'Swim Academy',
        ],
        'gymnastics@marjcc.org': [
            'Tiny Tumblers Gymnastics', 'Recreational Gymnastics',
            'Competitive Gymnastics Team',
        ],
        'volleyballcamp@marjcc.org': [
            'Volleyball',
        ],
        'karatecamp@marjcc.org': [
            'MMA Camp',
        ],
        'tnuah@marjcc.org': [
            'Teeny Tiny Tnuah', 'Tiny Tnuah 1', 'Tiny Tnuah 2',
            'Tnuah 1', 'Tnuah 2', 'Extreme Tnuah',
        ],
        'artexploration@marjcc.org': [
            'Art Exploration',
        ],
        'musicacademy@marjcc.org': [
            'Music Camp',
        ],
        'theatercamp@marjcc.org': [
            'Theater Camp',
        ],
        'madatzim@marjcc.org': [
            'Madatzim 9th Grade', 'Madatzim 10th Grade',
        ],
        'ometz@marjcc.org': [
            'OMETZ',
        ],
    }
    for ul_username, ul_programs in UNIT_LEADERS.items():
        if not UserAccount.query.filter_by(username=ul_username).first():
            u = UserAccount(
                username=ul_username,
                password_hash=generate_password_hash('M@rjcc2026'),
                role='unit_leader',
            )
            u.set_permissions(UNIT_LEADER_PERMISSIONS)
            db.session.add(u)
            db.session.flush()  # ensure user exists before adding assignments
            for prog in ul_programs:
                db.session.add(UnitLeaderAssignment(
                    username=ul_username, program_name=prog))
            print(f"Created unit leader: {ul_username} -> {', '.join(ul_programs)}")

    db.session.commit()

# ==================== CAMP WEEK UTILITIES ====================

# Week date ranges for Camp Sol Taplin 2026
CAMP_WEEK_DATES = {
    1: ('2026-06-08', '2026-06-12'),
    2: ('2026-06-15', '2026-06-19'),
    3: ('2026-06-22', '2026-06-26'),
    4: ('2026-06-29', '2026-07-03'),
    5: ('2026-07-06', '2026-07-10'),
    6: ('2026-07-13', '2026-07-17'),
    7: ('2026-07-20', '2026-07-24'),
    8: ('2026-07-27', '2026-07-31'),
    9: ('2026-08-03', '2026-08-07'),
}

def _parse_date_param(source=None, key='date'):
    """Parse a date string from request args or a dict, defaulting to today."""
    if source is None:
        date_str = request.args.get(key, date.today().isoformat())
    else:
        date_str = source.get(key, date.today().isoformat())
    try:
        return date.fromisoformat(date_str)
    except (ValueError, TypeError):
        return date.today()

def get_current_camp_week(today=None):
    """Return current camp week number (1-9) or None if not during camp."""
    if today is None:
        today = datetime.now().date()
    elif isinstance(today, datetime):
        today = today.date()
    for week_num, (start_str, end_str) in CAMP_WEEK_DATES.items():
        start = date.fromisoformat(start_str)
        end = date.fromisoformat(end_str)
        if start <= today <= end:
            return week_num
    return None

def is_camp_day(today=None):
    """Return True if today is a weekday within a camp week."""
    if today is None:
        today = datetime.now().date()
    elif isinstance(today, datetime):
        today = today.date()
    if today.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    return get_current_camp_week(today) is not None

class User(UserMixin):
    def __init__(self, username, role, permissions):
        self.id = username
        self.role = role
        self.permissions = permissions  # list of permission strings

    def has_permission(self, perm):
        """Check if user has a specific permission. Admin always has all."""
        if self.role == 'admin':
            return True
        return perm in self.permissions

@login_manager.user_loader
def load_user(username):
    u = db.session.get(UserAccount, username)
    if u:
        return User(u.username, u.role, u.get_permissions())
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

# Finance cache (separate, longer TTL)
finance_cache = {
    'data': None,
    'fetched_at': None,
    'is_fetching': False
}
FINANCE_CACHE_TTL_MINUTES = 60

# In-memory persons cache (avoids reading 200KB+ JSON from disk on every click)
_persons_mem_cache = None  # Loaded lazily on first use, then kept in memory

# PO (Purchase Order) data cache — persisted to data/po_data.json
po_cache = {'data': None, 'uploaded_at': None}
po_cache_path = os.path.join('data', 'po_data.json')
if os.path.exists(po_cache_path):
    try:
        with open(po_cache_path) as f:
            po_cache = json.load(f)
    except Exception:
        po_cache = {'data': None, 'uploaded_at': None}

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

def _load_program_settings() -> dict:
    """Load program settings from DB for the enrollment processor"""
    all_settings = ProgramSetting.query.all()
    total_goal_row = GlobalSetting.query.filter_by(key='total_goal').first()
    programs = {}
    for s in all_settings:
        # Compute weeks_offered from weeks_active string
        weeks_active = s.weeks_active or '1,2,3,4,5,6,7,8,9'
        weeks_count = len([w for w in weeks_active.split(',') if w.strip()])
        programs[s.program] = {
            'goal': s.goal,
            'weeks_offered': weeks_count if weeks_count > 0 else 9,
            'weeks_active': weeks_active,
            'active': s.active
        }
    return {
        'programs': programs,
        'total_goal': int(total_goal_row.value) if total_goal_row else 750
    }

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
            # Pre-fetch missing persons in background
            _prefetch_all_persons(cached['data'])
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
        
        # Load program settings from DB for processing
        db_settings = _load_program_settings()

        # Process into dashboard format
        processor = EnrollmentDataProcessor()
        processed_data = processor.process_enrollment_data(raw_data, program_settings=db_settings)
        
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

        # Pre-fetch all person details in background so clicks are instant
        _prefetch_all_persons(processed_data)

        return processed_data

    except Exception as e:
        print(f"Error fetching API data: {e}")
        traceback.print_exc()
        return api_cache.get('data')  # Return cached data if available

    finally:
        api_cache['is_fetching'] = False

def _prefetch_all_persons(enrollment_data):
    """Pre-fetch all person details in a background thread so participant clicks are instant."""
    participants = enrollment_data.get('participants', {})
    if not participants:
        return

    # Collect ALL unique person IDs across all programs/weeks
    all_person_ids = set()
    for program_data in participants.values():
        for week_participants in program_data.values():
            for p in week_participants:
                pid = p.get('person_id')
                if pid:
                    all_person_ids.add(pid)

    if not all_person_ids:
        return

    # Check which ones are missing from cache
    cache = _load_persons_cache()
    missing = [pid for pid in all_person_ids if str(pid) not in cache]

    if not missing:
        print(f"Persons pre-fetch: all {len(all_person_ids)} persons already cached [OK]")
        return

    print(f"Persons pre-fetch: {len(missing)}/{len(all_person_ids)} missing, fetching in background...")

    def _bg_prefetch(app_ctx, pids_to_fetch):
        with app_ctx:
            try:
                cache = _load_persons_cache()
                _fetch_and_cache_persons(pids_to_fetch, cache)
                print(f"Persons pre-fetch complete: {len(pids_to_fetch)} persons fetched [OK]")
            except Exception as e:
                print(f"Persons pre-fetch error: {e}")
                traceback.print_exc()

    t = threading.Thread(
        target=_bg_prefetch,
        args=(app.app_context(), missing),
        daemon=True
    )
    t.start()

def fetch_financial_data(force_refresh: bool = False, enrollment_report: dict = None) -> dict:
    """
    Fetch financial data from CampMinder Financial API.
    Cached separately from enrollment data with 60-min TTL.
    """
    global finance_cache

    if not is_api_configured():
        return None

    # Check cache
    if not force_refresh and finance_cache.get('data') and finance_cache.get('fetched_at'):
        try:
            cached_at = datetime.fromisoformat(finance_cache['fetched_at'])
            if datetime.now() - cached_at < timedelta(minutes=FINANCE_CACHE_TTL_MINUTES):
                print("Using cached finance data")
                return finance_cache['data']
        except Exception:
            pass

    if finance_cache.get('is_fetching'):
        # Safety timeout: reset is_fetching if stuck for more than 3 minutes
        fetch_start = finance_cache.get('fetch_start')
        if fetch_start and (datetime.now() - fetch_start).total_seconds() > 180:
            print("WARNING: is_fetching stuck for >3min, resetting", flush=True)
            finance_cache['is_fetching'] = False
        else:
            return finance_cache.get('data')

    try:
        finance_cache['is_fetching'] = True
        finance_cache['fetch_start'] = datetime.now()
        print(f"Fetching financial data from CampMinder API (Season {CAMPMINDER_SEASON_ID})...", flush=True)

        client = CampMinderAPIClient(CAMPMINDER_API_KEY, CAMPMINDER_SUBSCRIPTION_KEY)

        # Fetch the three financial endpoints
        categories = client.get_financial_categories()
        payment_methods = client.get_payment_methods()
        transactions = client.get_transaction_details(CAMPMINDER_SEASON_ID)

        print(f"Financial data fetched: {len(transactions)} transactions, "
              f"{len(categories)} categories, {len(payment_methods)} payment methods", flush=True)

        # Process financial data
        processor = FinancialDataProcessor()
        finance_data = processor.process_financial_data(
            transactions=transactions,
            categories=categories,
            payment_methods=payment_methods,
            enrollment_report=enrollment_report,
            season=CAMPMINDER_SEASON_ID
        )

        # Also try to fetch historical financial data for comparison
        for hist_season in [2025, 2024]:
            try:
                hist_txns = client.get_transaction_details(hist_season)
                if hist_txns:
                    hist_data = processor.process_financial_data(
                        transactions=hist_txns,
                        categories=categories,
                        payment_methods=payment_methods,
                        enrollment_report=None,
                        season=hist_season
                    )
                    finance_data[f'historical_{hist_season}'] = {
                        'net_revenue': hist_data['summary']['net_revenue'],
                        'gross_revenue': hist_data['summary']['gross_revenue'],
                        'total_discounts': hist_data['summary']['total_discounts'],
                        'timeline': hist_data.get('timeline', [])
                    }
                    print(f"Historical finance {hist_season}: ${hist_data['summary']['net_revenue']:,.2f} net revenue")
            except Exception as e:
                print(f"Could not fetch historical finance for {hist_season}: {e}")

        # Read revenue goal
        try:
            rg = GlobalSetting.query.filter_by(key='revenue_goal').first()
            finance_data['revenue_goal'] = int(rg.value) if rg and rg.value != '0' else 0
        except Exception:
            finance_data['revenue_goal'] = 0

        # Update cache
        finance_cache['data'] = finance_data
        finance_cache['fetched_at'] = datetime.now().isoformat()

        print(f"Finance data processed: ${finance_data['summary']['net_revenue']:,.2f} net revenue", flush=True)
        return finance_data

    except Exception as e:
        print(f"Error fetching financial data: {e}", flush=True)
        traceback.print_exc()
        import sys; sys.stderr.flush()
        return finance_cache.get('data')

    finally:
        finance_cache['is_fetching'] = False


# ==================== ROUTES ====================

@app.route('/')
def index():
    if current_user.is_authenticated:
        if hasattr(current_user, 'role') and current_user.role == 'unit_leader':
            return redirect(url_for('attendance_page'))
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
            user = User(u.username, u.role, u.get_permissions())
            login_user(user)
            # Unit leaders go directly to attendance page
            if u.role == 'unit_leader':
                return redirect(url_for('attendance_page'))
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid username or password', 'error')

    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

# ==================== PUBLIC SHARED VIEW ====================

@app.route('/shared/<token>')
def shared_matrix(token):
    """Public read-only enrollment matrix view (no login required)"""
    # Validate token against stored share_token
    stored = GlobalSetting.query.filter_by(key='share_token').first()
    if not stored or stored.value != token:
        return "Not Found", 404

    # Fetch enrollment data (same logic as dashboard route)
    report_data = None
    generated_at = None
    data_source = None

    if is_api_configured():
        api_data = fetch_live_data(force_refresh=False)
        if api_data:
            report_data = api_data
            generated_at = api_cache.get('fetched_at', datetime.now().isoformat())
            if generated_at:
                try:
                    dt = datetime.fromisoformat(generated_at)
                    generated_at = dt.strftime('%B %d, %Y at %I:%M %p')
                except (ValueError, TypeError):
                    pass
            data_source = 'api'

    # Fall back to CSV upload data
    if not report_data and current_report.get('data'):
        report_data = current_report.get('data')
        generated_at = current_report.get('generated_at')
        data_source = 'csv'

    if not report_data:
        return "Not Found", 404

    # Get 2025 program-level data for Old View Stats comparison
    today = datetime.now()
    programs_2025 = historical_manager.get_programs_as_of_date(2025, today.month, today.day)
    programs_2025_map = {}
    if isinstance(programs_2025, list):
        for p in programs_2025:
            if isinstance(p, dict):
                programs_2025_map[p.get('program', '')] = p

    return render_template('shared_matrix.html',
                         report=report_data,
                         generated_at=generated_at,
                         data_source=data_source,
                         programs_2025_map=programs_2025_map)

# ==================== DASHBOARD ====================

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
                except (ValueError, TypeError):
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
    
    # Get historical comparison data (pass 2026 daily for milestones)
    current_daily = report_data.get('date_stats', {}).get('daily', []) if report_data else []
    historical_comparison = historical_manager.get_comparison_data(current_daily=current_daily)
    
    # Get daily data for charts
    historical_data_2025 = historical_manager.get_daily_data(2025)
    historical_data_2024 = historical_manager.get_daily_data(2024)
    
    # Get comparison chart data
    comparison_chart_data = historical_manager.get_weekly_comparison_chart_data()

    # Get Children's Trust stats for historical years
    ct_stats_2024 = historical_manager.get_childrens_trust_stats(2024)
    ct_stats_2025 = historical_manager.get_childrens_trust_stats(2025)

    # Get CT daily data for date-filtered display in By Date tab
    ct_daily_2025 = historical_manager.get_ct_daily_data(2025)
    ct_daily_2024 = historical_manager.get_ct_daily_data(2024)

    # Get 2025 program-level data for OLD VIEW STATS comparison
    # Filter to only include enrollments up to the equivalent date last year
    # (e.g. if today is Feb 14 2026, show 2025 data through Feb 14 2025)
    programs_2025 = historical_manager.get_programs_as_of_date(2025, today.month, today.day)
    # Build a dict for quick lookup by program name
    programs_2025_map = {}
    if isinstance(programs_2025, list):
        for p in programs_2025:
            if isinstance(p, dict):
                programs_2025_map[p.get('program', '')] = p

    # Get financial data from cache only (non-blocking).
    # If no cache, the frontend will auto-trigger /api/finance/refresh via AJAX.
    finance_data = None
    if current_user.has_permission('view_finance') and is_api_configured():
        finance_data = finance_cache.get('data')
        # Kick off background fetch if cache is empty or stale
        if not finance_data:
            def _bg_fetch(app_ctx, report):
                with app_ctx:
                    try:
                        fetch_financial_data(enrollment_report=report)
                    except Exception as e:
                        print(f"Background finance fetch error: {e}", flush=True)
                        import traceback; traceback.print_exc()
            t = threading.Thread(target=_bg_fetch, args=(app.app_context(), report_data), daemon=True)
            t.start()

    # Budget + PO data for Finance tab
    budget_context = {
        'budget': BUDGET_FY2026,
        'po': po_cache.get('data'),
        'po_uploaded_at': po_cache.get('uploaded_at'),
    }

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
                         ct_stats_2024=ct_stats_2024,
                         ct_stats_2025=ct_stats_2025,
                         ct_daily_2024=ct_daily_2024,
                         ct_daily_2025=ct_daily_2025,
                         programs_2025_map=programs_2025_map,
                         finance_data=finance_data,
                         budget_data=budget_context,
                         camp_week_dates=CAMP_WEEK_DATES,
                         user=current_user)

# ==================== USER MANAGEMENT ROUTES ====================

@app.route('/admin/users')
@login_required
def admin_users():
    """User management page"""
    if not current_user.has_permission('manage_users'):
        flash('Access denied.', 'error')
        return redirect(url_for('dashboard'))
    
    all_users = UserAccount.query.all()
    user_list = []
    for u in all_users:
        perms = u.get_permissions()
        user_list.append({
            'username': u.username,
            'role': u.role,
            'permissions': perms,
            'perm_count': len(perms),
            'created_at': u.created_at.isoformat() if u.created_at else 'Unknown'
        })

    return render_template('admin_users.html', users=user_list, user=current_user,
                         all_permissions=ALL_PERMISSIONS,
                         permission_labels=PERMISSION_LABELS,
                         role_defaults=ROLE_DEFAULT_PERMISSIONS,
                         active_page='manage_users')

@app.route('/api/users', methods=['GET'])
@login_required
def api_get_users():
    """API: Get all users"""
    if not current_user.has_permission('manage_users'):
        return jsonify({'error': 'Unauthorized'}), 403

    all_users = UserAccount.query.all()
    user_list = []
    for u in all_users:
        perms = u.get_permissions()
        user_list.append({
            'username': u.username,
            'role': u.role,
            'permissions': perms,
            'perm_count': len(perms),
            'created_at': u.created_at.isoformat() if u.created_at else 'Unknown'
        })

    return jsonify({'users': user_list})

@app.route('/api/users', methods=['POST'])
@login_required
def api_create_user():
    """API: Create new user"""
    if not current_user.has_permission('manage_users'):
        return jsonify({'error': 'Unauthorized'}), 403

    data = request.get_json()
    username = data.get('username', '').strip().lower()
    password = data.get('password', '')
    role = data.get('role', 'viewer')
    permissions = data.get('permissions', None)

    # Validation
    if not username or len(username) < 3:
        return jsonify({'error': 'Username must be at least 3 characters'}), 400

    if not password or len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400

    if role not in ['admin', 'viewer', 'unit_leader']:
        return jsonify({'error': 'Invalid role'}), 400

    # Check if username is alphanumeric (allow @ for emails)
    clean = username.replace('_', '').replace('.', '').replace('@', '')
    if not clean.isalnum():
        return jsonify({'error': 'Username can only contain letters, numbers, underscores, dots and @'}), 400

    existing = UserAccount.query.filter_by(username=username).first()
    if existing:
        return jsonify({'error': 'Username already exists'}), 400

    # Determine permissions
    if permissions is not None:
        valid_perms = [p for p in permissions if p in ALL_PERMISSIONS]
        perms_json = json.dumps(valid_perms)
    else:
        perms_json = json.dumps(ROLE_DEFAULT_PERMISSIONS.get(role, []))

    # Create user
    new_user = UserAccount(
        username=username,
        password_hash=generate_password_hash(password),
        role=role,
        permissions=perms_json
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
    if not current_user.has_permission('manage_users'):
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
    if not current_user.has_permission('manage_users'):
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
    """API: Change user role and reset permissions to role defaults"""
    if not current_user.has_permission('manage_users'):
        return jsonify({'error': 'Unauthorized'}), 403

    username = username.lower()

    # Cannot change your own role
    if username == current_user.id:
        return jsonify({'error': 'Cannot change your own role'}), 400

    data = request.get_json()
    new_role = data.get('role', '')

    if new_role not in ['admin', 'viewer', 'unit_leader']:
        return jsonify({'error': 'Invalid role'}), 400

    u = UserAccount.query.filter_by(username=username).first()
    if not u:
        return jsonify({'error': 'User not found'}), 404

    u.role = new_role
    u.set_permissions(ROLE_DEFAULT_PERMISSIONS.get(new_role, []))
    db.session.commit()

    return jsonify({
        'success': True,
        'message': f'Role for "{username}" changed to {new_role}'
    })

@app.route('/api/users/<username>/permissions', methods=['GET'])
@login_required
def api_get_user_permissions(username):
    """API: Get permissions for a user"""
    if not current_user.has_permission('manage_users'):
        return jsonify({'error': 'Unauthorized'}), 403
    u = UserAccount.query.filter_by(username=username.lower()).first()
    if not u:
        return jsonify({'error': 'User not found'}), 404
    return jsonify({
        'username': u.username,
        'role': u.role,
        'permissions': u.get_permissions()
    })

@app.route('/api/users/<username>/permissions', methods=['PUT'])
@login_required
def api_update_user_permissions(username):
    """API: Update permissions for a user"""
    if not current_user.has_permission('manage_users'):
        return jsonify({'error': 'Unauthorized'}), 403
    username = username.lower()
    data = request.get_json()
    permissions = data.get('permissions', [])
    valid = [p for p in permissions if p in ALL_PERMISSIONS]
    u = UserAccount.query.filter_by(username=username).first()
    if not u:
        return jsonify({'error': 'User not found'}), 404
    u.set_permissions(valid)
    db.session.commit()
    return jsonify({'success': True, 'message': f'Permissions updated for "{username}"'})

# ==================== PROGRAM SETTINGS ROUTES ====================

SETTINGS_ORDER = [
    'Infants', 'Toddler', 'PK2', 'PK3', 'PK4',
    'Tsofim', "Children's Trust Tsofim",
    'Yeladim', "Children's Trust Yeladim",
    'Chaverim', "Children's Trust Chaverim",
    'Giborim', "Children's Trust Giborim",
    'Madli-Teen', "Children's Trust Madli-Teen",
    'Teen Travel', 'Teen Travel: Epic Trip to Orlando',
    'Basketball', 'Flag Football', 'Soccer',
    'Sports Academy 1', 'Sports Academy 2',
    'Tennis Academy', 'Tennis Academy - Half Day',
    'Swim Academy',
    'Tiny Tumblers Gymnastics', 'Recreational Gymnastics',
    'Competitive Gymnastics Team', 'Volleyball', 'MMA Camp',
    'Teeny Tiny Tnuah', 'Tiny Tnuah 1', 'Tiny Tnuah 2',
    'Tnuah 1', 'Tnuah 2', 'Extreme Tnuah',
    'Art Exploration', 'Music Camp', 'Theater Camp',
    'Madatzim 9th Grade', 'Madatzim 10th Grade',
    'OMETZ'
]

def _sort_settings(settings_list):
    """Sort settings by SETTINGS_ORDER"""
    order_map = {name: i for i, name in enumerate(SETTINGS_ORDER)}
    return sorted(settings_list, key=lambda s: order_map.get(s.program, 999))

@app.route('/admin/settings')
@login_required
def admin_settings():
    """Program settings management page"""
    if not current_user.has_permission('manage_settings'):
        flash('Access denied.', 'error')
        return redirect(url_for('dashboard'))
    all_settings = _sort_settings(ProgramSetting.query.all())
    total_goal = GlobalSetting.query.filter_by(key='total_goal').first()
    revenue_goal = GlobalSetting.query.filter_by(key='revenue_goal').first()
    return render_template('admin_settings.html',
                         settings=all_settings,
                         total_goal=int(total_goal.value) if total_goal else 750,
                         revenue_goal=int(revenue_goal.value) if revenue_goal and revenue_goal.value != '0' else 0,
                         user=current_user,
                         active_page='camps_goals')

@app.route('/api/settings', methods=['GET'])
@login_required
def api_get_settings():
    """API: Get all program settings"""
    if not current_user.has_permission('manage_settings'):
        return jsonify({'error': 'Unauthorized'}), 403
    all_settings = _sort_settings(ProgramSetting.query.all())
    total_goal = GlobalSetting.query.filter_by(key='total_goal').first()
    revenue_goal = GlobalSetting.query.filter_by(key='revenue_goal').first()
    return jsonify({
        'programs': [{
            'program': s.program,
            'goal': s.goal,
            'weeks_offered': s.weeks_offered,
            'weeks_active': s.weeks_active or '1,2,3,4,5,6,7,8,9',
            'active': s.active
        } for s in all_settings],
        'total_goal': int(total_goal.value) if total_goal else 750,
        'revenue_goal': int(revenue_goal.value) if revenue_goal and revenue_goal.value != '0' else 0
    })

@app.route('/api/settings', methods=['PUT'])
@login_required
def api_update_settings():
    """API: Bulk update program settings"""
    if not current_user.has_permission('manage_settings'):
        return jsonify({'error': 'Unauthorized'}), 403

    data = request.get_json()
    programs = data.get('programs', [])
    total_goal = data.get('total_goal')

    for p in programs:
        weeks_active = p.get('weeks_active', '1,2,3,4,5,6,7,8,9')
        weeks_count = len([w for w in weeks_active.split(',') if w.strip()])
        setting = ProgramSetting.query.filter_by(program=p['program']).first()
        if setting:
            setting.goal = int(p.get('goal', setting.goal))
            setting.weeks_active = weeks_active
            setting.weeks_offered = weeks_count if weeks_count > 0 else 9
            setting.active = bool(p.get('active', setting.active))
        else:
            db.session.add(ProgramSetting(
                program=p['program'],
                goal=int(p.get('goal', 0)),
                weeks_active=weeks_active,
                weeks_offered=weeks_count if weeks_count > 0 else 9,
                active=bool(p.get('active', True))
            ))

    if total_goal is not None:
        gs = GlobalSetting.query.filter_by(key='total_goal').first()
        if gs:
            gs.value = str(int(total_goal))
        else:
            db.session.add(GlobalSetting(key='total_goal', value=str(int(total_goal))))

    # Save revenue goal if provided
    revenue_goal = data.get('revenue_goal')
    if revenue_goal is not None:
        rg = GlobalSetting.query.filter_by(key='revenue_goal').first()
        if rg:
            rg.value = str(int(revenue_goal))
        else:
            db.session.add(GlobalSetting(key='revenue_goal', value=str(int(revenue_goal))))

    db.session.commit()

    # Clear both memory and file cache so dashboard recalculates with new settings
    api_cache['data'] = None
    api_cache['fetched_at'] = None
    # Also clear finance cache so it picks up new revenue_goal
    finance_cache['data'] = None
    finance_cache['fetched_at'] = None
    # Also clear the file cache
    try:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
    except Exception:
        pass

    return jsonify({'success': True, 'message': 'Settings saved successfully'})

# ==================== SHARE LINK API ====================

@app.route('/api/share-token', methods=['GET'])
@login_required
def api_get_share_token():
    """Get current share token for public matrix view"""
    if not current_user.has_permission('manage_settings'):
        return jsonify({'error': 'Unauthorized'}), 403
    existing = GlobalSetting.query.filter_by(key='share_token').first()
    if existing:
        return jsonify({
            'token': existing.value,
            'url': url_for('shared_matrix', token=existing.value, _external=True)
        })
    return jsonify({'token': None, 'url': None})

@app.route('/api/share-token/generate', methods=['POST'])
@login_required
def api_generate_share_token():
    """Generate a new share token for public matrix view"""
    if not current_user.has_permission('manage_settings'):
        return jsonify({'error': 'Unauthorized'}), 403
    token = str(uuid.uuid4())
    existing = GlobalSetting.query.filter_by(key='share_token').first()
    if existing:
        existing.value = token
    else:
        db.session.add(GlobalSetting(key='share_token', value=token))
    db.session.commit()
    return jsonify({
        'success': True,
        'token': token,
        'url': url_for('shared_matrix', token=token, _external=True)
    })

@app.route('/api/share-token', methods=['DELETE'])
@login_required
def api_revoke_share_token():
    """Revoke the current share token"""
    if not current_user.has_permission('manage_settings'):
        return jsonify({'error': 'Unauthorized'}), 403
    existing = GlobalSetting.query.filter_by(key='share_token').first()
    if existing:
        db.session.delete(existing)
        db.session.commit()
    return jsonify({'success': True, 'message': 'Share link revoked'})

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
            'error': 'An internal error occurred'
        }), 500

@app.route('/api/finance/refresh', methods=['POST'])
@login_required
def api_finance_refresh():
    """Refresh financial data from CampMinder Financial API"""
    if not current_user.has_permission('view_finance'):
        return jsonify({'success': False, 'error': 'Access denied'}), 403

    if not is_api_configured():
        return jsonify({
            'success': False,
            'error': 'CampMinder API not configured.'
        }), 400

    try:
        print("Manual finance refresh requested...")
        # Get current enrollment data for cross-referencing
        enrollment_report = api_cache.get('data')
        finance_data = fetch_financial_data(force_refresh=True, enrollment_report=enrollment_report)

        if finance_data:
            return jsonify({
                'success': True,
                'message': 'Financial data refreshed successfully',
                'finance_data': finance_data,
                'fetched_at': finance_cache.get('fetched_at')
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Failed to fetch financial data from API'
            }), 500

    except Exception as e:
        print(f"Finance refresh error: {e}")
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': 'An internal error occurred'
        }), 500

def _load_persons_cache():
    """Load the persons cache — from memory if available, otherwise from disk (once)."""
    global _persons_mem_cache
    if _persons_mem_cache is not None:
        return _persons_mem_cache
    persons_cache_file = os.path.join(DATA_FOLDER, 'persons_cache.json')
    persons_cache = {}
    try:
        if os.path.exists(persons_cache_file):
            with open(persons_cache_file, 'r') as f:
                persons_cache = json.load(f)
            print(f"Persons cache loaded from disk: {len(persons_cache)} entries")
    except Exception:
        pass
    _persons_mem_cache = persons_cache
    return _persons_mem_cache

def _fetch_and_cache_persons(pids_to_fetch, persons_cache):
    """Fetch person details from CampMinder API and update the persons cache.

    Fetches camper data, guardian data (emails, names, phones),
    custom fields (Share Group With), and sibling info.

    Args:
        pids_to_fetch: List of person IDs not yet in cache
        persons_cache: Current cache dict (modified in-place)

    Returns:
        Updated persons_cache dict
    """
    persons_cache_file = os.path.join(DATA_FOLDER, 'persons_cache.json')
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

        # Step 2: Batch fetch all guardians (with contact details for emails/phones,
        #          and relatives to find ward/sibling relationships)
        guardian_map = {}
        if guardian_ids_needed:
            guardian_ids_list = list(guardian_ids_needed)
            print(f"Fetching {len(guardian_ids_list)} guardians via batch API...")
            guardian_results = client.get_persons_batch(
                guardian_ids_list,
                include_contact_details=True,
                include_relatives=True
            )
            for g in guardian_results:
                gid = g.get('ID')
                if gid:
                    guardian_map[gid] = g
            print(f"Got {len(guardian_map)} guardian results from API")

        # Step 3: Load "Share Group With" from uploaded CSV data
        share_group_map = {}  # pid -> value
        sgw_file = os.path.join(DATA_FOLDER, 'share_group.json')
        try:
            if os.path.exists(sgw_file):
                with open(sgw_file, 'r') as f:
                    share_group_map = json.load(f)
                print(f"Loaded {sum(1 for v in share_group_map.values() if v)} "
                      f"Share Group With entries from file")
            else:
                print("No share_group.json file found (upload CSV from CampMinder)")
        except Exception as e:
            print(f"Error loading share_group.json: {e}")

        # Step 3.5: Build sibling map from guardian wards
        # Each guardian's Relatives with IsWard=True are their children
        # Siblings of camper X = all wards of X's guardians, minus X itself
        ward_ids_all = set()  # all child IDs found across all guardians
        guardian_wards = {}  # guardian_id -> list of ward PIDs
        for gid, g_person in guardian_map.items():
            wards = [r['ID'] for r in g_person.get('Relatives', []) if r.get('IsWard')]
            guardian_wards[gid] = wards
            ward_ids_all.update(wards)

        # Fetch ward info (name + DOB) for all wards
        # ward_info: pid -> {'first_name': ..., 'dob': ...}
        ward_info = {}
        # First populate from camper_map (already fetched campers)
        for wid in ward_ids_all:
            if wid in camper_map:
                n = camper_map[wid].get('Name', {})
                ward_info[wid] = {
                    'first_name': n.get('First', ''),
                    'dob': camper_map[wid].get('DateOfBirth', '')
                }
            elif str(wid) in persons_cache:
                info = persons_cache[str(wid)]
                ward_info[wid] = {
                    'first_name': info.get('first_name', ''),
                    'dob': info.get('date_of_birth', '')
                }

        # Fetch remaining wards that we don't have yet (with camper details for DOB)
        unknown_wards = [wid for wid in ward_ids_all if wid not in ward_info]
        if unknown_wards:
            print(f"Fetching info for {len(unknown_wards)} sibling persons...")
            ward_results = client.get_persons_batch(
                unknown_wards,
                include_contact_details=False,
                include_relatives=False,
                include_camper_details=True
            )
            for wp in ward_results:
                wid = wp.get('ID')
                if wid:
                    n = wp.get('Name', {})
                    ward_info[wid] = {
                        'first_name': n.get('First', ''),
                        'dob': wp.get('DateOfBirth', '')
                    }

        # Step 4: Build person_info for each camper
        for pid in pids_to_fetch:
            camper = camper_map.get(pid)
            if camper:
                # Get grade from CampMinder's CamperDetails
                camper_details = camper.get('CamperDetails', {}) or {}
                grade = camper_details.get('CampGradeName', '') or camper_details.get('SchoolGradeName', '')

                # Get guardians from relatives
                relatives = camper.get('Relatives', [])
                guardians = [r for r in relatives if r.get('IsGuardian')]
                guardians.sort(key=lambda r: (not r.get('IsPrimary', False)))

                # Find siblings: collect all wards from this camper's guardians, exclude self
                sibling_list = []  # [{id, first_name, dob}, ...]
                seen_sibling_ids = set()
                for g in guardians:
                    g_id = g.get('ID')
                    for ward_id in guardian_wards.get(g_id, []):
                        if ward_id != pid and ward_id not in seen_sibling_ids:
                            seen_sibling_ids.add(ward_id)
                            wi = ward_info.get(ward_id, {})
                            if wi.get('first_name'):
                                sibling_list.append({
                                    'id': ward_id,
                                    'first_name': wi['first_name'],
                                    'dob': wi.get('dob', '')
                                })

                # Sort siblings by name for display
                sibling_list.sort(key=lambda s: s['first_name'].lower())

                person_info = {
                    'first_name': camper.get('Name', {}).get('First', ''),
                    'last_name': camper.get('Name', {}).get('Last', ''),
                    'f1p1_email': '', 'f1p1_email2': '',
                    'f1p2_email': '', 'f1p2_email2': '',
                    'guardian1_name': '', 'guardian1_phones': '',
                    'guardian2_name': '', 'guardian2_phones': '',
                    'grade': grade,
                    'date_of_birth': camper.get('DateOfBirth', ''),
                    'guardian_ids': [g.get('ID') for g in guardians if g.get('ID')],
                    'share_group_with': share_group_map.get(str(pid), ''),
                    'gender': camper.get('GenderName', ''),
                    'medical_notes': '',
                    'siblings': [s['first_name'] for s in sibling_list],
                    'sibling_details': sibling_list,
                    'aftercare': '',
                    'carpool': ''
                }

                for g_idx, guardian in enumerate(guardians[:2]):
                    g_id = guardian.get('ID')
                    g_person = guardian_map.get(g_id)
                    if g_person:
                        # Guardian emails
                        emails = g_person.get('ContactDetails', {}).get('Emails', [])
                        prefix = 'f1p1' if g_idx == 0 else 'f1p2'
                        if len(emails) > 0:
                            person_info[f'{prefix}_email'] = emails[0].get('Address', '')
                        if len(emails) > 1:
                            person_info[f'{prefix}_email2'] = emails[1].get('Address', '')

                        # Guardian name
                        g_first = g_person.get('Name', {}).get('First', '')
                        g_last = g_person.get('Name', {}).get('Last', '')
                        guardian_name = f"{g_first} {g_last}".strip()

                        # Guardian phone numbers
                        phones = g_person.get('ContactDetails', {}).get('PhoneNumbers', [])
                        phone_numbers = ', '.join(
                            ph.get('Number', '') for ph in phones if ph.get('Number')
                        )

                        g_prefix = 'guardian1' if g_idx == 0 else 'guardian2'
                        person_info[f'{g_prefix}_name'] = guardian_name
                        person_info[f'{g_prefix}_phones'] = phone_numbers

                persons_cache[str(pid)] = person_info
            else:
                persons_cache[str(pid)] = {
                    'first_name': 'Camper', 'last_name': str(pid),
                    'f1p1_email': '', 'f1p1_email2': '',
                    'f1p2_email': '', 'f1p2_email2': '',
                    'guardian1_name': '', 'guardian1_phones': '',
                    'guardian2_name': '', 'guardian2_phones': '',
                    'grade': '',
                    'date_of_birth': '',
                    'guardian_ids': [],
                    'share_group_with': '',
                    'gender': '',
                    'medical_notes': '',
                    'siblings': [],
                    'sibling_details': [],
                    'aftercare': '',
                    'carpool': ''
                }

        # Save updated cache to disk AND update in-memory cache
        global _persons_mem_cache
        _persons_mem_cache = persons_cache
        try:
            with open(persons_cache_file, 'w') as f:
                json.dump(persons_cache, f)
        except Exception:
            pass
        print(f"Persons cache updated: {len(persons_cache)} entries total")
    except Exception as e:
        print(f"Error fetching persons batch: {e}")
        traceback.print_exc()

    return persons_cache

def _load_and_fetch_persons(person_ids):
    """Load persons cache and auto-fetch any missing IDs from CampMinder API."""
    cache = _load_persons_cache()
    to_fetch = [pid for pid in person_ids if pid and str(pid) not in cache]
    if to_fetch and is_api_configured():
        cache = _fetch_and_cache_persons(to_fetch, cache)
    return cache

def _sync_bac_to_cache(persons_cache=None):
    """Sync Before/After Care weeks from CampMinder financial + ECA data into persons_cache.

    Args:
        persons_cache: Existing cache dict. If None, loads from file.

    Returns:
        Updated persons_cache dict with bac_weeks populated.
    """
    import re
    persons_cache_file = os.path.join(DATA_FOLDER, 'persons_cache.json')

    if persons_cache is None:
        persons_cache = _load_persons_cache()

    if not is_api_configured():
        return persons_cache

    try:
        api = CampMinderAPIClient()

        # Part 1: BAC from financial transactions
        print("BAC sync: fetching financial transactions...")
        transactions = api.get_transaction_details(2026)
        bac_persons = {}
        for t in transactions:
            desc = str(t.get('description', ''))
            if 'before and after' not in desc.lower():
                continue
            if t.get('isReversed', False):
                continue
            pid = t.get('personId')
            week_match = re.search(r'Week\s*(\d+)', desc, re.IGNORECASE)
            if pid and week_match:
                bac_persons.setdefault(pid, set()).add(int(week_match.group(1)))

        # Part 2: ECA from session enrollments
        print("BAC sync: fetching ECA session data...")
        attendees = api.get_attendees(2026, api.client_id, status=6)
        eca_session_to_week = {
            1369500: 1, 1369501: 2, 1369502: 3, 1369503: 4,
            1369504: 5, 1369505: 6, 1369506: 7, 1369507: 8, 1369508: 9
        }
        eca_persons = {}
        for att in attendees:
            pid = att.get('PersonID')
            for sps in att.get('SessionProgramStatus', []):
                sid = sps.get('SessionID')
                status_id = sps.get('StatusID')
                if sid in eca_session_to_week and status_id in [2, 4]:
                    eca_persons.setdefault(pid, set()).add(eca_session_to_week[sid])

        # Part 3: Merge into persons_cache
        all_kc_persons = set(bac_persons.keys()) | set(eca_persons.keys())
        for pid in all_kc_persons:
            str_pid = str(pid)
            combined = sorted(bac_persons.get(pid, set()) | eca_persons.get(pid, set()))
            if str_pid in persons_cache:
                persons_cache[str_pid]['bac_weeks'] = combined
            else:
                persons_cache[str_pid] = {'bac_weeks': combined}

        # Save to file and update in-memory cache
        global _persons_mem_cache
        _persons_mem_cache = persons_cache
        try:
            with open(persons_cache_file, 'w') as f:
                json.dump(persons_cache, f)
        except Exception:
            pass

        print(f"BAC sync complete: {len(bac_persons)} financial, {len(eca_persons)} ECA, {len(all_kc_persons)} total KC persons")

    except Exception as e:
        print(f"Error syncing BAC data: {e}")
        traceback.print_exc()

    return persons_cache

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

    # Load persons cache, auto-fetch missing ones from API
    persons_cache = _load_and_fetch_persons(person_ids)

    # Load group assignments for this program/week from DB
    group_map = _get_group_map(program, week)

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

    # Sort: unassigned first, then by group number, then by last name
    enriched.sort(key=lambda x: (
        0 if x['group'] == 0 else 1,
        x['group'],
        x['last_name'].lower(),
        x['first_name'].lower()
    ))

    return jsonify({'participants': enriched})

@app.route('/api/group-assignment/<program>/<int:week>', methods=['POST'])
@login_required
def api_save_group_assignment(program, week):
    """Save a group assignment for a single camper, propagating forward to subsequent weeks"""
    if not current_user.has_permission('edit_groups'):
        return jsonify({'error': 'Unauthorized'}), 403

    data = request.get_json()
    person_id = str(data.get('person_id', ''))
    group = data.get('group')
    propagate = data.get('propagate_forward', True)

    if not person_id:
        return jsonify({'error': 'person_id is required'}), 400

    # Determine which weeks to update
    weeks_to_update = [week]

    if propagate:
        cached_data = api_cache.get('data')
        if cached_data and 'participants' in cached_data:
            program_data = cached_data['participants'].get(program, {})
            enrolled_weeks = []
            for week_str, participants_list in program_data.items():
                w = int(week_str)
                for p in participants_list:
                    if str(p['person_id']) == person_id:
                        enrolled_weeks.append(w)
                        break
            # Only propagate to weeks >= current week where camper is enrolled
            weeks_to_update = sorted([w for w in enrolled_weeks if w >= week])
            if not weeks_to_update:
                weeks_to_update = [week]

    # Apply the group assignment to all target weeks
    updated_weeks = []
    for w in weeks_to_update:
        existing = GroupAssignment.query.filter_by(program=program, week=w, person_id=person_id).first()

        if group is None or group == 0:
            if existing:
                db.session.delete(existing)
            updated_weeks.append(w)
        else:
            if existing:
                existing.group_number = int(group)
            else:
                db.session.add(GroupAssignment(
                    program=program, week=w, person_id=person_id, group_number=int(group)
                ))
            updated_weeks.append(w)

    db.session.commit()

    return jsonify({
        'success': True,
        'key': f"{program}_{week}",
        'person_id': person_id,
        'group': group,
        'updated_weeks': updated_weeks
    })

@app.route('/api/reset-groups/<program>/<int:week>', methods=['POST'])
@login_required
def api_reset_groups(program, week):
    """Reset all group assignments for a given program/week (set all to unassigned)"""
    if not current_user.has_permission('edit_groups'):
        return jsonify({'error': 'Unauthorized'}), 403

    deleted = GroupAssignment.query.filter_by(program=program, week=week).delete()
    db.session.commit()

    return jsonify({
        'success': True,
        'deleted_count': deleted,
        'program': program,
        'week': week
    })

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

    # Load persons cache, auto-fetch missing ones from API
    all_pids = [p['person_id'] for p in participants]
    persons_cache = _load_and_fetch_persons(all_pids)

    # Load group assignments from DB
    group_map = _get_group_map(program, week)

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

def _generate_enrollment_excel(programs):
    """Generate enrollment Excel workbook for given programs.
    Returns (BytesIO output, filename string).
    Raises ValueError if data is unavailable.
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, Border, Side, PatternFill

    # Get participants data from cache
    data = api_cache.get('data')
    if not data or 'participants' not in data:
        raise ValueError('No data available')

    # Collect participants per week across all selected programs
    week_participants = {}
    for program in programs:
        program_data = data['participants'].get(program, {})
        for week_str, participants in program_data.items():
            week_num = int(week_str)
            if week_num not in week_participants:
                week_participants[week_num] = []
            for p in participants:
                week_participants[week_num].append(p['person_id'])

    if not week_participants:
        raise ValueError('No enrollment data found for selected programs')

    # Load persons cache and fetch any missing persons
    persons_cache = _load_persons_cache()

    all_pids = set()
    for pids in week_participants.values():
        all_pids.update(pids)

    pids_to_fetch = [
        pid for pid in all_pids
        if str(pid) not in persons_cache or 'guardian1_name' not in persons_cache.get(str(pid), {})
    ]

    if pids_to_fetch and is_api_configured():
        persons_cache = _fetch_and_cache_persons(pids_to_fetch, persons_cache)

    # Build per-week program lookup for ALL enrolled campers
    all_participants = data.get('participants', {})
    pid_programs_by_week = {}
    for prog_name, prog_weeks in all_participants.items():
        for week_str, plist in prog_weeks.items():
            wk = int(week_str)
            if wk not in pid_programs_by_week:
                pid_programs_by_week[wk] = {}
            for p in plist:
                pid = p['person_id']
                if pid not in pid_programs_by_week[wk]:
                    pid_programs_by_week[wk][pid] = []
                pid_programs_by_week[wk][pid].append(prog_name)

    # Load Share Group With data (from uploaded CSV)
    share_group_data = {}
    sgw_file = os.path.join(DATA_FOLDER, 'share_group.json')
    if os.path.exists(sgw_file):
        try:
            with open(sgw_file, 'r') as f:
                share_group_data = json.load(f)
        except Exception:
            pass

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

    for week_num in sorted(week_participants.keys()):
        person_ids = week_participants[week_num]
        if not person_ids:
            continue

        week_prog_map = pid_programs_by_week.get(week_num, {})

        # Deduplicate by person_id
        seen = set()
        campers = []
        for pid in person_ids:
            if pid in seen:
                continue
            seen.add(pid)
            info = persons_cache.get(str(pid), {})

            # --- Siblings: first names only ---
            sibling_details = info.get('sibling_details', [])
            siblings_raw = info.get('siblings', [])
            if isinstance(siblings_raw, list):
                sibling_first_names = list(siblings_raw)
            else:
                sibling_first_names = [s.strip() for s in str(siblings_raw).split(',') if s.strip()]

            # --- Find youngest sibling ---
            camper_dob = info.get('date_of_birth', '')
            family_members = [{'id': pid, 'first_name': info.get('first_name', ''), 'dob': camper_dob}]
            family_members.extend(sibling_details)

            youngest_id = None
            youngest_name = ''
            youngest_dob = ''
            for member in family_members:
                m_dob = member.get('dob', '')
                if m_dob and (not youngest_dob or m_dob > youngest_dob):
                    youngest_dob = m_dob
                    youngest_id = member.get('id')
                    youngest_name = member.get('first_name', '')

            display_siblings = []
            for sname in sibling_first_names:
                if sname == youngest_name and youngest_id != pid:
                    display_siblings.append(f'*{sname}')
                else:
                    display_siblings.append(sname)

            youngest_program = ''
            if youngest_id and youngest_id != pid:
                progs = week_prog_map.get(youngest_id, [])
                youngest_program = ', '.join(progs) if progs else ''

            campers.append({
                'pid': pid,
                'first_name': info.get('first_name', 'Camper'),
                'last_name': info.get('last_name', ''),
                'grade': info.get('grade', ''),
                'f1p1_email': info.get('f1p1_email', ''),
                'f1p1_email2': info.get('f1p1_email2', ''),
                'f1p2_email': info.get('f1p2_email', ''),
                'f1p2_email2': info.get('f1p2_email2', ''),
                'guardian1_name': info.get('guardian1_name', ''),
                'guardian1_phones': info.get('guardian1_phones', ''),
                'guardian2_name': info.get('guardian2_name', ''),
                'guardian2_phones': info.get('guardian2_phones', ''),
                'siblings': ', '.join(display_siblings),
                'youngest_program': youngest_program,
                'share_group_with': (share_group_data.get(str(pid), '')
                                     or info.get('share_group_with', '')),
            })

        campers.sort(key=lambda c: (c['last_name'].lower(), c['first_name'].lower()))

        ws = wb.create_sheet(title=f'W{week_num}')

        program_list = ', '.join(programs)
        ws.merge_cells('A1:O1')
        ws['A1'] = f'Week {week_num} — {program_list}'
        ws['A1'].font = title_font

        ws['A2'] = f'Total: {len(campers)} campers'
        ws['A2'].font = Font(italic=True, size=10, color='666666')

        headers = ['#', 'Last Name', 'First Name', 'Grade',
                   'Email 1', 'Email 2', 'Email 3', 'Email 4',
                   'Guardian 1', 'Guardian 1 Phone',
                   'Guardian 2', 'Guardian 2 Phone',
                   'Siblings', 'Youngest Sibling Program',
                   'Share Group With']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=4, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = thin_border
            cell.alignment = Alignment(horizontal='center', wrap_text=True)

        for idx, camper in enumerate(campers, 1):
            row = idx + 4
            values = [
                idx, camper['last_name'], camper['first_name'], camper['grade'],
                camper['f1p1_email'], camper['f1p1_email2'],
                camper['f1p2_email'], camper['f1p2_email2'],
                camper['guardian1_name'], camper['guardian1_phones'],
                camper['guardian2_name'], camper['guardian2_phones'],
                camper['siblings'], camper['youngest_program'],
                camper['share_group_with'],
            ]
            for col, val in enumerate(values, 1):
                cell = ws.cell(row=row, column=col, value=val)
                cell.border = thin_border

        ws.column_dimensions['A'].width = 4
        ws.column_dimensions['B'].width = 16
        ws.column_dimensions['C'].width = 16
        ws.column_dimensions['D'].width = 10
        ws.column_dimensions['E'].width = 26
        ws.column_dimensions['F'].width = 26
        ws.column_dimensions['G'].width = 26
        ws.column_dimensions['H'].width = 26
        ws.column_dimensions['I'].width = 22
        ws.column_dimensions['J'].width = 18
        ws.column_dimensions['K'].width = 22
        ws.column_dimensions['L'].width = 18
        ws.column_dimensions['M'].width = 24
        ws.column_dimensions['N'].width = 28
        ws.column_dimensions['O'].width = 30

        ws.page_setup.orientation = 'landscape'
        ws.page_setup.fitToWidth = 1

    if not wb.sheetnames:
        ws = wb.create_sheet(title='No Data')
        ws['A1'] = 'No enrollment data found for the selected programs.'

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    safe_names = '_'.join(p.replace(' ', '')[:15] for p in programs[:3])
    if len(programs) > 3:
        safe_names += f'_and_{len(programs)-3}_more'
    filename = f"Enrollment_{safe_names}_{datetime.now().strftime('%Y%m%d')}.xlsx"

    return output, filename


@app.route('/api/download-multi-program-enrollment', methods=['POST'])
@login_required
def download_multi_program_enrollment():
    """Generate and download enrollment Excel for selected programs."""
    if not current_user.has_permission('download_excel'):
        return jsonify({'error': 'Unauthorized'}), 403

    req_data = request.get_json()
    programs = req_data.get('programs', [])
    if not programs:
        return jsonify({'error': 'No programs selected'}), 400

    try:
        output, filename = _generate_enrollment_excel(programs)
    except ValueError as e:
        return jsonify({'error': str(e)}), 404

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

    all_pids = [p['person_id'] for p in participants]
    persons_cache = _load_and_fetch_persons(all_pids)

    group_map = _get_group_map(program, week)

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

@app.route('/api/upload-po', methods=['POST'])
@login_required
def upload_po():
    """Upload PO Excel file, parse and store spending data for Budget vs Actual."""
    if not current_user.has_permission('view_finance'):
        return jsonify({'error': 'Unauthorized'}), 403

    file = request.files.get('file')
    if not file or not file.filename:
        return jsonify({'error': 'No file provided'}), 400

    if not file.filename.lower().endswith('.xlsx'):
        return jsonify({'error': 'Please upload a .xlsx file'}), 400

    try:
        file_bytes = file.read()
        categories = parse_po_file(file_bytes)
        budget_vs_actual = build_budget_vs_actual(categories)

        po_cache['data'] = {
            'categories': categories,
            'budget_vs_actual': budget_vs_actual,
        }
        po_cache['uploaded_at'] = datetime.now().isoformat()

        # Persist to disk
        os.makedirs('data', exist_ok=True)
        with open(po_cache_path, 'w') as f:
            json.dump(po_cache, f, default=str)

        total_spent = budget_vs_actual['totals']['actual']
        return jsonify({
            'success': True,
            'message': f'PO data loaded: {len(categories)} categories, ${total_spent:,.0f} total spent',
            'data': po_cache['data'],
            'uploaded_at': po_cache['uploaded_at']
        })
    except Exception as e:
        import traceback
        print(f"PO upload error: {traceback.format_exc()}")
        return jsonify({'error': f'Failed to parse PO file: {str(e)}'}), 500

@app.route('/api/upload-share-group', methods=['POST'])
@login_required
def upload_share_group():
    """Upload CampMinder 'Share Group With' CSV export.
    Expected columns: PersonID, Full Name, Share Group With
    Saves to data/share_group.json and updates persons_cache.
    """
    import csv
    import io

    if not current_user.has_permission('upload_csv'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400

    try:
        # Read CSV content (handle BOM and multiline fields)
        raw = file.read().decode('utf-8-sig')
        reader = csv.reader(io.StringIO(raw))
        header = next(reader)

        # Find column indices
        header_lower = [h.strip().lower() for h in header]
        pid_col = None
        sgw_col = None
        for i, h in enumerate(header_lower):
            if h == 'personid':
                pid_col = i
            elif 'share group' in h:
                sgw_col = i

        if pid_col is None or sgw_col is None:
            return jsonify({
                'success': False,
                'error': 'CSV must have "PersonID" and "Share Group With" columns'
            }), 400

        # Parse rows
        share_group_data = {}
        count = 0
        for row in reader:
            if len(row) <= max(pid_col, sgw_col):
                continue
            pid = row[pid_col].strip().strip('"')
            sgw = row[sgw_col].strip().strip('"')
            if pid and pid.isdigit():
                # Normalize: replace newlines with comma-space
                sgw_clean = ', '.join(
                    line.strip() for line in sgw.split('\n') if line.strip()
                )
                share_group_data[pid] = sgw_clean
                if sgw_clean:
                    count += 1

        # Save to JSON file
        sgw_file = os.path.join(DATA_FOLDER, 'share_group.json')
        with open(sgw_file, 'w') as f:
            json.dump(share_group_data, f)

        # Also update persons_cache if it exists
        persons_cache = _load_persons_cache()
        updated = 0
        for pid_str, sgw_val in share_group_data.items():
            if pid_str in persons_cache:
                persons_cache[pid_str]['share_group_with'] = sgw_val
                updated += 1

        if updated > 0:
            persons_cache_file = os.path.join(DATA_FOLDER, 'persons_cache.json')
            with open(persons_cache_file, 'w') as f:
                json.dump(persons_cache, f)

        print(f"Share Group With: loaded {count} entries with data, "
              f"updated {updated} cached persons")

        return jsonify({
            'success': True,
            'message': f'Imported {count} Share Group With entries. '
                       f'Updated {updated} cached campers.'
        })

    except Exception as e:
        print(f"Error processing Share Group CSV: {e}")
        traceback.print_exc()
        return jsonify({'success': False, 'error': 'An internal error occurred'}), 500


@app.route('/upload', methods=['POST'])
@login_required
def upload_file():
    """Handle CSV file upload and processing"""
    print("=" * 50)
    print("UPLOAD REQUEST RECEIVED")
    print("=" * 50)
    
    try:
        if not current_user.has_permission('upload_csv'):
            print("ERROR: User lacks upload_csv permission")
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403
        
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
    if not current_user.has_permission('download_excel'):
        flash('Access denied.', 'error')
        return redirect(url_for('dashboard'))
    if current_report['data'] is None:
        flash('No report data available', 'error')
        return redirect(url_for('dashboard'))
    
    try:
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            summary_data = {
                'Metric': ['Total Enrollment', 'Total Camper Weeks', 'Total FTC', 'Goal', '% to Goal'],
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

# ==================== ATTENDANCE ROUTES ====================

def _ensure_enrollment_cache():
    """Ensure enrollment data is loaded in api_cache (from file if needed).
    Unlike load_api_cache(), this ignores TTL — we always want enrollment data available.
    """
    global api_cache
    if not api_cache.get('data') or not api_cache['data'].get('participants'):
        # Try load_api_cache first (respects TTL)
        cached = load_api_cache()
        if cached and cached.get('data'):
            api_cache['data'] = cached['data']
            api_cache['fetched_at'] = cached.get('fetched_at')
        else:
            # Force-load from file ignoring TTL (for attendance — always need enrollment data)
            if os.path.exists(CACHE_FILE):
                try:
                    with open(CACHE_FILE, 'r') as f:
                        cached = json.load(f)
                    if cached and cached.get('data'):
                        api_cache['data'] = cached['data']
                        api_cache['fetched_at'] = cached.get('fetched_at')
                except Exception as e:
                    print(f"Error force-loading enrollment cache: {e}")

def _check_program_access(program_name):
    """Return 403 response if user lacks program access, else None. Admins always pass."""
    if current_user.role != 'admin':
        assignment = UnitLeaderAssignment.query.filter_by(
            username=current_user.id, program_name=program_name
        ).first()
        if not assignment:
            return jsonify({'error': 'Not authorized for this program'}), 403
    return None

def _get_group_map(program, week):
    """Return dict mapping person_id -> group_number for a program/week."""
    ga_rows = GroupAssignment.query.filter_by(program=program, week=week).all()
    return {ga.person_id: ga.group_number for ga in ga_rows}

def _get_active_checkpoints():
    """Return list of active attendance checkpoints sorted by order."""
    return AttendanceCheckpoint.query.filter_by(active=True).order_by(AttendanceCheckpoint.sort_order).all()

@app.route('/attendance')
@login_required
def attendance_page():
    """Standalone mobile-first attendance page for unit leaders."""
    return render_template('attendance.html')

@app.route('/api/attendance/my-programs')
@login_required
def attendance_my_programs():
    """Return programs assigned to the current unit leader."""
    _ensure_enrollment_cache()
    if current_user.role == 'admin':
        # Admin sees all programs from enrollment data
        programs = []
        if api_cache.get('data') and api_cache['data'].get('participants'):
            programs = sorted(api_cache['data']['participants'].keys())
        return jsonify({'programs': programs})
    # Unit leader: return only assigned programs
    assignments = UnitLeaderAssignment.query.filter_by(username=current_user.id).all()
    programs = [a.program_name for a in assignments]
    return jsonify({'programs': sorted(programs)})

@app.route('/api/attendance/checkpoints')
@login_required
def attendance_checkpoints():
    """Return active checkpoints."""
    checkpoints = _get_active_checkpoints()
    return jsonify({'checkpoints': [
        {'id': c.id, 'name': c.name, 'time_label': c.time_label, 'sort_order': c.sort_order}
        for c in checkpoints
    ]})

@app.route('/api/attendance/campers/<program>/<int:week>')
@login_required
def attendance_campers(program, week):
    """Return camper list for a program/week with attendance for a specific date."""
    # Check access: admin can see all, unit_leader only assigned programs
    denied = _check_program_access(program)
    if denied:
        return denied

    _ensure_enrollment_cache()

    # Parse date from query param (default to today)
    target_date = _parse_date_param()

    # Get campers from enrollment data
    campers = []
    if api_cache.get('data') and api_cache['data'].get('participants'):
        participants = api_cache['data']['participants']
        week_str = str(week)
        if program in participants and week_str in participants[program]:
            campers = participants[program][week_str]

    # Load person names from cache, auto-fetch missing ones from API
    all_pids = [c.get('personId') or c.get('person_id') for c in campers]
    persons_map = _load_and_fetch_persons(all_pids)

    # Auto-sync BAC data if no bac_weeks found (needed for KC buttons)
    has_any_bac = any(
        isinstance(persons_map.get(str(c.get('personId') or c.get('person_id'))), dict) and
        persons_map.get(str(c.get('personId') or c.get('person_id')), {}).get('bac_weeks')
        for c in campers
    ) if campers else False
    if not has_any_bac and campers and is_api_configured():
        print("No bac_weeks found in persons_cache — auto-syncing BAC data...")
        persons_map = _sync_bac_to_cache(persons_map)

    # Get attendance records for specified date + program
    records = AttendanceRecord.query.filter_by(
        program_name=program, date=target_date
    ).all()
    # Build lookup: (person_id, checkpoint_id) → status
    attendance_map = {}
    for r in records:
        attendance_map[(r.person_id, r.checkpoint_id)] = {
            'status': r.status,
            'notes': r.notes,
            'recorded_at': r.recorded_at.isoformat() if r.recorded_at else None
        }

    # Helper to extract name from persons_cache entry (dict with first_name/last_name)
    def _person_name(person_id, fallback=''):
        entry = persons_map.get(str(person_id))
        if isinstance(entry, dict):
            fn = entry.get('first_name', '')
            ln = entry.get('last_name', '')
            return f'{fn} {ln}'.strip() or fallback
        elif isinstance(entry, str):
            return entry or fallback
        return fallback

    # Determine KC (Kid Connection / Before & After Care) eligibility per camper
    # Primary source: 'bac_weeks' list in persons_cache (synced from CampMinder financial API + ECA sessions)
    # Fallback: legacy fields for backward compatibility

    def _has_kc(person_id):
        entry = persons_map.get(str(person_id))
        if not isinstance(entry, dict):
            return False
        # Primary: check bac_weeks list (populated by /api/attendance/sync-bac)
        bac_weeks = entry.get('bac_weeks')
        if isinstance(bac_weeks, list) and week in bac_weeks:
            return True
        return False

    # Build reverse lookup: all enrolled person_ids by program+week for sibling matching
    enrolled_by_program = {}  # {program_name: set_of_person_ids}
    if api_cache.get('data') and api_cache['data'].get('participants'):
        all_participants = api_cache['data']['participants']
        for prog_name, weeks_data in all_participants.items():
            if str(week) in weeks_data:
                for c in weeks_data[str(week)]:
                    pid = str(c.get('personId') or c.get('person_id', ''))
                    if pid:
                        enrolled_by_program.setdefault(prog_name, set()).add(pid)
    def _find_youngest_enrolled_sibling(person_id):
        """Find the youngest sibling (younger than camper) enrolled this week."""
        entry = persons_map.get(str(person_id))
        if not isinstance(entry, dict):
            return None
        sibling_details = entry.get('sibling_details')
        if not sibling_details or not isinstance(sibling_details, list):
            return None
        # Get camper's own DOB to compare
        camper_dob = entry.get('date_of_birth', '')
        # Find siblings that are enrolled this week in any program
        enrolled_siblings = []
        for sib in sibling_details:
            sib_id = str(sib.get('id', ''))
            if not sib_id:
                continue
            sib_dob = sib.get('dob', '')
            # Only include siblings younger than the camper (more recent DOB)
            if camper_dob and sib_dob and sib_dob <= camper_dob:
                continue  # Sibling is same age or older, skip
            # Check if this sibling is enrolled in any program this week
            for prog_name, enrolled_ids in enrolled_by_program.items():
                if sib_id in enrolled_ids:
                    sib_name = _person_name(sib_id, sib.get('first_name', ''))
                    enrolled_siblings.append({
                        'id': sib_id,
                        'name': sib_name,
                        'program': prog_name,
                        'dob': sib_dob
                    })
                    break  # Found one program for this sibling, enough
        if not enrolled_siblings:
            return None
        # Return the youngest (most recent dob)
        enrolled_siblings.sort(key=lambda s: s.get('dob', ''), reverse=True)
        youngest = enrolled_siblings[0]
        return {'name': youngest['name'], 'program': youngest['program']}

    # Load group assignments for sorting by group
    group_map = _get_group_map(program, week)

    # Build camper list with attendance data
    camper_list = []
    for camper in campers:
        person_id = camper.get('personId') or camper.get('person_id', '')
        name = _person_name(person_id, camper.get('name', f'Camper {person_id}'))
        camper_data = {
            'person_id': str(person_id),
            'name': name,
            'has_kc': _has_kc(person_id),
            'group_number': group_map.get(str(person_id), 0),
            'attendance': {}
        }
        # Fill in attendance per checkpoint
        for key, val in attendance_map.items():
            if key[0] == str(person_id):
                camper_data['attendance'][str(key[1])] = val
        # Youngest enrolled sibling
        sib = _find_youngest_enrolled_sibling(person_id)
        if sib:
            camper_data['youngest_sibling'] = sib
        camper_list.append(camper_data)

    # Sort by last name
    camper_list.sort(key=lambda c: c['name'].split()[-1] if c['name'] else '')

    return jsonify({
        'program': program,
        'week': week,
        'date': target_date.isoformat(),
        'campers': camper_list,
        'total': len(camper_list)
    })

@app.route('/api/attendance/record', methods=['POST'])
@login_required
def attendance_record():
    """Save a single attendance record (debounced from UI)."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    person_id = data.get('person_id')
    program_name = data.get('program_name')
    checkpoint_id = data.get('checkpoint_id')
    status = data.get('status', 'present')
    notes = data.get('notes', '')

    if not all([person_id, program_name, checkpoint_id]):
        return jsonify({'error': 'Missing required fields'}), 400

    if status not in ('present', 'absent', 'late', 'early_pickup', 'unmarked'):
        return jsonify({'error': 'Invalid status'}), 400

    # Check access
    denied = _check_program_access(program_name)
    if denied:
        return denied

    # Parse target date from request (default to today)
    target_date = _parse_date_param(source=data)

    # Server-side 5 PM lock enforcement (admins bypass)
    if current_user.role != 'admin':
        now = datetime.now()
        today = date.today()
        if target_date < today:
            return jsonify({'error': 'Cannot modify attendance for past days'}), 403
        if target_date == today and now.hour >= ATTENDANCE_LOCK_HOUR:
            return jsonify({'error': 'Day is locked after 5:00 PM'}), 403

    # Handle unmark: delete the record entirely
    if status == 'unmarked':
        existing = AttendanceRecord.query.filter_by(
            person_id=str(person_id),
            program_name=program_name,
            date=target_date,
            checkpoint_id=int(checkpoint_id)
        ).first()
        if existing:
            db.session.delete(existing)
            db.session.commit()
        return jsonify({'success': True, 'action': 'deleted'})

    current_week = get_current_camp_week(target_date)
    if current_week is None:
        # Allow recording even outside camp weeks (for testing/flexibility)
        current_week = 0

    # Upsert: update if exists, insert if not
    existing = AttendanceRecord.query.filter_by(
        person_id=str(person_id),
        program_name=program_name,
        date=target_date,
        checkpoint_id=int(checkpoint_id)
    ).first()

    if existing:
        existing.status = status
        existing.notes = notes
        existing.recorded_by = current_user.id
        existing.recorded_at = datetime.utcnow()
    else:
        record = AttendanceRecord(
            person_id=str(person_id),
            program_name=program_name,
            week=current_week,
            date=target_date,
            checkpoint_id=int(checkpoint_id),
            status=status,
            recorded_by=current_user.id,
            notes=notes
        )
        db.session.add(record)

    try:
        db.session.commit()
        return jsonify({'success': True, 'status': status})
    except Exception as e:
        db.session.rollback()
        traceback.print_exc()
        return jsonify({'error': 'An internal error occurred'}), 500

@app.route('/api/attendance/record-batch', methods=['POST'])
@login_required
def attendance_record_batch():
    """Batch save attendance records (Mark All Present)."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    program_name = data.get('program_name')
    checkpoint_id = data.get('checkpoint_id')
    status = data.get('status', 'present')
    person_ids = data.get('person_ids', [])

    if not all([program_name, checkpoint_id, person_ids]):
        return jsonify({'error': 'Missing required fields'}), 400

    # Check access
    denied = _check_program_access(program_name)
    if denied:
        return denied

    # Parse target date from request (default to today)
    target_date = _parse_date_param(source=data)

    # Server-side 5 PM lock enforcement (admins bypass)
    if current_user.role != 'admin':
        now = datetime.now()
        today = date.today()
        if target_date < today:
            return jsonify({'error': 'Cannot modify attendance for past days'}), 403
        if target_date == today and now.hour >= ATTENDANCE_LOCK_HOUR:
            return jsonify({'error': 'Day is locked after 5:00 PM'}), 403

    # Handle unmark: delete all matching records
    if status == 'unmarked':
        deleted = 0
        for pid in person_ids:
            existing = AttendanceRecord.query.filter_by(
                person_id=str(pid),
                program_name=program_name,
                date=target_date,
                checkpoint_id=int(checkpoint_id)
            ).first()
            if existing:
                db.session.delete(existing)
                deleted += 1
        db.session.commit()
        return jsonify({'success': True, 'count': deleted, 'action': 'deleted'})

    current_week = get_current_camp_week(target_date) or 0

    count = 0
    for pid in person_ids:
        existing = AttendanceRecord.query.filter_by(
            person_id=str(pid),
            program_name=program_name,
            date=target_date,
            checkpoint_id=int(checkpoint_id)
        ).first()
        if existing:
            existing.status = status
            existing.recorded_by = current_user.id
            existing.recorded_at = datetime.utcnow()
        else:
            db.session.add(AttendanceRecord(
                person_id=str(pid),
                program_name=program_name,
                week=current_week,
                date=target_date,
                checkpoint_id=int(checkpoint_id),
                status=status,
                recorded_by=current_user.id
            ))
        count += 1

    try:
        db.session.commit()
        return jsonify({'success': True, 'count': count})
    except Exception as e:
        db.session.rollback()
        traceback.print_exc()
        return jsonify({'error': 'An internal error occurred'}), 500

@app.route('/api/attendance/summary')
@login_required
def attendance_summary():
    """Admin: aggregated attendance stats for all programs on a given date."""
    _ensure_enrollment_cache()
    target_date = _parse_date_param()

    # Get all records for the date
    records = AttendanceRecord.query.filter_by(date=target_date).all()
    checkpoints = _get_active_checkpoints()

    # Get all programs from enrollment data
    all_programs = []
    total_campers_by_program = {}
    if api_cache.get('data') and api_cache['data'].get('participants'):
        participants = api_cache['data']['participants']
        # Figure out which week this date falls in
        week_num = get_current_camp_week(target_date)
        if week_num:
            week_str = str(week_num)
            for prog_name, weeks in participants.items():
                if week_str in weeks and len(weeks[week_str]) > 0:
                    all_programs.append(prog_name)
                    total_campers_by_program[prog_name] = len(weeks[week_str])

    # Build summary: program × checkpoint → {present, absent, late, early_pickup, total}
    summary = {}
    totals = {'present': 0, 'absent': 0, 'late': 0, 'early_pickup': 0}
    for r in records:
        key = (r.program_name, r.checkpoint_id)
        if key not in summary:
            summary[key] = {'present': 0, 'absent': 0, 'late': 0, 'early_pickup': 0, 'marked': 0}
        summary[key][r.status] = summary[key].get(r.status, 0) + 1
        summary[key]['marked'] += 1
        # Only count checkpoint 1 (daily attendance) in KPI totals to avoid double-counting KC
        if r.checkpoint_id == 1:
            totals[r.status] = totals.get(r.status, 0) + 1

    # Format response
    programs_data = []
    for prog in sorted(all_programs):
        total_campers = total_campers_by_program.get(prog, 0)
        prog_entry = {
            'program': prog,
            'total_campers': total_campers,
            'checkpoints': []
        }
        for cp in checkpoints:
            key = (prog, cp.id)
            stats = summary.get(key, {'present': 0, 'absent': 0, 'late': 0, 'early_pickup': 0, 'marked': 0})
            stats['checkpoint_id'] = cp.id
            stats['checkpoint_name'] = cp.name
            stats['total'] = total_campers
            stats['completion'] = round(stats['marked'] / total_campers * 100) if total_campers > 0 else 0
            prog_entry['checkpoints'].append(stats)
        programs_data.append(prog_entry)

    return jsonify({
        'date': target_date.isoformat(),
        'week': get_current_camp_week(target_date),
        'totals': totals,
        'total_campers': sum(total_campers_by_program.values()),
        'programs': programs_data,
        'checkpoints': [{'id': c.id, 'name': c.name, 'time_label': c.time_label} for c in checkpoints]
    })

@app.route('/api/attendance/trends')
@login_required
def attendance_trends():
    """Admin: attendance trend data across all programs, grouped by date."""
    _ensure_enrollment_cache()
    start_str = request.args.get('start')
    end_str = request.args.get('end')
    today = date.today()

    # Defaults: current camp week, or last 7 days
    if not start_str or not end_str:
        cw = get_current_camp_week(today)
        if cw and cw in CAMP_WEEK_DATES:
            start_date = date.fromisoformat(CAMP_WEEK_DATES[cw][0])
            end_date = today
        else:
            start_date = today - timedelta(days=6)
            end_date = today
    else:
        try:
            start_date = date.fromisoformat(start_str)
            end_date = date.fromisoformat(end_str)
        except ValueError:
            start_date = today - timedelta(days=6)
            end_date = today

    # Query checkpoint_id=1 (Morning) only to avoid double-counting KC
    records = AttendanceRecord.query.filter(
        AttendanceRecord.checkpoint_id == 1,
        AttendanceRecord.date >= start_date,
        AttendanceRecord.date <= end_date
    ).all()

    # Group by date
    by_date = {}
    for r in records:
        d = r.date.isoformat()
        if d not in by_date:
            by_date[d] = {'present': 0, 'absent': 0, 'late': 0, 'early_pickup': 0}
        if r.status in by_date[d]:
            by_date[d][r.status] += 1

    # Calculate total enrolled per date (sum across all programs for that date's week)
    participants = {}
    if api_cache.get('data') and api_cache['data'].get('participants'):
        participants = api_cache['data']['participants']

    dates_result = []
    current = start_date
    while current <= end_date:
        # Skip weekends
        if current.weekday() < 5:
            d_str = current.isoformat()
            counts = by_date.get(d_str, {'present': 0, 'absent': 0, 'late': 0, 'early_pickup': 0})
            # Calculate total enrolled for this date's week
            wk = get_current_camp_week(current)
            total_enrolled = 0
            if wk and participants:
                wk_str = str(wk)
                for prog_name, weeks in participants.items():
                    if wk_str in weeks:
                        total_enrolled += len(weeks[wk_str])
            attended = counts['present'] + counts['late']
            rate = round(attended / total_enrolled * 100, 1) if total_enrolled > 0 else 0
            dates_result.append({
                'date': d_str,
                'present': counts['present'],
                'absent': counts['absent'],
                'late': counts['late'],
                'early_pickup': counts['early_pickup'],
                'total_enrolled': total_enrolled,
                'rate': rate
            })
        current += timedelta(days=1)

    return jsonify({
        'start': start_date.isoformat(),
        'end': end_date.isoformat(),
        'dates': dates_result
    })

@app.route('/api/attendance/detail/<program>')
@login_required
def attendance_detail(program):
    """Admin: individual camper attendance for a specific program on a date."""
    _ensure_enrollment_cache()
    target_date = _parse_date_param()

    # Get campers from enrollment
    week_num = get_current_camp_week(target_date)
    campers = []
    if api_cache.get('data') and api_cache['data'].get('participants'):
        participants = api_cache['data']['participants']
        if week_num:
            week_str = str(week_num)
            if program in participants and week_str in participants[program]:
                campers = participants[program][week_str]

    # Load person names from cache, auto-fetch missing ones from API
    all_pids = [c.get('personId') or c.get('person_id') for c in campers]
    persons_map = _load_and_fetch_persons(all_pids)

    # Get records
    records = AttendanceRecord.query.filter_by(
        program_name=program, date=target_date
    ).all()
    att_map = {}
    for r in records:
        if r.person_id not in att_map:
            att_map[r.person_id] = {}
        att_map[r.person_id][str(r.checkpoint_id)] = {
            'status': r.status,
            'notes': r.notes,
            'recorded_by': r.recorded_by,
            'recorded_at': r.recorded_at.isoformat() if r.recorded_at else None
        }

    # Helper to extract name from persons_cache entry
    def _person_name(person_id, fallback=''):
        entry = persons_map.get(str(person_id))
        if isinstance(entry, dict):
            fn = entry.get('first_name', '')
            ln = entry.get('last_name', '')
            return f'{fn} {ln}'.strip() or fallback
        elif isinstance(entry, str):
            return entry or fallback
        return fallback

    camper_list = []
    for camper in campers:
        pid = str(camper.get('personId') or camper.get('person_id', ''))
        name = _person_name(pid, camper.get('name', f'Camper {pid}'))
        camper_list.append({
            'person_id': pid,
            'name': name,
            'attendance': att_map.get(pid, {})
        })
    camper_list.sort(key=lambda c: c['name'].split()[-1] if c['name'] else '')

    checkpoints = _get_active_checkpoints()

    return jsonify({
        'program': program,
        'date': target_date.isoformat(),
        'week': week_num,
        'campers': camper_list,
        'checkpoints': [{'id': c.id, 'name': c.name, 'time_label': c.time_label} for c in checkpoints]
    })

# ---- Admin Management: Unit Leader Assignments ----

@app.route('/api/attendance/assignments', methods=['GET'])
@login_required
def get_assignments():
    """Admin: list all unit leader → program assignments."""
    if current_user.role != 'admin':
        return jsonify({'error': 'Admin only'}), 403
    assignments = UnitLeaderAssignment.query.all()
    # Group by username
    by_user = {}
    for a in assignments:
        if a.username not in by_user:
            by_user[a.username] = []
        by_user[a.username].append(a.program_name)
    return jsonify({'assignments': by_user})

@app.route('/api/attendance/assignments', methods=['POST'])
@login_required
def save_assignment():
    """Admin: assign a program to a unit leader."""
    if current_user.role != 'admin':
        return jsonify({'error': 'Admin only'}), 403
    data = request.get_json()
    username = data.get('username')
    program_name = data.get('program_name')
    if not username or not program_name:
        return jsonify({'error': 'Missing username or program_name'}), 400
    # Check user exists and is unit_leader
    user_acc = UserAccount.query.filter_by(username=username).first()
    if not user_acc:
        return jsonify({'error': 'User not found'}), 404
    # Check if already assigned
    existing = UnitLeaderAssignment.query.filter_by(username=username, program_name=program_name).first()
    if existing:
        return jsonify({'message': 'Already assigned'}), 200
    db.session.add(UnitLeaderAssignment(username=username, program_name=program_name))
    db.session.commit()
    return jsonify({'success': True})

@app.route('/api/attendance/assignments', methods=['DELETE'])
@login_required
def delete_assignment():
    """Admin: remove a program assignment from a unit leader."""
    if current_user.role != 'admin':
        return jsonify({'error': 'Admin only'}), 403
    data = request.get_json()
    username = data.get('username')
    program_name = data.get('program_name')
    a = UnitLeaderAssignment.query.filter_by(username=username, program_name=program_name).first()
    if a:
        db.session.delete(a)
        db.session.commit()
    return jsonify({'success': True})

# ---- Admin Management: Checkpoints ----

@app.route('/api/attendance/checkpoints', methods=['PUT'])
@login_required
def update_checkpoint():
    """Admin: update a checkpoint's name, time_label, or active status."""
    if current_user.role != 'admin':
        return jsonify({'error': 'Admin only'}), 403
    data = request.get_json()
    cp_id = data.get('id')
    cp = db.session.get(AttendanceCheckpoint, cp_id)
    if not cp:
        return jsonify({'error': 'Checkpoint not found'}), 404
    if 'name' in data:
        cp.name = data['name']
    if 'time_label' in data:
        cp.time_label = data['time_label']
    if 'active' in data:
        cp.active = bool(data['active'])
    if 'sort_order' in data:
        cp.sort_order = int(data['sort_order'])
    db.session.commit()
    return jsonify({'success': True})

@app.route('/api/attendance/week-info')
@login_required
def attendance_week_info():
    """Return current camp week info and all week date ranges."""
    today = date.today()
    current_week = get_current_camp_week(today)
    return jsonify({
        'today': today.isoformat(),
        'current_week': current_week,
        'is_camp_day': is_camp_day(today),
        'weeks': {str(k): {'start': v[0], 'end': v[1]} for k, v in CAMP_WEEK_DATES.items()}
    })

@app.route('/api/attendance/sync-bac', methods=['POST'])
@login_required
def sync_bac_data():
    """Sync Before and After Care data from CampMinder financial transactions + ECA sessions.
    Delegates to _sync_bac_to_cache() helper."""
    if current_user.role != 'admin':
        return jsonify({'error': 'Admin only'}), 403
    if not CAMPMINDER_API_AVAILABLE:
        return jsonify({'error': 'CampMinder API not available'}), 500

    try:
        persons = _sync_bac_to_cache()
        kc_count = sum(1 for p in persons.values()
                       if isinstance(p, dict) and p.get('bac_weeks'))
        return jsonify({'success': True, 'total_kc_persons': kc_count})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': 'An internal error occurred'}), 500

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
    app.run(host='0.0.0.0', port=port, debug=os.environ.get('DEBUG', 'False') == 'True', threaded=True)
