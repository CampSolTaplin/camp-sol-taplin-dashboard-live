# Camp Sol Taplin - Live Enrollment Dashboard

Real-time enrollment dashboard for Camp Sol Taplin with CampMinder API integration.

## Features

- 🔴 **Live Data** - Real-time enrollment data from CampMinder API
- 📊 **Executive Dashboard** - KPIs, progress tracking, and pace comparison
- 📅 **Registration Timeline** - Filter by date, compare years, overlay charts
- 📈 **Year Comparison** - Compare 2024, 2025, and 2026 enrollment trends
- 📋 **Detailed View** - Program-by-week enrollment matrix
- 👥 **User Management** - Admin panel for user accounts
- 🔄 **Manual Refresh** - Button to force data refresh from API

## Quick Deploy to Render

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy)

### Manual Setup

1. Create a new **Web Service** on Render
2. Connect your GitHub repository
3. Configure:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn app:app`

4. Add **Environment Variables**:
   | Variable | Value |
   |----------|-------|
   | `CAMPMINDER_API_KEY` | Your CampMinder API Key |
   | `CAMPMINDER_SUBSCRIPTION_KEY` | Your Azure Subscription Key |
   | `CAMPMINDER_SEASON_ID` | `2026` |
   | `SECRET_KEY` | Any random string for session security |

5. Deploy!

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `CAMPMINDER_API_KEY` | Yes | CampMinder API authentication key |
| `CAMPMINDER_SUBSCRIPTION_KEY` | Yes | Azure API Management subscription key |
| `CAMPMINDER_SEASON_ID` | No | Season year (default: 2026) |
| `SECRET_KEY` | No | Flask secret key for sessions |

## Default Login

- **Username**: `admin`
- **Password**: `CampSol2026!`

⚠️ Change this password after first login!

## Project Structure

```
├── app.py                 # Main Flask application
├── campminder_api.py      # CampMinder API client
├── parser.py              # CSV parser (fallback)
├── historical_data.py     # Historical data manager
├── requirements.txt       # Python dependencies
├── templates/
│   ├── dashboard.html     # Main dashboard template
│   ├── admin_users.html   # User management page
│   ├── login.html         # Login page
│   ├── 404.html           # Error page
│   └── 500.html           # Error page
├── static/
│   ├── css/
│   │   └── dashboard.css  # Styles
│   ├── js/
│   │   └── dashboard.js   # JavaScript
│   └── images/
│       └── logo.png       # Camp logo
└── data/
    └── historical_enrollment.json  # 2024/2025 historical data
```

## API Integration

The dashboard connects to CampMinder's API to fetch:
- Sessions (weeks)
- Programs
- Attendees with enrollment status

### Session Name Mapping

The system automatically maps session names to weeks:
- `"Week 1"`, `"ECA Week 1"` → Week 1
- `"Weeks 2-5"` → Weeks 2, 3, 4, 5
- `"Full Session"` → Weeks 1-4
- `"Theater Camp Weeks 6-9"` → Weeks 6, 7, 8, 9

## Data Caching

- API data is cached for 15 minutes
- Manual refresh available via button
- Falls back to CSV upload if API unavailable

## License

Private - Camp Sol Taplin / Michael-Ann Russell JCC
