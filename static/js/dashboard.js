// Camp Sol Taplin Dashboard JavaScript

// Global chart instances
let cumulativeChartInstance = null;
let comparisonChartInstance = null;

// View Switching
function switchView(viewName) {
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.remove('active');
        if (item.dataset.view === viewName) {
            item.classList.add('active');
        }
    });
    
    document.querySelectorAll('.view-content').forEach(content => content.classList.remove('active'));
    
    const viewElement = document.getElementById(viewName + '-view');
    if (viewElement) {
        viewElement.classList.add('active');
    }
    
    // Initialize charts when switching views
    if (viewName === 'bydate') {
        setTimeout(initCumulativeChart, 100);
    } else if (viewName === 'comparison') {
        setTimeout(initComparisonChart, 100);
    }
}

// Filter by category
function filterByCategory(category) {
    switchView('detailed');
    const categoryFilter = document.getElementById('categoryFilter');
    if (categoryFilter) {
        categoryFilter.value = category;
        filterDetailedView();
    }
}

// Filter detailed view
function filterDetailedView() {
    const categoryFilter = document.getElementById('categoryFilter');
    const table = document.getElementById('enrollmentMatrix');
    if (!table) return;
    
    const filterValue = categoryFilter ? categoryFilter.value : 'all';
    const rows = table.querySelectorAll('tbody tr');
    
    rows.forEach(row => {
        const rowCategory = row.dataset.category || '';
        row.style.display = (filterValue === 'all' || rowCategory === filterValue) ? '' : 'none';
    });
}

// Show participants modal
function showParticipants(program, week) {
    const modal = document.getElementById('participantsModal');
    const title = document.getElementById('participantsTitle');
    const list = document.getElementById('participantsList');
    
    if (!modal || !window.participantsData) return;
    
    const programData = window.participantsData[program];
    const participants = programData ? (programData[String(week)] || []) : [];
    
    title.textContent = '👥 ' + program + ' - Week ' + week;
    
    if (participants.length === 0) {
        list.innerHTML = '<div class="participant-count">No participants found</div>';
    } else {
        let html = '<div class="participant-count">' + participants.length + ' participant' + (participants.length !== 1 ? 's' : '') + '</div>';
        html += '<div class="participants-items">';
        participants.forEach(function(p, index) {
            html += '<div class="participant-item">';
            html += '<div class="participant-name">' + (index + 1) + '. ' + p.first_name + ' ' + p.last_name + '</div>';
            html += '<div class="participant-date">' + (p.enrollment_date || 'N/A') + '</div>';
            html += '</div>';
        });
        html += '</div>';
        list.innerHTML = html;
    }
    
    modal.classList.add('show');
}

function closeParticipantsModal() {
    const modal = document.getElementById('participantsModal');
    if (modal) modal.classList.remove('show');
}

// Date Chart Functions
function getDateStatsForYear(year) {
    if (year === '2026' || year === 2026) return window.dateStats2026 || [];
    if (year === '2025' || year === 2025) return window.dateStats2025 || [];
    if (year === '2024' || year === 2024) return window.dateStats2024 || [];
    return [];
}

function filterDateStats(data, startDate, endDate) {
    if (!data) return [];
    let filtered = data;
    if (startDate) {
        filtered = filtered.filter(d => d.date >= startDate);
    }
    if (endDate) {
        filtered = filtered.filter(d => d.date <= endDate);
    }
    return filtered;
}

function updateDateChart() {
    const yearSelect = document.getElementById('yearSelect');
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    
    const year = yearSelect ? yearSelect.value : '2026';
    let data = getDateStatsForYear(year);
    data = filterDateStats(data, startDate, endDate);
    
    // Update chart
    if (cumulativeChartInstance) {
        cumulativeChartInstance.data.labels = data.map(d => d.date);
        cumulativeChartInstance.data.datasets[0].data = data.map(d => d.cumulative_weeks);
        cumulativeChartInstance.data.datasets[0].label = 'Cumulative Weeks ' + year;
        cumulativeChartInstance.update();
    }
    
    // Update summary
    if (data.length > 0) {
        const last = data[data.length - 1];
        const totalCampers = document.getElementById('totalCampersDisplay');
        const totalWeeks = document.getElementById('totalWeeksDisplay');
        const daysCount = document.getElementById('daysCountDisplay');
        
        if (totalCampers) totalCampers.textContent = last.cumulative_campers;
        if (totalWeeks) totalWeeks.textContent = last.cumulative_weeks;
        if (daysCount) daysCount.textContent = data.length;
    }
    
    // Update table
    updateDailyTable(data);
    
    // Update date range label
    const label = document.getElementById('dateRangeLabel');
    if (label && startDate && endDate) {
        label.textContent = '(' + startDate + ' to ' + endDate + ')';
    } else if (label) {
        label.textContent = '';
    }
}

function updateDailyTable(data) {
    const tbody = document.getElementById('dailyTableBody');
    if (!tbody) return;
    
    let html = '';
    const reversedData = [...data].reverse().slice(0, 50);
    
    reversedData.forEach(day => {
        html += '<tr>';
        html += '<td>' + day.date + '</td>';
        html += '<td>' + day.new_registrations + '</td>';
        html += '<td>' + day.camper_weeks_added + '</td>';
        html += '<td>' + day.cumulative_campers + '</td>';
        html += '<td>' + day.cumulative_weeks + '</td>';
        html += '</tr>';
    });
    
    tbody.innerHTML = html;
}

function resetDateFilters() {
    document.getElementById('yearSelect').value = '2026';
    document.getElementById('startDate').value = '';
    document.getElementById('endDate').value = '';
    updateDateChart();
}

function initCumulativeChart() {
    const canvas = document.getElementById('cumulativeChart');
    if (!canvas) return;
    
    const data = window.dateStats2026 || [];
    if (data.length === 0) return;
    
    const ctx = canvas.getContext('2d');
    
    if (cumulativeChartInstance) {
        cumulativeChartInstance.destroy();
    }
    
    cumulativeChartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => d.date),
            datasets: [{
                label: 'Cumulative Weeks 2026',
                data: data.map(d => d.cumulative_weeks),
                borderColor: '#00A9CE',
                backgroundColor: 'rgba(0, 169, 206, 0.1)',
                borderWidth: 3,
                fill: true,
                tension: 0.3,
                pointRadius: 3,
                pointBackgroundColor: '#00A9CE'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true, position: 'top' },
                tooltip: {
                    callbacks: {
                        title: ctx => 'Date: ' + ctx[0].label,
                        label: ctx => 'Cumulative Weeks: ' + ctx.parsed.y.toLocaleString()
                    }
                }
            },
            scales: {
                x: {
                    title: { display: true, text: 'Registration Date', font: { weight: 'bold' } },
                    ticks: { maxRotation: 45, maxTicksLimit: 15 }
                },
                y: {
                    title: { display: true, text: 'Cumulative Camper Weeks', font: { weight: 'bold' } },
                    beginAtZero: true
                }
            }
        }
    });
}

function initComparisonChart() {
    const canvas = document.getElementById('comparisonChart');
    if (!canvas || !window.comparisonChartData) return;
    
    const ctx = canvas.getContext('2d');
    const data = window.comparisonChartData;
    
    if (comparisonChartInstance) {
        comparisonChartInstance.destroy();
    }
    
    const datasets = [];
    
    if (data['2024'] && data['2024'].length > 0) {
        datasets.push({
            label: '2024',
            data: data['2024'],
            borderColor: '#9E9E9E',
            backgroundColor: 'transparent',
            borderWidth: 2,
            borderDash: [5, 5],
            tension: 0.3,
            pointRadius: 0
        });
    }
    
    if (data['2025'] && data['2025'].length > 0) {
        datasets.push({
            label: '2025',
            data: data['2025'],
            borderColor: '#7CB342',
            backgroundColor: 'transparent',
            borderWidth: 3,
            tension: 0.3,
            pointRadius: 0
        });
    }
    
    // Add 2026 if we have current data
    if (window.dateStats2026 && window.dateStats2026.length > 0) {
        const data2026 = [];
        const labels = data.labels || [];
        
        for (let i = 0; i < labels.length; i++) {
            const daysOffset = labels[i];
            let cumulative = 0;
            
            for (const day of window.dateStats2026) {
                const dayDate = new Date(day.date);
                const yearStart = new Date(dayDate.getFullYear(), 0, 1);
                const daysDiff = Math.floor((dayDate - yearStart) / (1000 * 60 * 60 * 24));
                
                if (daysDiff <= daysOffset) {
                    cumulative = day.cumulative_weeks;
                } else {
                    break;
                }
            }
            data2026.push(cumulative || null);
        }
        
        datasets.push({
            label: '2026 (Current)',
            data: data2026,
            borderColor: '#00A9CE',
            backgroundColor: 'rgba(0, 169, 206, 0.1)',
            borderWidth: 4,
            fill: true,
            tension: 0.3,
            pointRadius: 0
        });
    }
    
    comparisonChartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: (data.labels || []).map(d => 'Day ' + d),
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true, position: 'top' },
                title: {
                    display: true,
                    text: 'Cumulative Camper Weeks by Days from January 1',
                    font: { size: 16 }
                }
            },
            scales: {
                x: {
                    title: { display: true, text: 'Days from January 1', font: { weight: 'bold' } },
                    ticks: { maxTicksLimit: 20 }
                },
                y: {
                    title: { display: true, text: 'Cumulative Camper Weeks', font: { weight: 'bold' } },
                    beginAtZero: true
                }
            }
        }
    });
}

// Upload Functions
function openUploadModal() {
    const modal = document.getElementById('uploadModal');
    if (modal) modal.classList.add('show');
}

function closeUploadModal() {
    const modal = document.getElementById('uploadModal');
    if (modal) {
        modal.classList.remove('show');
        clearFile();
    }
}

function clearFile() {
    const fileInput = document.getElementById('fileInput');
    const fileInfo = document.getElementById('fileInfo');
    const uploadSubmit = document.getElementById('uploadSubmit');
    
    if (fileInput) fileInput.value = '';
    if (fileInfo) fileInfo.style.display = 'none';
    if (uploadSubmit) uploadSubmit.disabled = true;
}

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    initUpload();
    initModalCloseHandlers();
    
    // Init chart if bydate view is active
    setTimeout(function() {
        if (document.getElementById('bydate-view') && 
            document.getElementById('bydate-view').classList.contains('active')) {
            initCumulativeChart();
        }
    }, 200);
});

function initUpload() {
    const uploadBtn = document.getElementById('uploadBtn');
    const uploadZone = document.getElementById('uploadZone');
    const fileInput = document.getElementById('fileInput');
    const fileInfo = document.getElementById('fileInfo');
    const fileName = document.getElementById('fileName');
    const uploadForm = document.getElementById('uploadForm');
    const uploadSubmit = document.getElementById('uploadSubmit');
    
    if (!uploadBtn) return;
    
    uploadBtn.addEventListener('click', openUploadModal);
    
    if (uploadZone) {
        uploadZone.addEventListener('click', function(e) {
            if (e.target.tagName !== 'INPUT') fileInput.click();
        });
        
        uploadZone.addEventListener('dragover', function(e) {
            e.preventDefault();
            uploadZone.style.borderColor = 'var(--teal)';
            uploadZone.style.background = 'var(--teal-light)';
        });
        
        uploadZone.addEventListener('dragleave', function() {
            uploadZone.style.borderColor = '';
            uploadZone.style.background = '';
        });
        
        uploadZone.addEventListener('drop', function(e) {
            e.preventDefault();
            uploadZone.style.borderColor = '';
            uploadZone.style.background = '';
            if (e.dataTransfer.files.length) {
                fileInput.files = e.dataTransfer.files;
                handleFileSelect();
            }
        });
    }
    
    if (fileInput) {
        fileInput.addEventListener('change', handleFileSelect);
    }
    
    function handleFileSelect() {
        if (fileInput.files.length > 0) {
            const file = fileInput.files[0];
            if (!file.name.toLowerCase().endsWith('.csv')) {
                alert('Please select a CSV file');
                fileInput.value = '';
                return;
            }
            fileName.textContent = file.name;
            fileInfo.style.display = 'flex';
            uploadSubmit.disabled = false;
        }
    }
    
    if (uploadForm) {
        uploadForm.addEventListener('submit', async function(e) {
            e.preventDefault();
            if (!fileInput.files.length) return;
            
            uploadSubmit.disabled = true;
            uploadSubmit.textContent = 'Processing...';
            
            const formData = new FormData();
            formData.append('file', fileInput.files[0]);
            
            try {
                const response = await fetch('/upload', { method: 'POST', body: formData });
                const result = await response.json();
                
                if (result.success) {
                    window.location.href = result.redirect;
                } else {
                    alert('Error: ' + result.error);
                    uploadSubmit.disabled = false;
                    uploadSubmit.textContent = 'Upload & Process';
                }
            } catch (error) {
                alert('Upload failed: ' + error.message);
                uploadSubmit.disabled = false;
                uploadSubmit.textContent = 'Upload & Process';
            }
        });
    }
}

function initModalCloseHandlers() {
    window.addEventListener('click', function(e) {
        if (e.target.classList.contains('modal')) {
            e.target.classList.remove('show');
        }
    });
    
    window.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            document.querySelectorAll('.modal').forEach(m => m.classList.remove('show'));
        }
    });
}

// Legacy
function openModal() { openUploadModal(); }
function closeModal() { closeUploadModal(); }

// ==================== API REFRESH FUNCTIONS ====================

async function refreshData() {
    const btn = document.getElementById('refreshBtn');
    if (!btn || btn.classList.contains('loading')) return;
    
    btn.classList.add('loading');
    btn.textContent = '🔄 Refreshing...';
    
    try {
        const response = await fetch('/api/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        
        const data = await response.json();
        
        if (data.success) {
            showToast('Data refreshed successfully! Reloading page...', 'success');
            setTimeout(() => {
                window.location.reload();
            }, 1500);
        } else {
            showToast('Error: ' + data.error, 'error');
            btn.classList.remove('loading');
            btn.textContent = '🔄 Refresh Data';
        }
    } catch (error) {
        showToast('Failed to refresh data: ' + error.message, 'error');
        btn.classList.remove('loading');
        btn.textContent = '🔄 Refresh Data';
    }
}

function showToast(message, type) {
    // Remove existing toast
    const existingToast = document.querySelector('.toast');
    if (existingToast) {
        existingToast.remove();
    }
    
    // Create toast
    const toast = document.createElement('div');
    toast.className = 'toast ' + type + ' show';
    toast.innerHTML = message;
    document.body.appendChild(toast);
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    }, 5000);
}

// Check API status on load
async function checkApiStatus() {
    try {
        const response = await fetch('/api/status');
        const data = await response.json();
        
        if (data.api_configured) {
            console.log('CampMinder API configured');
            console.log('Last fetch:', data.last_fetch);
            console.log('Has cached data:', data.has_cached_data);
        } else {
            console.log('CampMinder API not configured');
        }
    } catch (error) {
        console.log('Could not check API status');
    }
}

// Add to DOMContentLoaded
document.addEventListener('DOMContentLoaded', function() {
    checkApiStatus();
});

// ==================== CAMP COMPARISON FUNCTIONS ====================

let campCompareChart = null;

async function updateCampComparison() {
    const select = document.getElementById('campComparisonSelect');
    const programName = select.value;
    
    if (!programName) {
        document.getElementById('campComparisonContent').innerHTML = 
            '<p class="placeholder-text">Select a camp above to see the year-over-year comparison</p>';
        document.getElementById('campComparisonChart').style.display = 'none';
        document.getElementById('campComparisonTable').style.display = 'none';
        return;
    }
    
    // Show loading
    document.getElementById('campComparisonContent').innerHTML = 
        '<p class="placeholder-text">Loading comparison data...</p>';
    
    try {
        const response = await fetch(`/api/program-comparison/${encodeURIComponent(programName)}`);
        const data = await response.json();
        
        // Hide placeholder
        document.getElementById('campComparisonContent').innerHTML = '';
        
        // Build comparison chart and table
        buildCampComparisonChart(data);
        buildCampComparisonTable(data);
        
        document.getElementById('campComparisonChart').style.display = 'block';
        document.getElementById('campComparisonTable').style.display = 'block';
        
    } catch (error) {
        console.error('Error loading camp comparison:', error);
        document.getElementById('campComparisonContent').innerHTML = 
            '<p class="placeholder-text error">Error loading comparison data</p>';
    }
}

function buildCampComparisonChart(data) {
    const ctx = document.getElementById('campCompareChart').getContext('2d');
    
    // Destroy existing chart
    if (campCompareChart) {
        campCompareChart.destroy();
    }
    
    const weeks = ['Week 1', 'Week 2', 'Week 3', 'Week 4', 'Week 5', 'Week 6', 'Week 7', 'Week 8', 'Week 9'];
    
    const data2026 = data.data_2026 ? 
        [data.data_2026.week_1, data.data_2026.week_2, data.data_2026.week_3, 
         data.data_2026.week_4, data.data_2026.week_5, data.data_2026.week_6,
         data.data_2026.week_7, data.data_2026.week_8, data.data_2026.week_9] : 
        [0,0,0,0,0,0,0,0,0];
    
    const data2025 = data.data_2025 ? 
        [data.data_2025.week_1, data.data_2025.week_2, data.data_2025.week_3,
         data.data_2025.week_4, data.data_2025.week_5, data.data_2025.week_6,
         data.data_2025.week_7, data.data_2025.week_8, data.data_2025.week_9] :
        [0,0,0,0,0,0,0,0,0];
    
    campCompareChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: weeks,
            datasets: [
                {
                    label: '2026',
                    data: data2026,
                    backgroundColor: 'rgba(38, 166, 154, 0.8)',
                    borderColor: 'rgba(38, 166, 154, 1)',
                    borderWidth: 1
                },
                {
                    label: '2025',
                    data: data2025,
                    backgroundColor: 'rgba(255, 179, 71, 0.8)',
                    borderColor: 'rgba(255, 179, 71, 1)',
                    borderWidth: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: `${data.program_name} - Enrollment by Week`,
                    font: { size: 16 }
                },
                legend: {
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Campers'
                    }
                }
            }
        }
    });
}

function buildCampComparisonTable(data) {
    const tbody = document.getElementById('campComparisonTableBody');
    tbody.innerHTML = '';
    
    let total2026 = 0, total2025 = 0;
    
    for (let i = 1; i <= 9; i++) {
        const val2026 = data.data_2026 ? (data.data_2026[`week_${i}`] || 0) : 0;
        const val2025 = data.data_2025 ? (data.data_2025[`week_${i}`] || 0) : 0;
        const diff = val2026 - val2025;
        const pctChange = val2025 > 0 ? Math.round((diff / val2025) * 100) : (val2026 > 0 ? 100 : 0);
        
        total2026 += val2026;
        total2025 += val2025;
        
        const diffClass = diff > 0 ? 'positive' : (diff < 0 ? 'negative' : '');
        const diffSign = diff > 0 ? '+' : '';
        
        tbody.innerHTML += `
            <tr>
                <td>Week ${i}</td>
                <td>${val2026}</td>
                <td>${val2025}</td>
                <td class="${diffClass}">${diffSign}${diff}</td>
                <td class="${diffClass}">${diffSign}${pctChange}%</td>
            </tr>
        `;
    }
    
    // Add totals row
    const totalDiff = total2026 - total2025;
    const totalPct = total2025 > 0 ? Math.round((totalDiff / total2025) * 100) : 0;
    const totalClass = totalDiff > 0 ? 'positive' : (totalDiff < 0 ? 'negative' : '');
    const totalSign = totalDiff > 0 ? '+' : '';
    
    tbody.innerHTML += `
        <tr class="total-row">
            <td><strong>Total</strong></td>
            <td><strong>${total2026}</strong></td>
            <td><strong>${total2025}</strong></td>
            <td class="${totalClass}"><strong>${totalSign}${totalDiff}</strong></td>
            <td class="${totalClass}"><strong>${totalSign}${totalPct}%</strong></td>
        </tr>
    `;
}
