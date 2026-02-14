// Camp Sol Taplin Dashboard JavaScript

// Global chart instances
let cumulativeChartInstance = null;

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

// Current modal context
var currentModalProgram = '';
var currentModalWeek = 0;

// Render participants table from data
function renderParticipantsTable(participants, list, program, week) {
    if (participants.length === 0) {
        list.innerHTML = '<div class="participant-count">No participants found</div>';
        return;
    }

    var isAdmin = (window.userPermissions && window.userPermissions.indexOf('edit_groups') !== -1);

    // Collect all emails for copy button
    var allEmails = [];
    participants.forEach(function(p) {
        if (p.f1p1_email) allEmails.push(p.f1p1_email);
        if (p.f1p1_email2) allEmails.push(p.f1p1_email2);
        if (p.f1p2_email) allEmails.push(p.f1p2_email);
        if (p.f1p2_email2) allEmails.push(p.f1p2_email2);
    });
    allEmails = allEmails.filter(function(v, i, a) { return a.indexOf(v) === i; });

    // Action bar with buttons
    var html = '<div class="participant-actions-bar">';
    html += '<div class="participant-count">' + participants.length + ' participant' + (participants.length !== 1 ? 's' : '');
    if (allEmails.length > 0) {
        html += ' &nbsp; <button class="copy-emails-btn" onclick="copyAllEmails(this)" data-emails="' + allEmails.join(',') + '">📋 Copy All Emails (' + allEmails.length + ')</button>';
    }
    html += '</div>';
    if (program && week) {
        html += '<div class="participant-action-buttons">';
        html += '<button class="btn-action btn-download" onclick="downloadByGroups()">📥 Download By Groups</button>';
        html += '<button class="btn-action btn-print" onclick="printByGroups()">🖨️ Print By Groups</button>';
        html += '</div>';
    }
    html += '</div>';

    html += '<div class="participants-table-wrapper"><table class="participants-table">';
    html += '<thead><tr>';
    html += '<th>#</th>';
    html += '<th>Group</th>';
    html += '<th>Name</th>';
    html += '<th>Gender</th>';
    html += '<th>Share Group With</th>';
    html += '<th>F1P1 Login/Email</th>';
    html += '<th>F1P1 Email 2</th>';
    html += '<th>F1P2 Login/Email</th>';
    html += '<th>F1P2 Email 2</th>';
    html += '</tr></thead><tbody>';

    participants.forEach(function(p, index) {
        html += '<tr>';
        html += '<td>' + (index + 1) + '</td>';

        // Group column
        if (isAdmin && program && week) {
            var groupVal = p.group || 0;
            html += '<td class="group-cell">';
            html += '<select class="group-select" data-person-id="' + p.person_id + '" onchange="saveGroupAssignment(this)">';
            html += '<option value="0"' + (groupVal === 0 ? ' selected' : '') + '>-</option>';
            html += '<option value="1"' + (groupVal === 1 ? ' selected' : '') + '>1</option>';
            html += '<option value="2"' + (groupVal === 2 ? ' selected' : '') + '>2</option>';
            html += '<option value="3"' + (groupVal === 3 ? ' selected' : '') + '>3</option>';
            html += '</select>';
            html += '</td>';
        } else {
            var groupDisplay = p.group && p.group > 0 ? p.group : '-';
            html += '<td class="group-cell">' + groupDisplay + '</td>';
        }

        html += '<td class="participant-name-cell">' + p.first_name + ' ' + p.last_name + '</td>';
        html += '<td>' + (p.gender || '-') + '</td>';
        html += '<td>' + (p.share_group_with || '-') + '</td>';
        html += '<td>' + (p.f1p1_email || '-') + '</td>';
        html += '<td>' + (p.f1p1_email2 || '-') + '</td>';
        html += '<td>' + (p.f1p2_email || '-') + '</td>';
        html += '<td>' + (p.f1p2_email2 || '-') + '</td>';
        html += '</tr>';
    });

    html += '</tbody></table></div>';
    list.innerHTML = html;
}

// Save group assignment via AJAX
function saveGroupAssignment(selectEl) {
    var personId = selectEl.getAttribute('data-person-id');
    var group = parseInt(selectEl.value);

    selectEl.classList.add('saving');

    fetch('/api/group-assignment/' + encodeURIComponent(currentModalProgram) + '/' + currentModalWeek, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ person_id: personId, group: group })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        selectEl.classList.remove('saving');
        if (data.success) {
            selectEl.classList.add('saved');
            setTimeout(function() { selectEl.classList.remove('saved'); }, 1000);
        } else {
            selectEl.classList.add('error');
            setTimeout(function() { selectEl.classList.remove('error'); }, 2000);
            alert('Error saving group: ' + (data.error || 'Unknown error'));
        }
    })
    .catch(function(err) {
        selectEl.classList.remove('saving');
        selectEl.classList.add('error');
        setTimeout(function() { selectEl.classList.remove('error'); }, 2000);
        alert('Failed to save group assignment: ' + err.message);
    });
}

// Download By Groups Excel
function downloadByGroups() {
    if (!currentModalProgram || !currentModalWeek) return;
    window.open('/api/download-by-groups/' + encodeURIComponent(currentModalProgram) + '/' + currentModalWeek, '_blank');
}

// Print By Groups
function printByGroups() {
    if (!currentModalProgram || !currentModalWeek) return;
    window.open('/print-by-groups/' + encodeURIComponent(currentModalProgram) + '/' + currentModalWeek, '_blank');
}

// ==================== Multi-Program Enrollment Download ====================

function clearEnrollmentProgramSelection() {
    document.querySelectorAll('#enrollmentProgramCheckboxes input[type="checkbox"]').forEach(function(cb) {
        cb.checked = false;
    });
    updateEnrollmentDownloadButton();
}

function updateEnrollmentDownloadButton() {
    var checked = document.querySelectorAll('#enrollmentProgramCheckboxes input[type="checkbox"]:checked');
    var btn = document.getElementById('downloadEnrollmentBtn');
    if (btn) {
        btn.disabled = (checked.length === 0);
        btn.textContent = checked.length > 0
            ? '📥 Download Enrollment List (' + checked.length + ' programs)'
            : '📥 Download Enrollment List';
    }
}

function downloadMultiProgramEnrollment() {
    var selected = [];
    document.querySelectorAll('#enrollmentProgramCheckboxes input[type="checkbox"]:checked').forEach(function(cb) {
        selected.push(cb.value);
    });

    if (selected.length === 0) {
        alert('Please select at least one program.');
        return;
    }

    var statusEl = document.getElementById('enrollmentDownloadStatus');
    var btn = document.getElementById('downloadEnrollmentBtn');
    if (statusEl) statusEl.textContent = 'Generating...';
    if (btn) btn.disabled = true;

    fetch('/api/download-multi-program-enrollment', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ programs: selected })
    })
    .then(function(response) {
        if (!response.ok) {
            return response.json().then(function(err) { throw new Error(err.error || 'Download failed'); });
        }
        return response.blob();
    })
    .then(function(blob) {
        var url = window.URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        // Build filename from selected program names
        var namesPart = selected.map(function(s) { return s.replace(/\s+/g, ''); }).join('_');
        if (namesPart.length > 80) namesPart = namesPart.substring(0, 80);
        a.download = 'Enrollment_' + namesPart + '.xlsx';
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        if (statusEl) statusEl.textContent = 'Downloaded!';
        setTimeout(function() { if (statusEl) statusEl.textContent = ''; }, 3000);
    })
    .catch(function(err) {
        if (statusEl) statusEl.textContent = 'Error: ' + err.message;
        setTimeout(function() { if (statusEl) statusEl.textContent = ''; }, 5000);
    })
    .finally(function() {
        updateEnrollmentDownloadButton();
    });
}

// Wire up enrollment program checkboxes
(function() {
    var checkboxContainer = document.getElementById('enrollmentProgramCheckboxes');
    if (checkboxContainer) {
        checkboxContainer.addEventListener('change', updateEnrollmentDownloadButton);
    }
})();

// Upload Share Group With CSV
function uploadShareGroup() {
    var fileInput = document.getElementById('shareGroupFile');
    var status = document.getElementById('shareGroupStatus');
    if (!fileInput || !fileInput.files.length) {
        if (status) status.textContent = 'Please select a CSV file first.';
        return;
    }
    var formData = new FormData();
    formData.append('file', fileInput.files[0]);

    if (status) status.textContent = 'Uploading...';

    fetch('/api/upload-share-group', {
        method: 'POST',
        body: formData
    })
    .then(function(resp) { return resp.json(); })
    .then(function(data) {
        if (data.success) {
            if (status) status.textContent = '✅ ' + data.message;
            fileInput.value = '';
        } else {
            if (status) status.textContent = '❌ ' + (data.error || 'Upload failed');
        }
    })
    .catch(function(err) {
        if (status) status.textContent = '❌ Error: ' + err.message;
    });
}

// Show participants modal - fetches details on demand
function showParticipants(program, week) {
    const modal = document.getElementById('participantsModal');
    const title = document.getElementById('participantsTitle');
    const list = document.getElementById('participantsList');

    if (!modal) return;

    // Set global context for group operations
    currentModalProgram = program;
    currentModalWeek = week;

    title.textContent = '👥 ' + program + ' - Week ' + week;
    list.innerHTML = '<div class="participant-count">Loading participants... ⏳</div>';
    modal.classList.add('show');

    // Fetch enriched data from API
    fetch('/api/participants/' + encodeURIComponent(program) + '/' + week)
        .then(function(r) { return r.json(); })
        .then(function(data) {
            var participants = data.participants || [];
            renderParticipantsTable(participants, list, program, week);
        })
        .catch(function(err) {
            // Fallback to pre-loaded data (without emails)
            if (window.participantsData) {
                var programData = window.participantsData[program];
                var participants = programData ? (programData[String(week)] || []) : [];
                renderParticipantsTable(participants, list, program, week);
            } else {
                list.innerHTML = '<div class="participant-count">Failed to load participants</div>';
            }
        });
}

function copyAllEmails(btn) {
    var emails = btn.getAttribute('data-emails');
    if (navigator.clipboard) {
        navigator.clipboard.writeText(emails.replace(/,/g, '; ')).then(function() {
            var original = btn.textContent;
            btn.textContent = '✅ Copied!';
            btn.style.background = '#4CAF50';
            btn.style.color = 'white';
            setTimeout(function() {
                btn.innerHTML = original;
                btn.style.background = '';
                btn.style.color = '';
            }, 2000);
        });
    } else {
        // Fallback
        var ta = document.createElement('textarea');
        ta.value = emails.replace(/,/g, '; ');
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        var original = btn.textContent;
        btn.textContent = '✅ Copied!';
        setTimeout(function() { btn.innerHTML = original; }, 2000);
    }
}

function closeParticipantsModal() {
    const modal = document.getElementById('participantsModal');
    if (modal) modal.classList.remove('show');
}

// Date Chart Functions - Multi-year overlay

// Store full datasets for filtering
let fullChartLabels = [];
let fullChartDays = [];
let fullDatasets = {};

function buildMultiYearData() {
    const data = window.comparisonChartData;
    if (!data) return;

    fullChartLabels = data.labels || [];
    fullChartDays = data.days || [];
    const todayDayOfYear = data.today_day_of_year || 365;

    // 2024 dataset
    fullDatasets['2024'] = data['2024'] || [];

    // 2025 dataset
    fullDatasets['2025'] = data['2025'] || [];

    // 2026 dataset - build from current dateStats, cut at today
    fullDatasets['2026'] = [];
    if (window.dateStats2026 && window.dateStats2026.length > 0) {
        for (let i = 0; i < fullChartDays.length; i++) {
            const daysOffset = fullChartDays[i];
            if (daysOffset > todayDayOfYear) {
                fullDatasets['2026'].push(null);
                continue;
            }
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
            fullDatasets['2026'].push(cumulative || null);
        }
    }
}

function getFilteredIndices() {
    const startDateEl = document.getElementById('startDate');
    const endDateEl = document.getElementById('endDate');
    const startDate = startDateEl ? startDateEl.value : '';
    const endDate = endDateEl ? endDateEl.value : '';

    if (!startDate && !endDate) return null; // No filter

    // Convert date inputs to day-of-year offsets
    let startDay = 0;
    let endDay = 366;

    if (startDate) {
        const sd = new Date(startDate);
        const yearStart = new Date(sd.getFullYear(), 0, 1);
        startDay = Math.floor((sd - yearStart) / (1000 * 60 * 60 * 24));
    }
    if (endDate) {
        const ed = new Date(endDate);
        const yearStart = new Date(ed.getFullYear(), 0, 1);
        endDay = Math.floor((ed - yearStart) / (1000 * 60 * 60 * 24));
    }

    const indices = [];
    for (let i = 0; i < fullChartDays.length; i++) {
        if (fullChartDays[i] >= startDay && fullChartDays[i] <= endDay) {
            indices.push(i);
        }
    }
    return indices;
}

function updateDateChart() {
    if (!cumulativeChartInstance) return;

    const indices = getFilteredIndices();

    let labels, data2024, data2025, data2026;

    if (indices) {
        labels = indices.map(i => fullChartLabels[i]);
        data2024 = indices.map(i => fullDatasets['2024'][i]);
        data2025 = indices.map(i => fullDatasets['2025'][i]);
        data2026 = indices.map(i => fullDatasets['2026'][i]);
    } else {
        labels = fullChartLabels;
        data2024 = fullDatasets['2024'];
        data2025 = fullDatasets['2025'];
        data2026 = fullDatasets['2026'];
    }

    cumulativeChartInstance.data.labels = labels;
    cumulativeChartInstance.data.datasets[0].data = data2024;
    cumulativeChartInstance.data.datasets[1].data = data2025;
    cumulativeChartInstance.data.datasets[2].data = data2026;
    cumulativeChartInstance.update();
}

function resetDateFilters() {
    const startDate = document.getElementById('startDate');
    const endDate = document.getElementById('endDate');
    if (startDate) startDate.value = '';
    if (endDate) endDate.value = '';
    updateDateChart();
}

function initCumulativeChart() {
    const canvas = document.getElementById('cumulativeChart');
    if (!canvas || !window.comparisonChartData) return;

    const ctx = canvas.getContext('2d');

    if (cumulativeChartInstance) {
        cumulativeChartInstance.destroy();
    }

    // Build full data arrays
    buildMultiYearData();

    const datasets = [];

    // 2024 - gray dashed
    datasets.push({
        label: '2024',
        data: fullDatasets['2024'],
        borderColor: '#9E9E9E',
        backgroundColor: 'transparent',
        borderWidth: 2,
        borderDash: [5, 5],
        tension: 0.3,
        pointRadius: 0
    });

    // 2025 - green solid
    datasets.push({
        label: '2025',
        data: fullDatasets['2025'],
        borderColor: '#7CB342',
        backgroundColor: 'transparent',
        borderWidth: 3,
        tension: 0.3,
        pointRadius: 0
    });

    // 2026 - blue filled
    datasets.push({
        label: '2026 (Current)',
        data: fullDatasets['2026'],
        borderColor: '#00A9CE',
        backgroundColor: 'rgba(0, 169, 206, 0.1)',
        borderWidth: 4,
        fill: true,
        tension: 0.3,
        pointRadius: 0
    });

    cumulativeChartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: fullChartLabels,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true, position: 'top' },
                title: {
                    display: true,
                    text: 'Cumulative Camper Weeks Comparison',
                    font: { size: 16 }
                }
            },
            scales: {
                x: {
                    title: { display: true, text: 'Date', font: { weight: 'bold' } },
                    ticks: { maxTicksLimit: 15 }
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
let selectedCamps = [];
let campDataCache = {};

// Colors for multiple camps
const campColors = [
    { bg: 'rgba(38, 166, 154, 0.8)', border: 'rgba(38, 166, 154, 1)' },
    { bg: 'rgba(255, 179, 71, 0.8)', border: 'rgba(255, 179, 71, 1)' },
    { bg: 'rgba(66, 165, 245, 0.8)', border: 'rgba(66, 165, 245, 1)' },
    { bg: 'rgba(171, 71, 188, 0.8)', border: 'rgba(171, 71, 188, 1)' },
    { bg: 'rgba(255, 112, 67, 0.8)', border: 'rgba(255, 112, 67, 1)' },
    { bg: 'rgba(124, 179, 66, 0.8)', border: 'rgba(124, 179, 66, 1)' },
    { bg: 'rgba(141, 110, 99, 0.8)', border: 'rgba(141, 110, 99, 1)' },
    { bg: 'rgba(92, 107, 192, 0.8)', border: 'rgba(92, 107, 192, 1)' }
];

function clearCampSelection() {
    document.querySelectorAll('#campCheckboxes input[type="checkbox"]').forEach(cb => {
        cb.checked = false;
    });
    selectedCamps = [];
    campDataCache = {};
    document.getElementById('campComparisonContent').innerHTML = 
        '<p class="placeholder-text">Select camps above to see the year-over-year comparison</p>';
    document.getElementById('campComparisonChart').style.display = 'none';
    document.getElementById('campComparisonTables').innerHTML = '';
}

async function updateMultiCampComparison() {
    // Get all selected camps
    selectedCamps = [];
    document.querySelectorAll('#campCheckboxes input[type="checkbox"]:checked').forEach(cb => {
        selectedCamps.push(cb.value);
    });
    
    if (selectedCamps.length === 0) {
        document.getElementById('campComparisonContent').innerHTML = 
            '<p class="placeholder-text">Select camps above to see the year-over-year comparison</p>';
        document.getElementById('campComparisonChart').style.display = 'none';
        document.getElementById('campComparisonTables').innerHTML = '';
        return;
    }
    
    // Show loading
    document.getElementById('campComparisonContent').innerHTML = 
        '<p class="placeholder-text">Loading comparison data...</p>';
    
    try {
        // Fetch data for all selected camps
        const fetchPromises = selectedCamps.map(async (camp) => {
            if (!campDataCache[camp]) {
                const response = await fetch(`/api/program-comparison/${encodeURIComponent(camp)}`);
                campDataCache[camp] = await response.json();
            }
            return campDataCache[camp];
        });
        
        await Promise.all(fetchPromises);
        
        // Hide placeholder
        document.getElementById('campComparisonContent').innerHTML = '';
        
        // Build chart with all camps
        buildMultiCampChart();
        
        // Build tables for each camp
        buildMultiCampTables();
        
        document.getElementById('campComparisonChart').style.display = 'block';
        
    } catch (error) {
        console.error('Error loading camp comparison:', error);
        document.getElementById('campComparisonContent').innerHTML = 
            '<p class="placeholder-text error">Error loading comparison data</p>';
    }
}

function buildMultiCampChart() {
    const ctx = document.getElementById('campCompareChart').getContext('2d');
    
    // Destroy existing chart
    if (campCompareChart) {
        campCompareChart.destroy();
    }
    
    const weeks = ['Wk 1', 'Wk 2', 'Wk 3', 'Wk 4', 'Wk 5', 'Wk 6', 'Wk 7', 'Wk 8', 'Wk 9'];
    const datasets = [];
    
    selectedCamps.forEach((camp, index) => {
        const data = campDataCache[camp];
        const colorIndex = index % campColors.length;
        
        // 2026 data
        const data2026 = data.data_2026 ? 
            [data.data_2026.week_1, data.data_2026.week_2, data.data_2026.week_3, 
             data.data_2026.week_4, data.data_2026.week_5, data.data_2026.week_6,
             data.data_2026.week_7, data.data_2026.week_8, data.data_2026.week_9] : 
            [0,0,0,0,0,0,0,0,0];
        
        datasets.push({
            label: `${camp} (2026)`,
            data: data2026,
            backgroundColor: campColors[colorIndex].bg,
            borderColor: campColors[colorIndex].border,
            borderWidth: 1
        });
        
        // 2025 data (lighter/striped)
        const data2025 = data.data_2025 ? 
            [data.data_2025.week_1, data.data_2025.week_2, data.data_2025.week_3,
             data.data_2025.week_4, data.data_2025.week_5, data.data_2025.week_6,
             data.data_2025.week_7, data.data_2025.week_8, data.data_2025.week_9] :
            [0,0,0,0,0,0,0,0,0];
        
        datasets.push({
            label: `${camp} (2025)`,
            data: data2025,
            backgroundColor: campColors[colorIndex].bg.replace('0.8', '0.3'),
            borderColor: campColors[colorIndex].border,
            borderWidth: 1,
            borderDash: [5, 5]
        });
    });
    
    campCompareChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: weeks,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Enrollment by Week - 2026 vs 2025',
                    font: { size: 16 }
                },
                legend: {
                    position: 'top',
                    labels: { boxWidth: 12, font: { size: 10 } }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Campers' }
                }
            }
        }
    });
}

function buildMultiCampTables() {
    const container = document.getElementById('campComparisonTables');
    container.innerHTML = '';
    
    if (selectedCamps.length === 0) return;
    
    // Build consolidated table like Excel
    let tableHTML = `
        <div class="consolidated-comparison-table">
            <table class="comparison-matrix">
                <thead>
                    <tr class="header-row year-2026">
                        <th class="program-header">Program</th>
                        <th colspan="9" class="year-header">2026</th>
                        <th class="total-header">Total</th>
                    </tr>
                    <tr class="subheader-row">
                        <th></th>
                        <th>1</th><th>2</th><th>3</th><th>4</th><th>5</th><th>6</th><th>7</th><th>8</th><th>9</th>
                        <th></th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    // 2026 Data rows
    selectedCamps.forEach((camp) => {
        const data = campDataCache[camp];
        const d = data.data_2026 || {};
        const weeks = [d.week_1||0, d.week_2||0, d.week_3||0, d.week_4||0, d.week_5||0, d.week_6||0, d.week_7||0, d.week_8||0, d.week_9||0];
        const total = weeks.reduce((a,b) => a+b, 0);
        
        tableHTML += `
            <tr class="data-row year-2026">
                <td class="program-name">${camp}</td>
                ${weeks.map(w => `<td class="week-cell">${w || ''}</td>`).join('')}
                <td class="total-cell">${total}</td>
            </tr>
        `;
    });
    
    // Separator row
    tableHTML += `
                </tbody>
            </table>
            
            <table class="comparison-matrix table-2025">
                <thead>
                    <tr class="header-row year-2025">
                        <th class="program-header">Program</th>
                        <th colspan="9" class="year-header">2025</th>
                        <th class="total-header">Total</th>
                    </tr>
                    <tr class="subheader-row">
                        <th></th>
                        <th>1</th><th>2</th><th>3</th><th>4</th><th>5</th><th>6</th><th>7</th><th>8</th><th>9</th>
                        <th></th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    // 2025 Data rows
    selectedCamps.forEach((camp) => {
        const data = campDataCache[camp];
        const d = data.data_2025 || {};
        const weeks = [d.week_1||0, d.week_2||0, d.week_3||0, d.week_4||0, d.week_5||0, d.week_6||0, d.week_7||0, d.week_8||0, d.week_9||0];
        const total = weeks.reduce((a,b) => a+b, 0);
        
        tableHTML += `
            <tr class="data-row year-2025">
                <td class="program-name">${camp}</td>
                ${weeks.map(w => `<td class="week-cell">${w || ''}</td>`).join('')}
                <td class="total-cell">${total}</td>
            </tr>
        `;
    });
    
    tableHTML += `
                </tbody>
            </table>
        </div>
    `;
    
    container.innerHTML = tableHTML;
}

// ==================== RETENTION RATE ====================

async function loadRetentionRate() {
    const valueEl = document.getElementById('retentionValue');
    const subtextEl = document.getElementById('retentionSubtext');
    const cardEl = document.getElementById('retentionCard');
    
    if (!valueEl || !subtextEl) return;
    
    try {
        const response = await fetch('/api/retention');
        
        if (!response.ok) {
            throw new Error('Failed to fetch retention data');
        }
        
        const data = await response.json();
        
        if (data.error) {
            valueEl.textContent = 'N/A';
            subtextEl.textContent = data.error;
            return;
        }
        
        // Display retention rate
        const rate = data.retention_rate || 0;
        valueEl.innerHTML = `<span class="status-${rate >= 70 ? 'success' : rate >= 50 ? 'warning' : 'danger'}">${rate}%</span>`;
        subtextEl.innerHTML = `${data.returning_campers} of ${data.campers_previous} returned`;
        
        // Add tooltip with more details
        cardEl.title = `Returning: ${data.returning_campers}\nNew Campers: ${data.new_campers}\nLost: ${data.lost_campers}`;
        
    } catch (error) {
        console.error('Error loading retention rate:', error);
        valueEl.textContent = 'N/A';
        subtextEl.textContent = 'Unable to load';
    }
}

// Load retention rate on page load
document.addEventListener('DOMContentLoaded', function() {
    // Load retention rate asynchronously (it can take a few seconds)
    setTimeout(loadRetentionRate, 1000);
});
