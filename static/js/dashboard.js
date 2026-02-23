// Camp Sol Taplin Dashboard JavaScript

// Global chart instances
let cumulativeChartInstance = null;
let attendanceTrendChartInstance = null;

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
    if (viewName === 'finance') {
        setTimeout(initFinanceCharts, 100);
    }
    if (viewName === 'attendance') {
        setTimeout(initAttendanceView, 100);
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

    recalcEnrollmentTotals(table);
}

// Recalculate Enrollment Matrix TOTALS based on visible rows
function recalcEnrollmentTotals(table) {
    const rows = table.querySelectorAll('tbody tr');
    const sums = { w: [0,0,0,0,0,0,0,0,0], total: 0, fte: 0, goal: 0 };
    const nct = { w: [0,0,0,0,0,0,0,0,0], total: 0, fte: 0, goal: 0 };

    rows.forEach(function(row) {
        if (row.style.display === 'none') return;
        const isCT = row.dataset.isCt === 'true';
        for (let i = 0; i < 9; i++) {
            const val = parseFloat(row.dataset['w' + (i + 1)]) || 0;
            sums.w[i] += val;
            if (!isCT) nct.w[i] += val;
        }
        const t = parseFloat(row.dataset.total) || 0;
        const f = parseFloat(row.dataset.fte) || 0;
        const g = parseFloat(row.dataset.goal) || 0;
        sums.total += t; sums.fte += f; sums.goal += g;
        if (!isCT) { nct.total += t; nct.fte += f; nct.goal += g; }
    });

    for (let i = 1; i <= 9; i++) {
        setCell('em-totals-w' + i, sums.w[i - 1]);
        setCell('em-nct-w' + i, nct.w[i - 1]);
    }
    setCell('em-totals-total', sums.total);
    setCell('em-totals-fte', sums.fte.toFixed(2));
    setCell('em-totals-goal', sums.goal);
    setCell('em-totals-pct', sums.goal > 0 ? Math.round(sums.fte / sums.goal * 100) + '%' : '0%');
    setCell('em-nct-total', nct.total);
    setCell('em-nct-fte', nct.fte.toFixed(2));
    setCell('em-nct-goal', nct.goal);
    setCell('em-nct-pct', nct.goal > 0 ? Math.round(nct.fte / nct.goal * 100) + '%' : '0%');
}

// Filter Old View Stats
function filterOldView() {
    const filter = document.getElementById('oldViewCategoryFilter');
    const table = document.getElementById('oldViewMatrix');
    if (!table) return;

    const filterValue = filter ? filter.value : 'all';
    const rows = table.querySelectorAll('tbody tr');

    rows.forEach(function(row) {
        const cat = row.dataset.category || '';
        row.style.display = (filterValue === 'all' || cat === filterValue) ? '' : 'none';
    });

    recalcOldViewTotals(table);
}

// Recalculate Old View Stats TOTALS based on visible rows
function recalcOldViewTotals(table) {
    const rows = table.querySelectorAll('tbody tr');
    const s26 = { w: [0,0,0,0,0,0,0,0,0], total: 0, fte: 0, goal: 0 };
    const n26 = { w: [0,0,0,0,0,0,0,0,0], total: 0, fte: 0, goal: 0 };
    const s25 = { w: [0,0,0,0,0,0,0,0,0], total: 0, fte: 0 };
    const n25 = { w: [0,0,0,0,0,0,0,0,0], total: 0, fte: 0 };
    const counted25 = {};
    const countedNct25 = {};

    rows.forEach(function(row) {
        if (row.style.display === 'none') return;
        const isCT = row.dataset.isCt === 'true';
        const p25Name = row.dataset.p25Name || '';

        // 2026
        for (let i = 0; i < 9; i++) {
            const v = parseFloat(row.dataset['w' + (i+1) + '-26']) || 0;
            s26.w[i] += v;
            if (!isCT) n26.w[i] += v;
        }
        const t26 = parseFloat(row.dataset['total-26']) || 0;
        const f26 = parseFloat(row.dataset.fte26) || 0;
        const g = parseFloat(row.dataset.goal) || 0;
        s26.total += t26; s26.fte += f26; s26.goal += g;
        if (!isCT) { n26.total += t26; n26.fte += f26; n26.goal += g; }

        // 2025 with dedup
        if (p25Name && !counted25[p25Name]) {
            counted25[p25Name] = true;
            for (let i = 0; i < 9; i++) {
                s25.w[i] += parseFloat(row.dataset['w' + (i+1) + '-25']) || 0;
            }
            s25.total += parseFloat(row.dataset['total-25']) || 0;
            s25.fte += parseFloat(row.dataset.fte25) || 0;
        }
        if (!isCT && p25Name && !countedNct25[p25Name]) {
            countedNct25[p25Name] = true;
            for (let i = 0; i < 9; i++) {
                n25.w[i] += parseFloat(row.dataset['w' + (i+1) + '-25']) || 0;
            }
            n25.total += parseFloat(row.dataset['total-25']) || 0;
            n25.fte += parseFloat(row.dataset.fte25) || 0;
        }
    });

    // TOTALS row
    setCell('ov-totals-fte25', s25.fte.toFixed(1));
    setCell('ov-totals-fte26', s26.fte.toFixed(2));
    setCell('ov-totals-goal', s26.goal);
    for (let i = 1; i <= 9; i++) {
        setCell('ov-totals-w' + i + '-26', s26.w[i-1]);
        setCell('ov-totals-w' + i + '-25', s25.w[i-1]);
    }
    setCell('ov-totals-total-26', s26.total);
    setCell('ov-totals-total-25', s25.total);

    // TOTALS w/o CT row
    setCell('ov-nct-fte25', n25.fte.toFixed(1));
    setCell('ov-nct-fte26', n26.fte.toFixed(2));
    setCell('ov-nct-goal', n26.goal);
    for (let i = 1; i <= 9; i++) {
        setCell('ov-nct-w' + i + '-26', n26.w[i-1]);
        setCell('ov-nct-w' + i + '-25', n25.w[i-1]);
    }
    setCell('ov-nct-total-26', n26.total);
    setCell('ov-nct-total-25', n25.total);
}

// Current modal context
let currentModalProgram = '';
let currentModalWeek = 0;

// Render participants table from data
function renderParticipantsTable(participants, list, program, week) {
    if (participants.length === 0) {
        list.innerHTML = '<div class="participant-count">No participants found</div>';
        return;
    }

    const isAdmin = (window.userPermissions && window.userPermissions.indexOf('edit_groups') !== -1);

    // Collect all unique emails for copy button
    let allEmails = [];
    participants.forEach(function(p) {
        if (p.f1p1_email) allEmails.push(p.f1p1_email);
        if (p.f1p1_email2) allEmails.push(p.f1p1_email2);
        if (p.f1p2_email) allEmails.push(p.f1p2_email);
        if (p.f1p2_email2) allEmails.push(p.f1p2_email2);
    });
    allEmails = allEmails.filter(function(v, i, a) { return a.indexOf(v) === i; });

    // Action bar with buttons
    let html = '<div class="participant-actions-bar">';
    html += '<div class="participant-count">' + participants.length + ' participant' + (participants.length !== 1 ? 's' : '');
    if (allEmails.length > 0) {
        html += ' &nbsp; <button class="copy-emails-btn" onclick="copyAllEmails(this)" data-emails="' + allEmails.join(',') + '">📋 Copy All Emails (' + allEmails.length + ')</button>';
    }
    html += '</div>';
    if (program && week) {
        html += '<div class="participant-action-buttons">';
        if (isAdmin) {
            html += '<button class="btn-action btn-reset" onclick="resetGroups()">🔄 Reset Groups</button>';
        }
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

    let currentGroup = -1;
    let groupIndex = 0;

    participants.forEach(function(p, index) {
        const groupVal = p.group || 0;

        // Insert group separator when group changes
        if (groupVal !== currentGroup) {
            currentGroup = groupVal;
            groupIndex = 0;
            const separatorLabel = groupVal === 0 ? 'Unassigned' : 'Group ' + groupVal;
            const separatorIcon = groupVal === 0 ? '⬜' : '📌';
            html += '<tr class="group-separator group-separator-' + groupVal + '">';
            html += '<td colspan="9">' + separatorIcon + ' ' + separatorLabel + '</td>';
            html += '</tr>';
        }

        groupIndex++;
        const rowClass = groupVal > 0 ? ' class="group-row group-' + groupVal + '"' : '';
        html += '<tr' + rowClass + '>';
        html += '<td>' + groupIndex + '</td>';

        // Group column
        if (isAdmin && program && week) {
            html += '<td class="group-cell">';
            html += '<select class="group-select" data-person-id="' + p.person_id + '" onchange="saveGroupAssignment(this)">';
            html += '<option value="0"' + (groupVal === 0 ? ' selected' : '') + '>-</option>';
            html += '<option value="1"' + (groupVal === 1 ? ' selected' : '') + '>1</option>';
            html += '<option value="2"' + (groupVal === 2 ? ' selected' : '') + '>2</option>';
            html += '<option value="3"' + (groupVal === 3 ? ' selected' : '') + '>3</option>';
            html += '</select>';
            html += '</td>';
        } else {
            const groupDisplay = groupVal > 0 ? groupVal : '-';
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

// Save group assignment via AJAX (with forward propagation)
function saveGroupAssignment(selectEl) {
    const personId = selectEl.getAttribute('data-person-id');
    const group = parseInt(selectEl.value);

    selectEl.classList.add('saving');

    fetch('/api/group-assignment/' + encodeURIComponent(currentModalProgram) + '/' + currentModalWeek, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ person_id: personId, group: group, propagate_forward: true })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        selectEl.classList.remove('saving');
        if (data.success) {
            selectEl.classList.add('saved');
            setTimeout(function() { selectEl.classList.remove('saved'); }, 1000);

            // Show toast with updated weeks info
            const updatedWeeks = data.updated_weeks || [currentModalWeek];
            if (updatedWeeks.length > 1) {
                const groupLabel = group === 0 ? 'Unassigned' : 'Group ' + group;
                showToast(groupLabel + ' set for weeks ' + updatedWeeks.join(', '), 'success');
            }

            // Re-render table with updated group sorting
            refreshParticipantsTable();
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

// Re-fetch and re-render participants table after a group change
function refreshParticipantsTable() {
    const list = document.getElementById('participantsList');
    if (!list || !currentModalProgram || !currentModalWeek) return;

    fetch('/api/participants/' + encodeURIComponent(currentModalProgram) + '/' + currentModalWeek)
        .then(function(r) { return r.json(); })
        .then(function(data) {
            const participants = data.participants || [];
            renderParticipantsTable(participants, list, currentModalProgram, currentModalWeek);
        })
        .catch(function(err) {
            console.error('Failed to refresh participants table:', err);
        });
}

// Reset all group assignments for current program/week
function resetGroups() {
    if (!currentModalProgram || !currentModalWeek) return;

    if (!confirm('Reset all group assignments for ' + currentModalProgram + ' Week ' + currentModalWeek + ' to unassigned?')) {
        return;
    }

    fetch('/api/reset-groups/' + encodeURIComponent(currentModalProgram) + '/' + currentModalWeek, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.success) {
            showToast('Reset ' + data.deleted_count + ' group assignment' + (data.deleted_count !== 1 ? 's' : '') + ' for Week ' + currentModalWeek, 'success');
            refreshParticipantsTable();
        } else {
            alert('Error resetting groups: ' + (data.error || 'Unknown error'));
        }
    })
    .catch(function(err) {
        alert('Failed to reset groups: ' + err.message);
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
    const checked = document.querySelectorAll('#enrollmentProgramCheckboxes input[type="checkbox"]:checked');
    const count = checked.length;
    const btn = document.getElementById('downloadEnrollmentBtn');
    const emailBtn = document.getElementById('emailEnrollmentBtn');
    if (btn) {
        btn.disabled = (count === 0);
        btn.textContent = count > 0
            ? '📥 Download Enrollment List (' + count + ' programs)'
            : '📥 Download Enrollment List';
    }
    if (emailBtn) {
        emailBtn.disabled = (count === 0);
        emailBtn.textContent = count > 0
            ? '📧 Email Enrollment List (' + count + ' programs)'
            : '📧 Email Enrollment List';
    }
}

function downloadMultiProgramEnrollment() {
    const selected = [];
    document.querySelectorAll('#enrollmentProgramCheckboxes input[type="checkbox"]:checked').forEach(function(cb) {
        selected.push(cb.value);
    });

    if (selected.length === 0) {
        alert('Please select at least one program.');
        return;
    }

    const statusEl = document.getElementById('enrollmentDownloadStatus');
    const btn = document.getElementById('downloadEnrollmentBtn');
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
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        // Build filename from selected program names
        let namesPart = selected.map(function(s) { return s.replace(/\s+/g, ''); }).join('_');
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
    const checkboxContainer = document.getElementById('enrollmentProgramCheckboxes');
    if (checkboxContainer) {
        checkboxContainer.addEventListener('change', updateEnrollmentDownloadButton);
    }
})();

// ==================== Email Enrollment List (mailto) ====================

function emailEnrollmentList() {
    const selected = [];
    document.querySelectorAll('#enrollmentProgramCheckboxes input[type="checkbox"]:checked').forEach(function(cb) {
        selected.push(cb.value);
    });

    if (selected.length === 0) {
        alert('Please select at least one program.');
        return;
    }

    const statusEl = document.getElementById('enrollmentDownloadStatus');
    const emailBtn = document.getElementById('emailEnrollmentBtn');
    if (statusEl) statusEl.textContent = 'Generating...';
    if (emailBtn) emailBtn.disabled = true;

    // Step 1: Download the Excel file
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
        // Trigger file download
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        let namesPart = selected.map(function(s) { return s.replace(/\s+/g, ''); }).join('_');
        if (namesPart.length > 80) namesPart = namesPart.substring(0, 80);
        a.download = 'Enrollment_' + namesPart + '.xlsx';
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);

        if (statusEl) statusEl.textContent = 'Downloaded!';
        setTimeout(function() { if (statusEl) statusEl.textContent = ''; }, 3000);

        // Step 2: Open mailto with pre-filled subject and body
        const programList = selected.join(', ');
        const subject = 'Camp Sol Taplin - Enrollment Roster';
        const body = 'Hi,\n\nPlease find attached the enrollment roster for ' + programList + '.\n\nBest regards,\nCamp Sol Taplin';
        const mailtoUrl = 'mailto:?subject=' + encodeURIComponent(subject) + '&body=' + encodeURIComponent(body);

        setTimeout(function() {
            window.location.href = mailtoUrl;
        }, 500);
    })
    .catch(function(err) {
        if (statusEl) statusEl.textContent = 'Error: ' + err.message;
        setTimeout(function() { if (statusEl) statusEl.textContent = ''; }, 5000);
    })
    .finally(function() {
        updateEnrollmentDownloadButton();
    });
}

// Upload Share Group With CSV
function uploadShareGroup() {
    const fileInput = document.getElementById('shareGroupFile');
    const status = document.getElementById('shareGroupStatus');
    if (!fileInput || !fileInput.files.length) {
        if (status) status.textContent = 'Please select a CSV file first.';
        return;
    }
    const formData = new FormData();
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
            const participants = data.participants || [];
            renderParticipantsTable(participants, list, program, week);
        })
        .catch(function(err) {
            // Fallback to pre-loaded data (without emails)
            if (window.participantsData) {
                const programData = window.participantsData[program];
                const participants = programData ? (programData[String(week)] || []) : [];
                renderParticipantsTable(participants, list, program, week);
            } else {
                list.innerHTML = '<div class="participant-count">Failed to load participants</div>';
            }
        });
}

function copyAllEmails(btn) {
    const emails = btn.getAttribute('data-emails');
    if (navigator.clipboard) {
        navigator.clipboard.writeText(emails.replace(/,/g, '; ')).then(function() {
            const original = btn.textContent;
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
        const ta = document.createElement('textarea');
        ta.value = emails.replace(/,/g, '; ');
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        const original = btn.textContent;
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

// Convert month/day to day-of-year (using 2024 as reference for leap year safety)
function monthDayToDayOfYear(month, day) {
    const ref = new Date(2024, month - 1, day);
    const yearStart = new Date(2024, 0, 1);
    return Math.floor((ref - yearStart) / (1000 * 60 * 60 * 24));
}

// Populate day dropdown (1-31) for a given month
function populateDays(monthSelectId, daySelectId) {
    const monthVal = parseInt(document.getElementById(monthSelectId).value);
    const daySelect = document.getElementById(daySelectId);
    const currentDay = daySelect.value;

    daySelect.innerHTML = '<option value="">Day</option>';
    if (!monthVal) return;

    const daysInMonth = new Date(2024, monthVal, 0).getDate();
    for (let d = 1; d <= daysInMonth; d++) {
        const opt = document.createElement('option');
        opt.value = d;
        opt.textContent = d;
        if (parseInt(currentDay) === d) opt.selected = true;
        daySelect.appendChild(opt);
    }
}

function getFilteredIndices() {
    const startMonth = document.getElementById('startMonth');
    const startDayEl = document.getElementById('startDay');
    const endMonth = document.getElementById('endMonth');
    const endDayEl = document.getElementById('endDay');

    const sm = startMonth ? parseInt(startMonth.value) : 0;
    const sd = startDayEl ? parseInt(startDayEl.value) : 0;
    const em = endMonth ? parseInt(endMonth.value) : 0;
    const ed = endDayEl ? parseInt(endDayEl.value) : 0;

    // Need at least a complete from or to (month+day)
    const hasStart = sm > 0 && sd > 0;
    const hasEnd = em > 0 && ed > 0;

    if (!hasStart && !hasEnd) return null; // No filter

    let startDayOfYear = 0;
    let endDayOfYear = 366;

    if (hasStart) startDayOfYear = monthDayToDayOfYear(sm, sd);
    if (hasEnd) endDayOfYear = monthDayToDayOfYear(em, ed);

    const indices = [];
    for (let i = 0; i < fullChartDays.length; i++) {
        if (fullChartDays[i] >= startDayOfYear && fullChartDays[i] <= endDayOfYear) {
            indices.push(i);
        }
    }
    return indices;
}

function updateDateChart() {
    // Populate day dropdowns when month changes
    populateDays('startMonth', 'startDay');
    populateDays('endMonth', 'endDay');

    // Always update the summary table based on the "To" date filter
    updateSummaryTable();

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

// Look up cumulative stats at a given day-of-year from daily data
function getCumulativeAtDayOfYear(dailyData, targetDayOfYear) {
    if (!dailyData || dailyData.length === 0) return null;
    let result = null;
    for (const day of dailyData) {
        // Parse date string as local (avoid UTC timezone shift with new Date("YYYY-MM-DD"))
        const parts = day.date.split('-');
        const year = parseInt(parts[0]);
        const month = parseInt(parts[1]) - 1;
        const d = parseInt(parts[2]);
        const dayDate = new Date(year, month, d);
        const yearStart = new Date(year, 0, 1);
        const dayOfYear = Math.floor((dayDate - yearStart) / (1000 * 60 * 60 * 24));
        if (dayOfYear <= targetDayOfYear) {
            result = {
                campers: day.cumulative_campers,
                weeks: day.cumulative_weeks
            };
        } else {
            break;
        }
    }
    return result;
}

// Look up CT unique campers at a given day-of-year from CT daily data
function getCTAtDayOfYear(ctDailyData, targetDayOfYear) {
    if (!ctDailyData || ctDailyData.length === 0) return null;
    let result = null;
    for (const entry of ctDailyData) {
        const parts = entry.date.split('-');
        const year = parseInt(parts[0]);
        const month = parseInt(parts[1]) - 1;
        const d = parseInt(parts[2]);
        const dayDate = new Date(year, month, d);
        const yearStart = new Date(year, 0, 1);
        const dayOfYear = Math.floor((dayDate - yearStart) / (1000 * 60 * 60 * 24));
        if (dayOfYear <= targetDayOfYear) {
            result = entry.ct_campers;
        } else {
            break;
        }
    }
    return result;
}

// Update the Year-over-Year Enrollment Summary table based on date filter
function updateSummaryTable() {
    const em = parseInt(document.getElementById('endMonth')?.value) || 0;
    const ed = parseInt(document.getElementById('endDay')?.value) || 0;
    const hasEndDate = em > 0 && ed > 0;

    const labelEl = document.getElementById('summaryDateLabel');
    const fullStats = window.fullSeasonStats || {};

    if (!hasEndDate) {
        // No filter - show full season totals
        if (labelEl) labelEl.textContent = '(Full Season)';
        for (const year of ['2024', '2025', '2026']) {
            const stats = fullStats[year] || {};
            setCell('stats-' + year + '-campers', stats.campers || 0);
            setCell('stats-' + year + '-weeks', stats.weeks || 0);
            setCell('stats-' + year + '-ct', stats.ct || 0);
        }
        return;
    }

    // Filter active - compute day-of-year for the end date
    const endDayOfYear = monthDayToDayOfYear(em, ed);
    const monthNames = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    if (labelEl) labelEl.textContent = '(as of ' + monthNames[em] + ' ' + ed + ')';

    // Update each year's stats using daily data
    const yearDailyMap = {
        '2024': window.dateStats2024 || [],
        '2025': window.dateStats2025 || [],
        '2026': window.dateStats2026 || []
    };

    for (const year of ['2024', '2025', '2026']) {
        const cumulative = getCumulativeAtDayOfYear(yearDailyMap[year], endDayOfYear);
        if (cumulative) {
            setCell('stats-' + year + '-campers', cumulative.campers);
            setCell('stats-' + year + '-weeks', cumulative.weeks);
        } else {
            setCell('stats-' + year + '-campers', '-');
            setCell('stats-' + year + '-weeks', '-');
        }
        // CT: use date-filtered CT daily data if available, otherwise full season
        const ctDailyMap = {'2024': window.ctDaily2024 || [], '2025': window.ctDaily2025 || [], '2026': []};
        const ctVal = getCTAtDayOfYear(ctDailyMap[year], endDayOfYear);
        setCell('stats-' + year + '-ct', ctVal !== null ? ctVal : (fullStats[year] || {}).ct || '-');
    }
}

function setCell(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
}

function resetDateFilters() {
    ['startMonth', 'startDay', 'endMonth', 'endDay'].forEach(function(id) {
        const el = document.getElementById(id);
        if (el) el.value = '';
    });
    // Reset day dropdowns
    populateDays('startMonth', 'startDay');
    populateDays('endMonth', 'endDay');
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

// ==================== FINANCE TAB FUNCTIONS ====================

let revenueTimelineChartInstance = null;
let paymentMethodChartInstance = null;
let camperDistributionChartInstance = null;
let financeChartsInitialized = false;

function initFinanceCharts() {
    if (financeChartsInitialized) return;

    // CampMinder charts need financeData
    if (window.financeData) {
        populateFinanceProgramTable();
        initRevenueTimelineChart();
        initPaymentMethodChart();
        initCamperDistributionChart();
    }

    // Budget chart works with budgetData (independent of financeData)
    if (window.budgetData && window.budgetData.po && window.budgetData.po.budget_vs_actual) {
        initBudgetVsActualChart();
    }

    financeChartsInitialized = true;
}

function populateFinanceProgramTable() {
    const tbody = document.getElementById('financeByProgramBody');
    if (!tbody || !window.financeData || !window.financeData.by_enrollment_category) return;

    let html = '';
    window.financeData.by_enrollment_category.forEach(function(cat) {
        html += '<tr>';
        html += '<td style="font-weight:600;">' + (cat.emoji || '') + ' ' + cat.category + '</td>';
        html += '<td>' + (cat.enrolled || '-') + '</td>';
        html += '<td>' + (cat.fte ? cat.fte.toFixed(1) : '-') + '</td>';
        html += '<td>$' + formatNumber(cat.gross_revenue || 0) + '</td>';
        html += '<td class="finance-negative">$' + formatNumber(Math.abs(cat.discounts || 0)) + '</td>';
        html += '<td style="font-weight:600;">$' + formatNumber(cat.net_revenue || 0) + '</td>';
        html += '<td>-</td>';
        html += '<td>$' + formatNumber(cat.revenue_per_ftc || 0) + '</td>';
        html += '</tr>';
    });

    // Also add a total row
    const totals = window.financeData.summary;
    html += '<tr style="font-weight:700; background:var(--bg);">';
    html += '<td>TOTAL</td>';
    html += '<td>' + (totals.total_campers || '-') + '</td>';
    html += '<td>' + (totals.total_fte ? totals.total_fte.toFixed(1) : '-') + '</td>';
    html += '<td>$' + formatNumber(totals.gross_revenue || 0) + '</td>';
    html += '<td class="finance-negative">$' + formatNumber(Math.abs(totals.total_discounts || 0)) + '</td>';
    html += '<td>$' + formatNumber(totals.net_revenue || 0) + '</td>';
    html += '<td>$' + formatNumber(totals.revenue_per_camper || 0) + '</td>';
    html += '<td>$' + formatNumber(totals.revenue_per_ftc || 0) + '</td>';
    html += '</tr>';

    tbody.innerHTML = html;
}

function initRevenueTimelineChart() {
    const canvas = document.getElementById('revenueTimelineChart');
    if (!canvas || !window.financeData || !window.financeData.timeline) return;

    if (revenueTimelineChartInstance) {
        revenueTimelineChartInstance.destroy();
    }

    const timeline = window.financeData.timeline;
    const labels = timeline.map(function(d) { return d.date; });
    const cumulativeRevenue = timeline.map(function(d) { return d.cumulative_revenue; });

    const datasets = [{
        label: '2026 Revenue',
        data: cumulativeRevenue,
        borderColor: '#10B981',
        backgroundColor: 'rgba(16, 185, 129, 0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        borderWidth: 2.5
    }];

    // Add historical timeline if available
    if (window.financeData.historical_2025 && window.financeData.historical_2025.timeline) {
        const hist25 = window.financeData.historical_2025.timeline;
        datasets.push({
            label: '2025 Revenue',
            data: hist25.map(function(d) { return d.cumulative_revenue; }),
            borderColor: '#6B7280',
            backgroundColor: 'transparent',
            borderDash: [5, 5],
            fill: false,
            tension: 0.3,
            pointRadius: 0,
            borderWidth: 1.5
        });
    }

    if (window.financeData.historical_2024 && window.financeData.historical_2024.timeline) {
        const hist24 = window.financeData.historical_2024.timeline;
        datasets.push({
            label: '2024 Revenue',
            data: hist24.map(function(d) { return d.cumulative_revenue; }),
            borderColor: '#D1D5DB',
            backgroundColor: 'transparent',
            borderDash: [3, 3],
            fill: false,
            tension: 0.3,
            pointRadius: 0,
            borderWidth: 1.5
        });
    }

    revenueTimelineChartInstance = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: { labels: labels, datasets: datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { position: 'top' },
                tooltip: {
                    callbacks: {
                        label: function(ctx) {
                            return ctx.dataset.label + ': $' + formatNumber(ctx.parsed.y);
                        }
                    }
                }
            },
            scales: {
                x: {
                    title: { display: true, text: 'Date' },
                    ticks: { maxTicksLimit: 12 }
                },
                y: {
                    title: { display: true, text: 'Cumulative Revenue ($)' },
                    ticks: {
                        callback: function(value) {
                            return '$' + formatNumber(value);
                        }
                    }
                }
            }
        }
    });
}

function initPaymentMethodChart() {
    const canvas = document.getElementById('paymentMethodChart');
    if (!canvas || !window.financeData || !window.financeData.by_payment_method) return;

    if (paymentMethodChartInstance) {
        paymentMethodChartInstance.destroy();
    }

    const methods = window.financeData.by_payment_method;
    const labels = methods.map(function(m) { return m.method; });
    const amounts = methods.map(function(m) { return m.amount; });

    const colors = ['#10B981', '#3B82F6', '#8B5CF6', '#F59E0B', '#EF4444', '#06B6D4', '#EC4899', '#6366F1'];

    paymentMethodChartInstance = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Amount',
                data: amounts,
                backgroundColor: colors.slice(0, labels.length),
                borderRadius: 6,
                barThickness: 30
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: function(ctx) {
                            return '$' + formatNumber(ctx.parsed.x);
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        callback: function(value) {
                            return '$' + formatNumber(value);
                        }
                    }
                }
            }
        }
    });
}

function initCamperDistributionChart() {
    const canvas = document.getElementById('camperDistributionChart');
    if (!canvas || !window.financeData || !window.financeData.distribution) return;

    if (camperDistributionChartInstance) {
        camperDistributionChartInstance.destroy();
    }

    const dist = window.financeData.distribution;

    camperDistributionChartInstance = new Chart(canvas.getContext('2d'), {
        type: 'doughnut',
        data: {
            labels: ['Full Price', 'Partial Discount', 'Heavy Discount', 'Subsidized'],
            datasets: [{
                data: [dist.full_price, dist.partial_discount, dist.heavy_discount, dist.subsidized],
                backgroundColor: ['#10B981', '#3B82F6', '#F59E0B', '#EF4444'],
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '55%',
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: function(ctx) {
                            const total = ctx.dataset.data.reduce(function(a, b) { return a + b; }, 0);
                            const pct = total > 0 ? ((ctx.parsed / total) * 100).toFixed(1) : '0';
                            return ctx.label + ': ' + ctx.parsed + ' campers (' + pct + '%)';
                        }
                    }
                }
            }
        }
    });
}

async function refreshFinanceData() {
    try {
        showToast('Refreshing financial data...', 'success');
        const response = await fetch('/api/finance/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        const data = await response.json();
        if (data.success) {
            showToast('Financial data refreshed! Reloading...', 'success');
            setTimeout(function() { window.location.reload(); }, 1500);
        } else {
            showToast('Error: ' + data.error, 'error');
        }
    } catch (error) {
        showToast('Failed to refresh finance data: ' + error.message, 'error');
    }
}

function formatNumber(num) {
    if (num === null || num === undefined) return '0';
    return Math.round(num).toLocaleString('en-US');
}

// ==================== BUDGET VS ACTUAL FUNCTIONS ====================

let budgetVsActualChartInstance = null;

function initBudgetVsActualChart() {
    const canvas = document.getElementById('budgetVsActualChart');
    if (!canvas || !window.budgetData || !window.budgetData.po || !window.budgetData.po.budget_vs_actual) return;

    if (budgetVsActualChartInstance) {
        budgetVsActualChartInstance.destroy();
    }

    const cats = window.budgetData.po.budget_vs_actual.categories;
    // Filter to only categories with budget > 0 and exclude Salaries (too large, skews chart)
    const filtered = cats.filter(function(c) { return c.budgeted > 0 && c.category !== 'Salaries & Benefits'; });

    const labels = filtered.map(function(c) { return c.category; });
    const budgetAmounts = filtered.map(function(c) { return c.budgeted; });
    const actualAmounts = filtered.map(function(c) { return c.actual; });

    budgetVsActualChartInstance = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Budget',
                    data: budgetAmounts,
                    backgroundColor: 'rgba(99, 102, 241, 0.7)',
                    borderColor: '#6366F1',
                    borderWidth: 1,
                    borderRadius: 4,
                    barPercentage: 0.8,
                    categoryPercentage: 0.7
                },
                {
                    label: 'Actual (PO)',
                    data: actualAmounts,
                    backgroundColor: actualAmounts.map(function(a, i) {
                        const pct = budgetAmounts[i] > 0 ? (a / budgetAmounts[i] * 100) : 0;
                        if (pct > 100) return 'rgba(239, 68, 68, 0.7)';
                        if (pct > 80) return 'rgba(245, 158, 11, 0.7)';
                        return 'rgba(16, 185, 129, 0.7)';
                    }),
                    borderColor: actualAmounts.map(function(a, i) {
                        const pct = budgetAmounts[i] > 0 ? (a / budgetAmounts[i] * 100) : 0;
                        if (pct > 100) return '#EF4444';
                        if (pct > 80) return '#F59E0B';
                        return '#10B981';
                    }),
                    borderWidth: 1,
                    borderRadius: 4,
                    barPercentage: 0.8,
                    categoryPercentage: 0.7
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'top' },
                tooltip: {
                    callbacks: {
                        label: function(ctx) {
                            return ctx.dataset.label + ': $' + formatNumber(ctx.parsed.y);
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: { font: { size: 11 }, maxRotation: 45 }
                },
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return '$' + formatNumber(value);
                        }
                    }
                }
            }
        }
    });
}

function uploadPOFile(file) {
    if (!file) return;

    if (!file.name.toLowerCase().endsWith('.xlsx')) {
        showToast('Please upload a .xlsx file', 'error');
        return;
    }

    showToast('Uploading PO file...', 'success');

    const formData = new FormData();
    formData.append('file', file);

    fetch('/api/upload-po', {
        method: 'POST',
        body: formData
    })
    .then(function(response) { return response.json(); })
    .then(function(data) {
        if (data.success) {
            showToast(data.message, 'success');
            // Reload to refresh the full page with new data
            setTimeout(function() { window.location.reload(); }, 1500);
        } else {
            showToast('Error: ' + data.error, 'error');
        }
    })
    .catch(function(error) {
        showToast('Upload failed: ' + error.message, 'error');
    });

    // Clear the file input so the same file can be re-uploaded
    document.getElementById('poFileInput').value = '';
}

// ==================== ADMIN ATTENDANCE ====================

let attRefreshInterval = null;
let attInitialized = false;

function initAttendanceView() {
    if (attInitialized) {
        // Already initialized — just refresh
        loadAttendanceSummary();
        return;
    }
    attInitialized = true;

    // Set date picker to today
    const dateInput = document.getElementById('att-admin-date');
    if (dateInput) {
        dateInput.value = new Date().toISOString().split('T')[0];
    }

    // Load summary
    loadAttendanceSummary();

    // Auto-refresh every 30s
    if (attRefreshInterval) clearInterval(attRefreshInterval);
    attRefreshInterval = setInterval(function() {
        // Only refresh if attendance view is active
        const view = document.getElementById('attendance-view');
        if (view && view.classList.contains('active')) {
            loadAttendanceSummary();
        }
    }, 30000);

    // Load unit leader assignments
    loadAssignments();

    // Load attendance trend chart
    loadAttendanceTrends();
}

function loadAttendanceSummary() {
    const dateInput = document.getElementById('att-admin-date');
    const dateVal = dateInput ? dateInput.value : '';
    const url = '/api/attendance/summary' + (dateVal ? '?date=' + dateVal : '');

    fetch(url).then(function(r) { return r.json(); }).then(function(data) {
        // Update KPIs
        document.getElementById('att-kpi-present').textContent = data.totals.present || 0;
        document.getElementById('att-kpi-absent').textContent = data.totals.absent || 0;
        document.getElementById('att-kpi-late').textContent = data.totals.late || 0;
        document.getElementById('att-kpi-early').textContent = data.totals.early_pickup || 0;

        // Update refresh badge
        const badge = document.getElementById('att-refresh-badge');
        if (badge) {
            const now = new Date();
            badge.textContent = 'Updated ' + now.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
        }

        // Render grid
        renderAttendanceGrid(data);
    }).catch(function(err) {
        console.error('Attendance summary error:', err);
    });
}

function renderAttendanceGrid(data) {
    const grid = document.getElementById('att-admin-grid');
    const empty = document.getElementById('att-admin-empty');
    if (!grid) return;

    const programs = data.programs || [];

    if (programs.length === 0) {
        grid.style.display = 'none';
        empty.style.display = 'block';
        return;
    }
    grid.style.display = 'table';
    empty.style.display = 'none';

    // Simplified header — single daily attendance column + KC columns
    const headerHtml = '<tr><th>Program</th><th>Enrolled</th><th>Attendance</th><th>Present</th><th>Absent</th><th>Late</th><th>Early</th></tr>';

    // Build rows — use only checkpoint 1 (Morning = daily) for main stats
    let bodyHtml = '';
    programs.forEach(function(prog) {
        // Find checkpoint 1 (Morning/Daily) stats
        let dailyStats = null;
        (prog.checkpoints || []).forEach(function(cp) {
            if (cp.checkpoint_id === 1) dailyStats = cp;
        });
        if (!dailyStats) {
            dailyStats = { marked: 0, total: prog.total_campers, completion: 0, present: 0, absent: 0, late: 0, early_pickup: 0 };
        }

        const pct = dailyStats.completion;
        let cellClass = 'att-cell';
        if (pct >= 100) cellClass += ' complete';
        else if (pct > 0) cellClass += ' partial';
        else cellClass += ' empty';

        bodyHtml += '<tr style="cursor:pointer;" onclick="loadAttendanceDetail(\'' + prog.program.replace(/'/g, "\\'") + '\')">';
        bodyHtml += '<td class="att-prog-name">' + prog.program + '</td>';
        bodyHtml += '<td class="att-cell-center">' + prog.total_campers + '</td>';
        bodyHtml += '<td class="' + cellClass + ' att-cell-center">';
        bodyHtml += '<div class="att-cell-pct">' + pct + '%</div>';
        bodyHtml += '<div class="att-cell-count">' + dailyStats.marked + '/' + dailyStats.total + '</div>';
        bodyHtml += '</td>';
        bodyHtml += '<td class="att-status-present">' + (dailyStats.present || 0) + '</td>';
        bodyHtml += '<td class="att-status-absent">' + (dailyStats.absent || 0) + '</td>';
        bodyHtml += '<td class="att-status-late">' + (dailyStats.late || 0) + '</td>';
        bodyHtml += '<td class="att-status-early">' + (dailyStats.early_pickup || 0) + '</td>';
        bodyHtml += '</tr>';
    });

    grid.querySelector('thead').innerHTML = headerHtml;
    grid.querySelector('tbody').innerHTML = bodyHtml;
}

function loadAttendanceDetail(program) {
    const dateInput = document.getElementById('att-admin-date');
    const dateVal = dateInput ? dateInput.value : '';
    const url = '/api/attendance/detail/' + encodeURIComponent(program) + (dateVal ? '?date=' + dateVal : '');

    document.getElementById('att-detail-title').textContent = program + ' — Detail';
    document.getElementById('att-admin-detail').style.display = 'block';
    document.getElementById('att-detail-body').innerHTML = '<div style="text-align:center;padding:20px;color:#6B7280;">Loading...</div>';

    fetch(url).then(function(r) { return r.json(); }).then(function(data) {
        const campers = data.campers || [];

        // Simplified detail: Status (checkpoint 1) + KC Before (cp 4) + KC After (cp 5)
        let html = '<table class="att-detail-table"><thead><tr>';
        html += '<th>Camper</th><th>Status</th><th>KC Before</th><th>KC After</th>';
        html += '</tr></thead><tbody>';

        campers.forEach(function(c) {
            html += '<tr><td style="font-weight:500;white-space:nowrap;">' + c.name + '</td>';

            // Main status (checkpoint 1)
            const att = c.attendance['1'] || {};
            const status = att.status || '—';
            let cls = 'att-status-badge';
            if (status === 'present') cls += ' present';
            else if (status === 'absent') cls += ' absent';
            else if (status === 'late') cls += ' late';
            else if (status === 'early_pickup') cls += ' early';
            const statusLabels = { present: '✓ Present', absent: '✗ Absent', late: 'LA', early_pickup: 'EP' };
            const label = statusLabels[status] || '—';
            html += '<td style="text-align:center;"><span class="' + cls + '">' + label + '</span></td>';

            // KC Before (checkpoint 4)
            const kcBefore = c.attendance['4'] || {};
            const kcbStatus = kcBefore.status || '';
            if (kcbStatus === 'present') {
                html += '<td style="text-align:center;"><span class="att-status-badge kc">KC ✓</span></td>';
            } else if (kcbStatus) {
                html += '<td style="text-align:center;"><span class="att-status-badge">—</span></td>';
            } else {
                html += '<td style="text-align:center;color:#9CA3AF;">—</td>';
            }

            // KC After (checkpoint 5)
            const kcAfter = c.attendance['5'] || {};
            const kcaStatus = kcAfter.status || '';
            if (kcaStatus === 'present') {
                html += '<td style="text-align:center;"><span class="att-status-badge kc">KC ✓</span></td>';
            } else if (kcaStatus) {
                html += '<td style="text-align:center;"><span class="att-status-badge">—</span></td>';
            } else {
                html += '<td style="text-align:center;color:#9CA3AF;">—</td>';
            }

            html += '</tr>';
        });

        html += '</tbody></table>';
        if (campers.length === 0) html = '<div style="text-align:center;padding:20px;color:#6B7280;">No campers enrolled.</div>';

        document.getElementById('att-detail-body').innerHTML = html;
    }).catch(function(err) {
        document.getElementById('att-detail-body').innerHTML = '<div style="color:#EF4444;">Error loading detail.</div>';
    });
}

// ---- Unit Leader Assignment Management ----

function loadAssignments() {
    fetch('/api/attendance/assignments').then(function(r) { return r.json(); }).then(function(data) {
        const container = document.getElementById('att-assignments-body');
        if (!container) return;

        const assignments = data.assignments || {};
        const keys = Object.keys(assignments);
        if (keys.length === 0) {
            container.innerHTML = '<div style="color:#6B7280;font-size:13px;padding:8px 0;">No assignments yet.</div>';
        } else {
            let html = '<div class="att-assign-list">';
            keys.forEach(function(username) {
                const progs = assignments[username];
                html += '<div class="att-assign-row">';
                html += '<strong>' + username + '</strong>: ';
                progs.forEach(function(prog) {
                    html += '<span class="att-assign-tag">' + prog + ' <button class="att-assign-remove" onclick="removeAssignment(\'' + username.replace(/'/g, "\\'") + '\', \'' + prog.replace(/'/g, "\\'") + '\')">&times;</button></span>';
                });
                html += '</div>';
            });
            html += '</div>';
            container.innerHTML = html;
        }

        // Populate user dropdown (fetch all users with unit_leader role)
        return fetch('/api/users');
    }).then(function(r) { return r ? r.json() : null; }).then(function(usersData) {
        if (!usersData) return;
        const userSelect = document.getElementById('att-assign-user');
        if (!userSelect) return;
        userSelect.innerHTML = '<option value="">Select user...</option>';
        (usersData.users || []).forEach(function(u) {
            userSelect.innerHTML += '<option value="' + u.username + '">' + u.username + ' (' + u.role + ')</option>';
        });

        // Populate program dropdown from enrollment data
        const progSelect = document.getElementById('att-assign-program');
        if (!progSelect) return;
        progSelect.innerHTML = '<option value="">Select program...</option>';
        const progNames = Object.keys(window.participantsData || {}).sort();
        progNames.forEach(function(p) {
            progSelect.innerHTML += '<option value="' + p + '">' + p + '</option>';
        });
    }).catch(function(err) {
        console.error('Load assignments error:', err);
    });
}

function addAssignment() {
    const username = document.getElementById('att-assign-user').value;
    const program = document.getElementById('att-assign-program').value;
    if (!username || !program) {
        showToast('Select both user and program', 'error');
        return;
    }
    fetch('/api/attendance/assignments', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: username, program_name: program })
    }).then(function(r) { return r.json(); }).then(function(data) {
        if (data.success || data.message) {
            showToast('Assignment added');
            loadAssignments();
        } else {
            showToast('Error: ' + (data.error || 'Unknown'), 'error');
        }
    });
}

function removeAssignment(username, program) {
    fetch('/api/attendance/assignments', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: username, program_name: program })
    }).then(function(r) { return r.json(); }).then(function(data) {
        if (data.success) {
            showToast('Assignment removed');
            loadAssignments();
        }
    });
}

// ==================== ATTENDANCE TREND CHART ====================
// CAMP_WEEK_DATES_JS is injected via <script> in dashboard.html from Python's CAMP_WEEK_DATES

function getCurrentCampWeekJS() {
    const today = new Date().toISOString().split('T')[0];
    for (const wk in CAMP_WEEK_DATES_JS) {
        if (today >= CAMP_WEEK_DATES_JS[wk][0] && today <= CAMP_WEEK_DATES_JS[wk][1]) {
            return parseInt(wk);
        }
    }
    return null;
}

function setTrendRange(preset) {
    const startInput = document.getElementById('att-trend-start');
    const endInput = document.getElementById('att-trend-end');
    if (!startInput || !endInput) return;

    const today = new Date().toISOString().split('T')[0];
    const cw = getCurrentCampWeekJS();

    if (preset === 'this_week') {
        if (cw && CAMP_WEEK_DATES_JS[cw]) {
            startInput.value = CAMP_WEEK_DATES_JS[cw][0];
            endInput.value = today;
        } else {
            // Fallback: last 5 days
            const d = new Date();
            d.setDate(d.getDate() - 4);
            startInput.value = d.toISOString().split('T')[0];
            endInput.value = today;
        }
    } else if (preset === 'last_week') {
        const prevWk = cw ? cw - 1 : null;
        if (prevWk && CAMP_WEEK_DATES_JS[prevWk]) {
            startInput.value = CAMP_WEEK_DATES_JS[prevWk][0];
            endInput.value = CAMP_WEEK_DATES_JS[prevWk][1];
        } else {
            const d2 = new Date();
            d2.setDate(d2.getDate() - 11);
            const d3 = new Date();
            d3.setDate(d3.getDate() - 7);
            startInput.value = d2.toISOString().split('T')[0];
            endInput.value = d3.toISOString().split('T')[0];
        }
    } else if (preset === 'all_season') {
        startInput.value = CAMP_WEEK_DATES_JS[1][0]; // First day of season
        endInput.value = CAMP_WEEK_DATES_JS[9][1];   // Last day of season
    }

    // Highlight active button
    document.querySelectorAll('.att-trend-quick-btn').forEach(function(btn) {
        btn.style.background = '';
        btn.style.color = '';
        btn.style.fontWeight = '';
    });
    const activeBtn = document.querySelector('.att-trend-quick-btn[onclick*="' + preset + '"]');
    if (activeBtn) {
        activeBtn.style.background = '#0D9488';
        activeBtn.style.color = 'white';
        activeBtn.style.fontWeight = '600';
    }

    loadAttendanceTrends();
}

function loadAttendanceTrends() {
    const startInput = document.getElementById('att-trend-start');
    const endInput = document.getElementById('att-trend-end');
    const start = startInput ? startInput.value : '';
    const end = endInput ? endInput.value : '';

    let url = '/api/attendance/trends';
    const params = [];
    if (start) params.push('start=' + start);
    if (end) params.push('end=' + end);
    if (params.length) url += '?' + params.join('&');

    fetch(url).then(function(r) { return r.json(); }).then(function(data) {
        // Update date inputs with server response
        if (startInput && data.start) startInput.value = data.start;
        if (endInput && data.end) endInput.value = data.end;

        const emptyEl = document.getElementById('att-trend-empty');
        const canvas = document.getElementById('attendanceTrendChart');

        if (!data.dates || data.dates.length === 0) {
            if (canvas) canvas.style.display = 'none';
            if (emptyEl) emptyEl.style.display = 'block';
            return;
        }

        if (canvas) canvas.style.display = 'block';
        if (emptyEl) emptyEl.style.display = 'none';

        renderAttendanceTrendChart(data.dates);
    }).catch(function(err) {
        console.error('Load attendance trends error:', err);
    });
}

function renderAttendanceTrendChart(dates) {
    const canvas = document.getElementById('attendanceTrendChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    if (attendanceTrendChartInstance) {
        attendanceTrendChartInstance.destroy();
    }

    // Format labels as "Mon 6/8"
    const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const labels = dates.map(function(d) {
        const dt = new Date(d.date + 'T12:00:00');
        const dayName = dayNames[dt.getDay()];
        return dayName + ' ' + (dt.getMonth() + 1) + '/' + dt.getDate();
    });

    const presentData = dates.map(function(d) { return d.present; });
    const lateData = dates.map(function(d) { return d.late; });
    const absentData = dates.map(function(d) { return d.absent; });
    const rateData = dates.map(function(d) { return d.rate; });

    attendanceTrendChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Present',
                    data: presentData,
                    backgroundColor: 'rgba(34, 197, 94, 0.8)',
                    borderColor: '#22C55E',
                    borderWidth: 1,
                    stack: 'counts',
                    order: 2
                },
                {
                    label: 'Late',
                    data: lateData,
                    backgroundColor: 'rgba(245, 158, 11, 0.8)',
                    borderColor: '#F59E0B',
                    borderWidth: 1,
                    stack: 'counts',
                    order: 2
                },
                {
                    label: 'Absent',
                    data: absentData,
                    backgroundColor: 'rgba(239, 68, 68, 0.8)',
                    borderColor: '#EF4444',
                    borderWidth: 1,
                    stack: 'counts',
                    order: 2
                },
                {
                    label: 'Attendance Rate %',
                    data: rateData,
                    type: 'line',
                    borderColor: '#0D9488',
                    backgroundColor: 'rgba(13, 148, 136, 0.1)',
                    borderWidth: 2.5,
                    pointRadius: 4,
                    pointBackgroundColor: '#0D9488',
                    tension: 0.3,
                    fill: false,
                    yAxisID: 'yRate',
                    order: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        usePointStyle: true,
                        padding: 16,
                        font: { family: "'DM Sans', sans-serif", size: 12 }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(17, 24, 39, 0.95)',
                    titleFont: { family: "'DM Sans', sans-serif", weight: '600' },
                    bodyFont: { family: "'DM Sans', sans-serif" },
                    padding: 12,
                    cornerRadius: 8,
                    callbacks: {
                        afterBody: function(context) {
                            const idx = context[0].dataIndex;
                            const d = dates[idx];
                            return 'Total Enrolled: ' + d.total_enrolled;
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: {
                        font: { family: "'DM Sans', sans-serif", size: 11 },
                        maxRotation: 45
                    }
                },
                y: {
                    stacked: true,
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Campers',
                        font: { family: "'DM Sans', sans-serif", weight: 'bold', size: 12 }
                    },
                    ticks: {
                        font: { family: "'DM Sans', sans-serif", size: 11 },
                        precision: 0
                    },
                    grid: { color: 'rgba(0,0,0,0.06)' }
                },
                yRate: {
                    position: 'right',
                    min: 0,
                    max: 100,
                    title: {
                        display: true,
                        text: 'Rate %',
                        font: { family: "'DM Sans', sans-serif", weight: 'bold', size: 12 }
                    },
                    ticks: {
                        font: { family: "'DM Sans', sans-serif", size: 11 },
                        callback: function(val) { return val + '%'; }
                    },
                    grid: { drawOnChartArea: false }
                }
            }
        }
    });
}

