/* ==================== ATTENDANCE — CLIENT LOGIC ==================== */

(function() {
    'use strict';

    // ---- State ----
    let programs = [];
    let campers = [];
    let selectedProgram = null;
    let selectedWeek = null;
    let selectedDate = null;       // YYYY-MM-DD string
    let isDayLocked = false;       // true after 5 PM for that date
    let weekDates = {};            // week# → {start, end}
    let saveTimers = {};

    // Fixed checkpoint ID = 1 (Morning) for all daily attendance
    const DAILY_CP = '1';
    // KC checkpoint IDs: 4 = KC Before, 5 = KC After
    const KC_BEFORE_CP = '4';
    const KC_AFTER_CP  = '5';
    // Early Pickup checkpoint ID = 6 (independent toggle, combinable with Present/Late)
    const EP_CP = '6';

    // ---- Init ----
    document.addEventListener('DOMContentLoaded', init);

    function init() {
        // Load week info + programs in parallel
        Promise.all([
            fetchJSON('/api/attendance/week-info'),
            fetchJSON('/api/attendance/my-programs')
        ]).then(([weekInfo, progData]) => {
            weekDates = weekInfo.weeks || {};

            // Set date picker to today
            var today = weekInfo.today || new Date().toISOString().split('T')[0];
            selectedDate = today;
            var dateInput = document.getElementById('att-date-input');
            if (dateInput) dateInput.value = today;

            // Header date
            var d = new Date(today + 'T12:00:00');
            var opts = { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' };
            document.getElementById('att-date').textContent = d.toLocaleDateString('en-US', opts);

            // Week info
            if (weekInfo.current_week) {
                selectedWeek = weekInfo.current_week;
                document.getElementById('week-info').textContent =
                    'Week ' + weekInfo.current_week + (weekInfo.is_camp_day ? ' — Camp in session' : '');
            } else {
                selectedWeek = findWeekForDate(today) || 1;
                document.getElementById('week-info').textContent = 'Off-season — Showing Week ' + selectedWeek + ' for testing';
            }

            // Check if day is locked
            updateLockStatus();

            // Programs
            programs = progData.programs || [];
            renderProgramGrid();
        }).catch(function(err) {
            console.error('Init error:', err);
            showToast('Failed to load data', 'error');
        });
    }

    // ---- Date Management ----
    function findWeekForDate(dateStr) {
        for (var w in weekDates) {
            if (dateStr >= weekDates[w].start && dateStr <= weekDates[w].end) {
                return parseInt(w);
            }
        }
        return null;
    }

    function updateLockStatus() {
        var now = new Date();
        var todayStr = now.toISOString().split('T')[0];

        if (selectedDate < todayStr) {
            // Past day — always locked
            isDayLocked = true;
        } else if (selectedDate === todayStr) {
            // Today — locked after 5 PM
            isDayLocked = now.getHours() >= 17;
        } else {
            // Future day — not locked
            isDayLocked = false;
        }

        // Show/hide lock badges
        var lockBadge = document.getElementById('att-lock-badge');
        var lockedBanner = document.getElementById('att-locked-banner');
        var markAllBtn = document.getElementById('mark-all-btn');
        var unmarkAllBtn = document.getElementById('unmark-all-btn');

        if (lockBadge) lockBadge.style.display = isDayLocked ? 'inline-flex' : 'none';
        if (lockedBanner) lockedBanner.style.display = isDayLocked ? 'flex' : 'none';
        if (markAllBtn) markAllBtn.style.display = isDayLocked ? 'none' : 'inline-flex';
        if (unmarkAllBtn) unmarkAllBtn.style.display = isDayLocked ? 'none' : 'inline-flex';
    }

    window.onDateChange = function() {
        var dateInput = document.getElementById('att-date-input');
        selectedDate = dateInput.value;

        // Recalculate week
        var w = findWeekForDate(selectedDate);
        if (w) {
            selectedWeek = w;
            document.getElementById('week-info').textContent = 'Week ' + w;
        } else {
            selectedWeek = 1;
            document.getElementById('week-info').textContent = 'Off-season — Showing Week 1';
        }

        // Update header date
        var d = new Date(selectedDate + 'T12:00:00');
        var opts = { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' };
        document.getElementById('att-date').textContent = d.toLocaleDateString('en-US', opts);

        updateLockStatus();

        // If already viewing a program, reload campers
        if (selectedProgram) {
            loadCampers();
        }
    };

    // ---- Program Grid ----
    function renderProgramGrid() {
        var grid = document.getElementById('program-grid');
        var empty = document.getElementById('no-programs');

        if (programs.length === 0) {
            grid.style.display = 'none';
            empty.style.display = 'block';
            return;
        }

        empty.style.display = 'none';
        grid.innerHTML = programs.map(function(prog) {
            return '<button class="program-btn" onclick="selectProgram(\'' + escHTML(prog).replace(/'/g, "\\'") + '\')">' +
                '<span>' + escHTML(prog) + '</span>' +
                '<span class="program-arrow"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="9 18 15 12 9 6"/></svg></span>' +
            '</button>';
        }).join('');
    }

    // ---- Select Program ----
    window.selectProgram = function(programName) {
        selectedProgram = programName;
        document.getElementById('selected-program-name').textContent = programName;

        // Show selected date label
        var d = new Date(selectedDate + 'T12:00:00');
        var opts = { month: 'short', day: 'numeric' };
        document.getElementById('att-selected-date-label').textContent = d.toLocaleDateString('en-US', opts);

        // Switch sections
        document.getElementById('program-selector').classList.remove('active');
        document.getElementById('camper-section').classList.add('active');

        updateLockStatus();
        loadCampers();
    };

    // ---- Go Back ----
    window.goBack = function() {
        selectedProgram = null;
        campers = [];
        document.getElementById('camper-section').classList.remove('active');
        document.getElementById('program-selector').classList.add('active');
    };

    // ---- Load Campers ----
    function loadCampers() {
        if (!selectedProgram || !selectedWeek) return;

        var list = document.getElementById('camper-list');
        list.innerHTML = '<div class="att-loading"><div class="att-spinner"></div> Loading campers...</div>';

        var url = '/api/attendance/campers/' + encodeURIComponent(selectedProgram) + '/' + selectedWeek + '?date=' + selectedDate;
        fetchJSON(url)
            .then(function(data) {
                campers = data.campers || [];
                document.getElementById('camper-count').textContent = campers.length + ' campers';
                renderCamperList();
            })
            .catch(function(err) {
                console.error('Load campers error:', err);
                list.innerHTML = '<div class="att-empty"><p>Failed to load campers</p></div>';
            });
    }

    // ---- Render Camper List ----
    function renderCamperList() {
        var list = document.getElementById('camper-list');
        if (campers.length === 0) {
            list.innerHTML = '<div class="att-empty"><p>No campers enrolled this week</p></div>';
            updateProgress();
            return;
        }

        var disabledAttr = isDayLocked ? ' disabled' : '';

        list.innerHTML = campers.map(function(c) {
            var att = (c.attendance && c.attendance[DAILY_CP]) || {};
            var currentStatus = att.status || '';

            // EP is now an independent flag stored on checkpoint 6
            var epAtt = (c.attendance && c.attendance[EP_CP]) || {};
            var hasEP = epAtt.status === 'early_pickup';

            // Border class — show EP combined indicator
            var borderClass = '';
            if (currentStatus === 'present' && hasEP) borderClass = 'marked-present-ep';
            else if (currentStatus === 'late' && hasEP) borderClass = 'marked-late-ep';
            else if (currentStatus === 'present') borderClass = 'marked';
            else if (currentStatus) borderClass = 'marked-' + currentStatus;

            var hasKC = !!c.has_kc;
            var kcBeforeAtt = (c.attendance && c.attendance[KC_BEFORE_CP]) || {};
            var kcAfterAtt  = (c.attendance && c.attendance[KC_AFTER_CP]) || {};
            var kcBeforeStatus = kcBeforeAtt.status || '';
            var kcAfterStatus  = kcAfterAtt.status || '';

            var html = '<div class="camper-row ' + borderClass + '" id="row-' + c.person_id + '" data-pid="' + c.person_id + '">';

            // Camper info area (name + optional sibling)
            html += '<div class="camper-info">';
            html += '<span class="camper-name">' + escHTML(c.name) + '</span>';
            if (c.youngest_sibling) {
                html += '<span class="camper-sibling">' +
                    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/></svg> ' +
                    escHTML(c.youngest_sibling.name) + ' <span class="sibling-program">(' + escHTML(c.youngest_sibling.program) + ')</span>' +
                '</span>';
            }
            html += '</div>';

            html += '<div class="status-buttons">';

            // KC Before (only if camper has KC)
            if (hasKC) {
                html += '<button class="status-btn kc_btn ' + (kcBeforeStatus === 'present' ? 'active' : '') + '"' +
                    ' onclick="setKC(\'' + c.person_id + '\', \'before\', this)"' +
                    ' title="Kid Connection - Before Care"' + disabledAttr + '>KC</button>';
            }

            // Main attendance buttons (Present, Absent, Late)
            html += '<button class="status-btn present ' + (currentStatus === 'present' ? 'active' : '') + '"' +
                ' onclick="setStatus(\'' + c.person_id + '\', \'present\', this)"' +
                ' title="Present"' + disabledAttr + '>&#10003;</button>';
            html += '<button class="status-btn absent ' + (currentStatus === 'absent' ? 'active' : '') + '"' +
                ' onclick="setStatus(\'' + c.person_id + '\', \'absent\', this)"' +
                ' title="Absent"' + disabledAttr + '>&#10007;</button>';
            html += '<button class="status-btn late ' + (currentStatus === 'late' ? 'active' : '') + '"' +
                ' onclick="setStatus(\'' + c.person_id + '\', \'late\', this)"' +
                ' title="Late Arrival"' + disabledAttr + '>LA</button>';

            // EP is now an independent toggle button (can combine with Present or Late)
            html += '<button class="status-btn early_pickup ' + (hasEP ? 'active' : '') + '"' +
                ' onclick="toggleEP(\'' + c.person_id + '\', this)"' +
                ' title="Early Pickup (combinable with Present/Late)"' + disabledAttr + '>EP</button>';

            // KC After (only if camper has KC)
            if (hasKC) {
                html += '<button class="status-btn kc_btn ' + (kcAfterStatus === 'present' ? 'active' : '') + '"' +
                    ' onclick="setKC(\'' + c.person_id + '\', \'after\', this)"' +
                    ' title="Kid Connection - After Care"' + disabledAttr + '>KC</button>';
            }

            html += '</div></div>';
            return html;
        }).join('');

        updateProgress();
    }

    // ---- Set Status (single camper — main attendance: Present/Absent/Late) ----
    // Clicking the same status again toggles it OFF (unmarks the camper)
    window.setStatus = function(personId, status, btnEl) {
        if (isDayLocked) {
            showToast('Day is locked — cannot modify', 'error');
            return;
        }

        var camper = campers.find(function(c) { return c.person_id === personId; });
        if (!camper) return;
        if (!camper.attendance) camper.attendance = {};

        // Toggle off: if clicking the same status that's already active, clear it
        var currentStatus = (camper.attendance[DAILY_CP] && camper.attendance[DAILY_CP].status) || '';
        var isUnmarking = (currentStatus === status);
        var newStatus = isUnmarking ? 'absent' : status;

        camper.attendance[DAILY_CP] = isUnmarking ? {} : { status: status };

        // If unmarking or setting Absent, also clear EP
        if (isUnmarking || status === 'absent') {
            camper.attendance[EP_CP] = {};
            var epTimerKey = personId + '_ep';
            if (saveTimers[epTimerKey]) clearTimeout(saveTimers[epTimerKey]);
            saveTimers[epTimerKey] = setTimeout(function() {
                saveSingleRecord(personId, 'absent', EP_CP);
            }, 300);
        }

        // Update UI
        var row = document.getElementById('row-' + personId);
        if (row) {
            var epAtt = (camper.attendance[EP_CP]) || {};
            var hasEP = epAtt.status === 'early_pickup';

            row.className = 'camper-row';
            if (!isUnmarking) {
                if (status === 'present' && hasEP) row.classList.add('marked-present-ep');
                else if (status === 'late' && hasEP) row.classList.add('marked-late-ep');
                else if (status === 'present') row.classList.add('marked');
                else if (status) row.classList.add('marked-' + status);
            }

            // Update only main buttons (not KC, not EP)
            row.querySelectorAll('.status-btn.present, .status-btn.absent, .status-btn.late').forEach(function(btn) {
                btn.classList.remove('active');
            });
            if (!isUnmarking && btnEl) btnEl.classList.add('active');

            // If unmarking or absent, also deactivate EP button
            if (isUnmarking || status === 'absent') {
                var epBtn = row.querySelector('.status-btn.early_pickup');
                if (epBtn) epBtn.classList.remove('active');
            }
        }

        updateProgress();

        // Debounced save
        var timerKey = personId + '_main';
        if (saveTimers[timerKey]) clearTimeout(saveTimers[timerKey]);
        saveTimers[timerKey] = setTimeout(function() {
            saveSingleRecord(personId, newStatus, DAILY_CP);
        }, 300);
    };

    // ---- Toggle EP (Early Pickup — independent toggle, combinable with Present/Late) ----
    window.toggleEP = function(personId, btnEl) {
        if (isDayLocked) {
            showToast('Day is locked — cannot modify', 'error');
            return;
        }

        var camper = campers.find(function(c) { return c.person_id === personId; });
        if (!camper) return;

        // Check main status — EP can only be set if Present or Late
        var mainAtt = (camper.attendance && camper.attendance[DAILY_CP]) || {};
        var mainStatus = mainAtt.status || '';
        if (mainStatus !== 'present' && mainStatus !== 'late') {
            showToast('Mark Present or Late first, then toggle EP', 'error');
            return;
        }

        if (!camper.attendance) camper.attendance = {};
        var currentEP = (camper.attendance[EP_CP] && camper.attendance[EP_CP].status) || '';
        var isTogglingOn = currentEP !== 'early_pickup';

        if (isTogglingOn) {
            camper.attendance[EP_CP] = { status: 'early_pickup' };
            if (btnEl) btnEl.classList.add('active');
        } else {
            camper.attendance[EP_CP] = {};
            if (btnEl) btnEl.classList.remove('active');
        }

        // Update row border to reflect combination
        var row = document.getElementById('row-' + personId);
        if (row) {
            row.className = 'camper-row';
            if (mainStatus === 'present' && isTogglingOn) row.classList.add('marked-present-ep');
            else if (mainStatus === 'late' && isTogglingOn) row.classList.add('marked-late-ep');
            else if (mainStatus === 'present') row.classList.add('marked');
            else if (mainStatus) row.classList.add('marked-' + mainStatus);
        }

        // Debounced save
        var timerKey = personId + '_ep';
        if (saveTimers[timerKey]) clearTimeout(saveTimers[timerKey]);
        saveTimers[timerKey] = setTimeout(function() {
            if (isTogglingOn) {
                saveSingleRecord(personId, 'early_pickup', EP_CP);
            } else {
                saveSingleRecord(personId, 'absent', EP_CP);  // Clear EP
            }
        }, 300);
    };

    // ---- Set KC Status ----
    window.setKC = function(personId, which, btnEl) {
        if (isDayLocked) {
            showToast('Day is locked — cannot modify', 'error');
            return;
        }

        var cpId = (which === 'before') ? KC_BEFORE_CP : KC_AFTER_CP;
        var camper = campers.find(function(c) { return c.person_id === personId; });
        if (!camper) return;

        if (!camper.attendance) camper.attendance = {};
        var current = (camper.attendance[cpId] && camper.attendance[cpId].status) || '';
        var newStatus = (current === 'present') ? '' : 'present';  // Toggle

        if (newStatus) {
            camper.attendance[cpId] = { status: 'present' };
            if (btnEl) btnEl.classList.add('active');
        } else {
            camper.attendance[cpId] = {};
            if (btnEl) btnEl.classList.remove('active');
        }

        // Save
        var timerKey = personId + '_kc_' + which;
        if (saveTimers[timerKey]) clearTimeout(saveTimers[timerKey]);
        saveTimers[timerKey] = setTimeout(function() {
            if (newStatus) {
                saveSingleRecord(personId, 'present', cpId);
            } else {
                // To "unmark" we save as 'absent' or we need a delete endpoint
                // For now, mark as absent to clear
                saveSingleRecord(personId, 'absent', cpId);
            }
        }, 300);
    };

    // ---- Save Single Record ----
    function saveSingleRecord(personId, status, checkpointId) {
        fetch('/api/attendance/record', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                person_id: personId,
                program_name: selectedProgram,
                checkpoint_id: parseInt(checkpointId),
                status: status,
                date: selectedDate
            })
        }).then(function(r) { return r.json(); }).then(function(data) {
            if (!data.success) {
                showToast('Save failed: ' + (data.error || 'Unknown'), 'error');
            }
        }).catch(function(err) {
            console.error('Save error:', err);
            showToast('Network error — could not save', 'error');
        });
    }

    // ---- Mark All Present ----
    window.markAllPresent = function() {
        if (isDayLocked) {
            showToast('Day is locked', 'error');
            return;
        }

        var unmarked = campers.filter(function(c) {
            var att = (c.attendance && c.attendance[DAILY_CP]) || {};
            return !att.status;
        });

        if (unmarked.length === 0) {
            showToast('All campers already marked!');
            return;
        }

        // Update local state + UI
        unmarked.forEach(function(c) {
            if (!c.attendance) c.attendance = {};
            c.attendance[DAILY_CP] = { status: 'present' };
        });
        renderCamperList();

        // Batch save
        var btn = document.getElementById('mark-all-btn');
        btn.disabled = true;
        btn.textContent = 'Saving...';

        fetch('/api/attendance/record-batch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                program_name: selectedProgram,
                checkpoint_id: parseInt(DAILY_CP),
                status: 'present',
                person_ids: unmarked.map(function(c) { return c.person_id; }),
                date: selectedDate
            })
        }).then(function(r) { return r.json(); }).then(function(data) {
            if (data.success) {
                showToast(data.count + ' campers marked present', 'success');
            } else {
                showToast('Batch save failed', 'error');
            }
        }).catch(function(err) {
            showToast('Network error', 'error');
        }).finally(function() {
            btn.disabled = false;
            btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg> Mark All Present';
        });
    };

    // ---- Unmark All ----
    window.unmarkAll = function() {
        if (isDayLocked) {
            showToast('Day is locked', 'error');
            return;
        }

        var marked = campers.filter(function(c) {
            var att = (c.attendance && c.attendance[DAILY_CP]) || {};
            return !!att.status;
        });

        if (marked.length === 0) {
            showToast('No campers are marked');
            return;
        }

        // Update local state: clear main status + EP for all
        marked.forEach(function(c) {
            if (!c.attendance) c.attendance = {};
            c.attendance[DAILY_CP] = {};
            c.attendance[EP_CP] = {};
        });
        renderCamperList();

        // Batch save as absent for main checkpoint
        var btn = document.getElementById('unmark-all-btn');
        btn.disabled = true;
        btn.textContent = 'Clearing...';

        var personIds = marked.map(function(c) { return c.person_id; });

        // Clear main status
        fetch('/api/attendance/record-batch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                program_name: selectedProgram,
                checkpoint_id: parseInt(DAILY_CP),
                status: 'absent',
                person_ids: personIds,
                date: selectedDate
            })
        }).then(function(r) { return r.json(); }).then(function(data) {
            // Also clear EP for all
            return fetch('/api/attendance/record-batch', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    program_name: selectedProgram,
                    checkpoint_id: parseInt(EP_CP),
                    status: 'absent',
                    person_ids: personIds,
                    date: selectedDate
                })
            });
        }).then(function(r) { return r.json(); }).then(function(data) {
            showToast(marked.length + ' campers unmarked', 'success');
        }).catch(function(err) {
            showToast('Network error', 'error');
        }).finally(function() {
            btn.disabled = false;
            btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg> Unmark All';
        });
    };

    // ---- Update Progress ----
    function updateProgress() {
        var total = campers.length;
        var marked = campers.filter(function(c) {
            var att = (c.attendance && c.attendance[DAILY_CP]) || {};
            return !!att.status;
        }).length;

        var pct = total > 0 ? Math.round(marked / total * 100) : 0;
        var fill = document.getElementById('progress-fill');
        fill.style.width = pct + '%';
        fill.classList.toggle('complete', pct === 100);

        document.getElementById('progress-text').textContent = marked + '/' + total + ' marked';

        var markBtn = document.getElementById('mark-all-btn');
        if (markBtn && !isDayLocked) markBtn.disabled = (marked >= total);

        var unmarkBtn = document.getElementById('unmark-all-btn');
        if (unmarkBtn && !isDayLocked) unmarkBtn.disabled = (marked === 0);
    }

    // ---- Toast ----
    function showToast(msg, type) {
        var toast = document.getElementById('att-toast');
        toast.textContent = msg;
        toast.className = 'att-toast show' + (type ? ' ' + type : '');
        setTimeout(function() {
            toast.className = 'att-toast';
        }, 2500);
    }
    window.showToast = showToast;

    // ---- Helpers ----
    function fetchJSON(url) {
        return fetch(url).then(function(r) {
            if (!r.ok) throw new Error('HTTP ' + r.status);
            return r.json();
        });
    }

    function escHTML(str) {
        var div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

})();
