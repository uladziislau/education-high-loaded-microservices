// Global state
let currentRate = 50;
let currentAutoscale = false;
let replicaCounts = {
    AuthService: 1,
    UserService: 1,
    NotificationService: 1
};
let serviceLatencies = {
    AuthService: -1,
    UserService: -1,
    NotificationService: -1
};
let serviceFailures = {
    AuthService: false,
    UserService: false,
    NotificationService: false
};
let serviceCacheMode = {
    UserService: 'None'
};

// Queue Backlog Trackers (simulated client-side telemetry)
let queueSimAuth = 0;
let queueAuthUser = 0;
let queueAuthNotify = 0;
let lastReplicaProcessed = {};
let lastMetricsTime = null;

// History of previous values to detect state transitions for console logger
let prevServiceFailures = {
    AuthService: false,
    UserService: false,
    NotificationService: false
};
let prevReplicaCounts = {
    AuthService: 0,
    UserService: 0,
    NotificationService: 0
};
let prevCacheMode = {
    UserService: 'None'
};

// RTT Latency Sparkline Graph History
let rttHistory = [];

// Helper functions for terminal trace logging
function addTerminalLog(tag, message, level = 'info') {
    const terminalBody = document.getElementById('terminal-body');
    if (!terminalBody) return;
    
    const row = document.createElement('div');
    row.className = 'terminal-row';
    
    const now = new Date();
    const timeStr = now.toTimeString().split(' ')[0];
    
    row.innerHTML = `
        <span class="terminal-time">${timeStr}</span>
        <span class="terminal-tag ${level}">${tag}</span>
        <span class="terminal-message ${level}">${message}</span>
    `;
    
    terminalBody.appendChild(row);
    
    while (terminalBody.children.length > 80) {
        terminalBody.removeChild(terminalBody.firstChild);
    }
    
    const autoscroll = document.getElementById('terminal-autoscroll');
    if (autoscroll && autoscroll.checked) {
        terminalBody.scrollTop = terminalBody.scrollHeight;
    }
}

window.clearTerminal = function() {
    const terminalBody = document.getElementById('terminal-body');
    if (terminalBody) {
        terminalBody.innerHTML = '';
    }
};

// Update Queue Badges on the SVG paths
function updateQueueBadge(badgeId, queueSize) {
    const badge = document.getElementById(badgeId);
    if (!badge) return;
    
    const rect = badge.querySelector('rect');
    const text = badge.querySelector('text');
    if (!rect || !text) return;
    
    const size = Math.round(queueSize);
    text.textContent = `Очередь: ${size}`;
    
    if (size === 0) {
        badge.setAttribute('class', 'queue-badge');
    } else if (size < 30) {
        badge.setAttribute('class', 'queue-badge warning');
    } else {
        badge.setAttribute('class', 'queue-badge critical');
    }
}

// Draw a canvas sparkline for RTT latency
function drawRttSparkline() {
    const canvas = document.getElementById('rtt-chart');
    if (!canvas) return;
    
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    if (rttHistory.length < 2) return;
    
    const maxVal = Math.max(...rttHistory, 50);
    const minVal = Math.min(...rttHistory, 0);
    const range = maxVal - minVal;
    
    const width = canvas.width;
    const height = canvas.height;
    
    ctx.beginPath();
    ctx.strokeStyle = '#60a5fa';
    ctx.lineWidth = 1.5;
    
    const gradient = ctx.createLinearGradient(0, 0, 0, height);
    gradient.addColorStop(0, 'rgba(96, 165, 250, 0.3)');
    gradient.addColorStop(1, 'rgba(96, 165, 250, 0)');
    
    const points = [];
    for (let i = 0; i < rttHistory.length; i++) {
        const x = (i / (rttHistory.length - 1)) * width;
        const y = height - ((rttHistory[i] - minVal) / (range || 1)) * (height - 6) - 3;
        points.push({ x, y });
    }
    
    ctx.moveTo(points[0].x, points[0].y);
    for (let i = 1; i < points.length; i++) {
        ctx.lineTo(points[i].x, points[i].y);
    }
    ctx.stroke();
    
    ctx.lineTo(points[points.length - 1].x, height);
    ctx.lineTo(points[0].x, height);
    ctx.closePath();
    ctx.fillStyle = gradient;
    ctx.fill();
    
    const lastPoint = points[points.length - 1];
    ctx.beginPath();
    ctx.arc(lastPoint.x, lastPoint.y, 2.5, 0, 2 * Math.PI);
    ctx.fillStyle = '#ef4444';
    ctx.fill();
}

// SVG Path elements for animations
const pathSimAuth = document.getElementById('link-sim-auth');
const pathAuthUser = document.getElementById('link-auth-user');
const pathAuthNotify = document.getElementById('link-auth-notify');
const pathUserSim = document.getElementById('link-user-sim');

// Particle Pool configuration for extreme performance
const maxParticles = 80;
const particlePool = [];
const activeParticles = [];
let spawnIntervalId = null;

// Initialize Web UI Controls
document.addEventListener('DOMContentLoaded', () => {
    // Populate particle pool
    const particlesContainer = document.getElementById('particles-container');
    if (particlesContainer) {
        for (let i = 0; i < maxParticles; i++) {
            const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            circle.setAttribute('r', '3');
            circle.style.display = 'none';
            particlesContainer.appendChild(circle);
            
            particlePool.push({
                element: circle,
                active: false,
                path: null,
                progress: 0,
                pathLength: 0,
                cssClass: ''
            });
        }
    }

    setupEventListeners();
    startEventStream();
    startParticleSpawner();
    
    // Start global game-loop for rendering particles
    requestAnimationFrame(globalAnimationLoop);
});

function setupEventListeners() {
    const loadSlider = document.getElementById('load-slider');
    const loadValue = document.getElementById('load-value');
    const autoscaleToggle = document.getElementById('autoscale-toggle');

    // Handle Load Slider
    if (loadSlider) {
        loadSlider.addEventListener('change', async (e) => {
            const rate = parseInt(e.target.value);
            loadValue.textContent = `${rate} RPS`;
            try {
                const res = await fetch('/api/load', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ rate })
                });
                if (res.ok) {
                    currentRate = rate;
                }
            } catch (err) {
                console.error('Failed to change simulator load rate:', err);
            }
        });

        loadSlider.addEventListener('input', (e) => {
            loadValue.textContent = `${e.target.value} RPS`;
        });
    }

    // Handle Autoscale Toggle
    if (autoscaleToggle) {
        autoscaleToggle.addEventListener('change', async (e) => {
            const enabled = e.target.checked;
            try {
                const res = await fetch('/api/autoscale', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ enabled })
                });
                if (res.ok) {
                    currentAutoscale = enabled;
                    updateAutoscaleUIState(enabled);
                }
            } catch (err) {
                console.error('Failed to toggle autoscaler:', err);
            }
        });
    }

    // Handle Latency Sliders
    const latencySliders = document.querySelectorAll('.latency-slider');
    latencySliders.forEach(slider => {
        const service = slider.getAttribute('data-service');
        const lbl = document.getElementById(`latency-lbl-${service}`);

        slider.addEventListener('input', (e) => {
            const val = parseInt(e.target.value);
            lbl.textContent = val === -1 ? 'Авто' : `${val} мс`;
        });

        slider.addEventListener('change', async (e) => {
            const val = parseInt(e.target.value);
            lbl.textContent = val === -1 ? 'Авто' : `${val} мс`;
            try {
                await fetch('/api/service/latency', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ serviceName: service, latencyMs: val })
                });
            } catch (err) {
                console.error(`Failed to update latency for ${service}:`, err);
            }
        });
    });
}

function updateAutoscaleUIState(enabled) {
    if (enabled) {
        document.body.classList.add('autoscale-active');
    } else {
        document.body.classList.remove('autoscale-active');
    }
}

// Global scope scaling button handler
async function adjustScale(serviceName, change) {
    if (currentAutoscale) return; // Scale buttons disabled during autoscaling

    const currentCount = replicaCounts[serviceName] || 1;
    const targetScale = currentCount + change;

    if (targetScale < 1 || targetScale > 5) return; // Keep scale between 1 and 5

    try {
        const res = await fetch('/api/scale', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ serviceName, replicas: targetScale })
        });
        if (res.ok) {
            replicaCounts[serviceName] = targetScale;
            document.getElementById(`replica-${serviceName}`).textContent = targetScale;
        }
    } catch (err) {
        console.error(`Failed to manually scale ${serviceName}:`, err);
    }
}

// Interactive failure trigger (Chaos Engineering)
async function toggleFailure(serviceName) {
    if (currentAutoscale) return; // Disabled during autoscaling

    const targetState = !serviceFailures[serviceName];
    try {
        const res = await fetch('/api/service/failure', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ serviceName, isFailed: targetState })
        });
        if (res.ok) {
            serviceFailures[serviceName] = targetState;
            const btn = document.getElementById(`btn-chaos-${serviceName}`);
            if (btn) {
                if (targetState) {
                    btn.classList.add('active');
                    btn.innerHTML = '<i class="fa-solid fa-heart-pulse"></i> Восстановить';
                } else {
                    btn.classList.remove('active');
                    btn.innerHTML = '<i class="fa-solid fa-skull"></i> Вызвать сбой';
                }
            }
        }
    } catch (err) {
        console.error(`Failed to toggle failure state for ${serviceName}:`, err);
    }
}

// Change Cache Mode for a service
async function changeCacheMode(serviceName, mode) {
    if (currentAutoscale) return;

    try {
        const res = await fetch('/api/service/cache', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ serviceName, cacheMode: mode })
        });
        if (res.ok) {
            serviceCacheMode[serviceName] = mode;
        }
    } catch (err) {
        console.error(`Failed to change cache mode for ${serviceName}:`, err);
    }
}

// Server-Sent Events (SSE) Stream
function startEventStream() {
    const eventSource = new EventSource('/api/metrics/stream');

    eventSource.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            updateDashboard(data);
        } catch (err) {
            console.error('Error parsing metrics SSE event:', err);
        }
    };

    eventSource.onerror = (err) => {
        console.error('Metrics stream lost. Reconnecting in 3s...', err);
        eventSource.close();
        setTimeout(startEventStream, 3000);
    };
}

// Update dashboard elements with incoming data
function updateDashboard(data) {
    // A. Calculate queues and logs time diff
    const now = Date.now();
    const dt = lastMetricsTime ? (now - lastMetricsTime) / 1000 : 1.0;
    lastMetricsTime = now;

    // B. Calculate actual throughput (RPS) of each service based on cumulative processed metrics diff
    let authProcessedThisSecond = 0;
    const authReplicas = data.Replicas.filter(r => r.ServiceName === 'AuthService');
    authReplicas.forEach(rep => {
        const prev = lastReplicaProcessed[rep.InstanceId] !== undefined ? lastReplicaProcessed[rep.InstanceId] : rep.Processed;
        const diff = Math.max(0, rep.Processed - prev);
        authProcessedThisSecond += diff;
        lastReplicaProcessed[rep.InstanceId] = rep.Processed;
    });
    let authProcessedRps = dt > 0 ? authProcessedThisSecond / dt : authProcessedThisSecond;

    let userProcessedThisSecond = 0;
    const userReplicas = data.Replicas.filter(r => r.ServiceName === 'UserService');
    userReplicas.forEach(rep => {
        const prev = lastReplicaProcessed[rep.InstanceId] !== undefined ? lastReplicaProcessed[rep.InstanceId] : rep.Processed;
        const diff = Math.max(0, rep.Processed - prev);
        userProcessedThisSecond += diff;
        lastReplicaProcessed[rep.InstanceId] = rep.Processed;
    });
    let userProcessedRps = dt > 0 ? userProcessedThisSecond / dt : userProcessedThisSecond;

    let notifyProcessedThisSecond = 0;
    const notifyReplicas = data.Replicas.filter(r => r.ServiceName === 'NotificationService');
    notifyReplicas.forEach(rep => {
        const prev = lastReplicaProcessed[rep.InstanceId] !== undefined ? lastReplicaProcessed[rep.InstanceId] : rep.Processed;
        const diff = Math.max(0, rep.Processed - prev);
        notifyProcessedThisSecond += diff;
        lastReplicaProcessed[rep.InstanceId] = rep.Processed;
    });
    let notifyProcessedRps = dt > 0 ? notifyProcessedThisSecond / dt : notifyProcessedThisSecond;

    // C. Update Queue size approximations (RPS in vs processed out)
    const targetRate = data.SimulatorRate;
    
    // Auth queue grows when generator rate is higher than auth throughput
    if (serviceFailures['AuthService']) {
        queueSimAuth = Math.max(0, queueSimAuth + targetRate * dt);
    } else {
        const authQueueDelta = targetRate - authProcessedRps;
        queueSimAuth = Math.max(0, queueSimAuth + authQueueDelta * dt);
    }

    // User/Notify queues grow when auth throughput is higher than user/notify throughput
    if (serviceFailures['UserService']) {
        queueAuthUser = Math.max(0, queueAuthUser + authProcessedRps * dt);
    } else {
        const userQueueDelta = authProcessedRps - userProcessedRps;
        queueAuthUser = Math.max(0, queueAuthUser + userQueueDelta * dt);
    }

    if (serviceFailures['NotificationService']) {
        queueAuthNotify = Math.max(0, queueAuthNotify + authProcessedRps * dt);
    } else {
        const notifyQueueDelta = authProcessedRps - notifyProcessedRps;
        queueAuthNotify = Math.max(0, queueAuthNotify + notifyQueueDelta * dt);
    }

    // Update queue badge elements visually
    updateQueueBadge('badge-sim-auth', queueSimAuth);
    updateQueueBadge('badge-auth-user', queueAuthUser);
    updateQueueBadge('badge-auth-notify', queueAuthNotify);

    // D. Log cache mode changes
    const currentMode = (data.ServiceCacheMode && data.ServiceCacheMode['UserService']) || 'None';
    if (currentMode !== prevCacheMode['UserService']) {
        let russianMode = 'Отключен';
        if (currentMode === 'Local') russianMode = 'Локальный кэш (.NET MemoryCache)';
        if (currentMode === 'Redis') russianMode = 'Распределенный кэш (Redis)';
        addTerminalLog('UserService', `Режим кэширования изменен: ${russianMode}`, 'info');
        prevCacheMode['UserService'] = currentMode;
    }

    // E. Periodic request flow log generation
    if (targetRate > 0 && Math.random() < 0.35) {
        if (serviceFailures['AuthService']) {
            addTerminalLog('AuthService', `ОШИБКА 500: Запрос отклонён. Сервер авторизации недоступен`, 'error');
        } else {
            if (Math.random() < 0.7) {
                addTerminalLog('AuthService', `JWT токен сгенерирован (задержка: ~${Math.round(20 + Math.random()*30)}мс)`, 'success');
                
                if (serviceFailures['UserService']) {
                    addTerminalLog('UserService', `ОШИБКА 500: Очередь RabbitMQ переполнена. Сервис пользователей недоступен`, 'error');
                } else if (userReplicas.length > 0) {
                    if (currentMode === 'None') {
                        addTerminalLog('UserService', `Чтение профиля. Кэш отключен -> Запрос в Slow DB (300мс) (Cache Miss)`, 'warn');
                    } else if (currentMode === 'Local') {
                        if (Math.random() < 0.85) {
                            addTerminalLog('UserService', `Чтение профиля. Cache Hit (Local MemoryCache) -> Задержка: 0мс`, 'success');
                        } else {
                            addTerminalLog('UserService', `Чтение профиля. Cache Miss -> Чтение из DB (300мс) -> Запись в MemoryCache`, 'info');
                        }
                    } else if (currentMode === 'Redis') {
                        if (Math.random() < 0.8) {
                            addTerminalLog('UserService', `Чтение профиля. Cache Hit (Redis Cache) -> Задержка: 5мс`, 'success');
                        } else {
                            addTerminalLog('UserService', `Чтение профиля. Cache Miss -> Чтение из DB (300мс) -> Запись в Redis`, 'info');
                        }
                    }
                }
                
                if (serviceFailures['NotificationService']) {
                    addTerminalLog('NotificationService', `ОШИБКА 500: Сбой отправки пуш-уведомления`, 'error');
                } else if (notifyReplicas.length > 0) {
                    if (Math.random() < 0.5) {
                        addTerminalLog('NotificationService', `Push-уведомление отправлено через WebSocket`, 'success');
                    }
                }
            }
        }
    }

    // 1. Sync autoscale configuration
    const autoscaleToggle = document.getElementById('autoscale-toggle');
    if (autoscaleToggle && autoscaleToggle.checked !== data.AutoScaling) {
        autoscaleToggle.checked = data.AutoScaling;
        currentAutoscale = data.AutoScaling;
        updateAutoscaleUIState(data.AutoScaling);
    }

    // 2. Sync load generator rate slider
    const loadSlider = document.getElementById('load-slider');
    const loadValue = document.getElementById('load-value');
    if (loadSlider && parseInt(loadSlider.value) !== data.SimulatorRate) {
        loadSlider.value = data.SimulatorRate;
        loadValue.textContent = `${data.SimulatorRate} RPS`;
        currentRate = data.SimulatorRate;
    }

    // 3. Sync service latency sliders and failure buttons (if not currently focused)
    const services = ['AuthService', 'UserService', 'NotificationService'];
    services.forEach(service => {
        if (data.ServiceLatencies && data.ServiceLatencies[service] !== undefined) {
            const serverLat = data.ServiceLatencies[service];
            serviceLatencies[service] = serverLat;
            
            const slider = document.querySelector(`.latency-slider[data-service="${service}"]`);
            if (slider && document.activeElement !== slider) {
                slider.value = serverLat;
                document.getElementById(`latency-lbl-${service}`).textContent = serverLat === -1 ? 'Авто' : `${serverLat} мс`;
            }
        }

        if (data.ServiceFailures && data.ServiceFailures[service] !== undefined) {
            const serverFail = data.ServiceFailures[service];
            
            if (serverFail !== prevServiceFailures[service]) {
                if (serverFail) {
                    addTerminalLog(service, `КРИТИЧЕСКИЙ СБОЙ: Симуляция хаоса активирована. Запросы сбрасываются`, 'error');
                } else {
                    addTerminalLog(service, `Восстановление работы: Реплики снова принимают запросы`, 'success');
                }
                prevServiceFailures[service] = serverFail;
            }

            serviceFailures[service] = serverFail;

            const btn = document.getElementById(`btn-chaos-${service}`);
            if (btn) {
                if (serverFail) {
                    btn.classList.add('active');
                    btn.innerHTML = '<i class="fa-solid fa-heart-pulse"></i> Восстановить';
                } else {
                    btn.classList.remove('active');
                    btn.innerHTML = '<i class="fa-solid fa-skull"></i> Вызвать сбой';
                }
            }

            // Sync SVG boundary box styling
            const groupRect = document.querySelector(`#group-${service} rect.group-boundary`);
            if (groupRect) {
                if (serverFail) {
                    groupRect.classList.add('failed');
                } else {
                    groupRect.classList.remove('failed');
                }
            }
        }
    });

    // 4. Update top metric cards
    if (data.Simulator) {
        document.getElementById('total-completed').textContent = data.Simulator.Processed.toLocaleString();
        document.getElementById('total-inflight').textContent = data.Simulator.InFlight.toLocaleString();
        document.getElementById('rtt-latency').textContent = `${data.Simulator.RttMs.toFixed(1)} мс`;
        
        rttHistory.push(data.Simulator.RttMs);
        if (rttHistory.length > 40) rttHistory.shift();
        drawRttSparkline();

        // Update Simulator node sub-text
        document.getElementById('metrics-Simulator').textContent = `${currentRate} RPS | Очередь: ${data.Simulator.InFlight}`;
        
        // Dynamic status representation for broker backlog
        const inflightStatus = document.getElementById('inflight-status');
        if (inflightStatus) {
            if (data.Simulator.InFlight > 40) {
                inflightStatus.className = 'card-trend text-warning';
                inflightStatus.innerHTML = '<i class="fa-solid fa-triangle-exclamation"></i> Высокая нагрузка брокера (затор)';
            } else {
                inflightStatus.className = 'card-trend';
                inflightStatus.innerHTML = '<i class="fa-solid fa-network-wired"></i> Нагрузка брокера RabbitMQ';
            }
        }
    }

    // 5. Update SVG paths classes to reflect queue backup visually
    updatePathStatus(pathSimAuth, 'AuthService', data.Replicas);
    updatePathStatus(pathAuthUser, 'UserService', data.Replicas);
    updatePathStatus(pathAuthNotify, 'NotificationService', data.Replicas);

    // Sync Cache UI select dropdown
    const cacheSelect = document.getElementById('cache-mode-UserService');
    if (cacheSelect && data.ServiceCacheMode && data.ServiceCacheMode['UserService'] !== undefined) {
        if (document.activeElement !== cacheSelect) {
            cacheSelect.value = data.ServiceCacheMode['UserService'];
            serviceCacheMode['UserService'] = data.ServiceCacheMode['UserService'];
        }
    }

    // 6. Sync replica controls and draw sub-nodes
    services.forEach(service => {
        const replicas = data.Replicas.filter(r => r.ServiceName === service);
        
        // Log scaling transitions
        if (replicas.length !== prevReplicaCounts[service]) {
            if (prevReplicaCounts[service] > 0 || replicas.length > 0) {
                if (replicas.length > prevReplicaCounts[service]) {
                    addTerminalLog(service, `Масштабирование: Добавлена новая реплика. Всего инстансов: ${replicas.length}`, 'info');
                } else if (replicas.length < prevReplicaCounts[service]) {
                    addTerminalLog(service, `Масштабирование: Выведена из эксплуатации лишняя реплика. Всего инстансов: ${replicas.length}`, 'warn');
                }
            }
            prevReplicaCounts[service] = replicas.length;
        }

        replicaCounts[service] = replicas.length;
        
        document.getElementById(`replica-${service}`).textContent = replicas.length;
        renderSvgReplicas(service, replicas);
    });

    // 7. Update active replica details table in Russian
    const tableBody = document.getElementById('replica-table-body');
    if (!tableBody) return;
    
    if (data.Replicas.length === 0) {
        tableBody.innerHTML = `<tr><td colspan="8" class="empty-row">Активные реплики отсутствуют. Ожидание запуска...</td></tr>`;
        return;
    }

    let rowsHtml = '';
    data.Replicas.forEach(rep => {
        const lastSeenDate = new Date(rep.LastSeen);
        const diffSecs = (new Date() - lastSeenDate) / 1000;
        const isActive = diffSecs < 3.0;
        
        const isFailed = serviceFailures[rep.ServiceName];
        const statusClass = isFailed ? 'idle' : (isActive ? 'active' : 'idle');
        const statusText = isFailed ? 'сбой' : (isActive ? 'активен' : 'ожидание');
        
        rowsHtml += `
            <tr>
                <td style="color: #fff; font-weight: 500;">${rep.InstanceId}</td>
                <td><span class="service-dot ${getServiceClass(rep.ServiceName)}"></span> ${rep.ServiceName}</td>
                <td>${rep.Port}</td>
                <td style="color: #a7f3d0;">${rep.Processed.toLocaleString()}</td>
                <td style="color: #fca5a5;">${rep.Failed.toLocaleString()}</td>
                <td>${isFailed ? '—' : `${rep.LatencyMs.toFixed(1)} мс`}</td>
                <td>${rep.MemoryMb.toFixed(1)} МБ</td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
            </tr>
        `;
    });
    tableBody.innerHTML = rowsHtml;
}

function updatePathStatus(pathElement, serviceName, replicas) {
    if (!pathElement) return;
    
    const isFailed = serviceFailures[serviceName];
    const serviceReps = replicas.filter(r => r.ServiceName === serviceName);
    const hasHighLatency = serviceReps.some(r => r.LatencyMs > 250);

    if (isFailed) {
        pathElement.className.baseVal = "queue-link critical";
    } else if (hasHighLatency) {
        pathElement.className.baseVal = "queue-link backup";
    } else {
        pathElement.className.baseVal = "queue-link";
    }
}

function getServiceClass(name) {
    if (name === 'AuthService') return 'auth';
    if (name === 'UserService') return 'user';
    return 'notify';
}

// Render dynamic replica circle nodes in SVG boundary boxes (Centered & Side-aligned labels)
function renderSvgReplicas(service, replicas) {
    const groupElement = document.getElementById(`replicas-${service}`);
    if (!groupElement) return;

    let cx = 0;
    let textX = 0;
    let yCenter = 0;
    let spacing = 35;

    if (service === 'AuthService') {
        cx = 280;
        yCenter = 200;
        spacing = 40;
    } else if (service === 'UserService') {
        cx = 540;
        yCenter = 145;
    } else if (service === 'NotificationService') {
        cx = 540;
        yCenter = 335;
    }
    textX = cx + 18;

    const N = replicas.length;
    const startY = yCenter - ((N - 1) * spacing) / 2;

    let nodesHtml = '';
    replicas.forEach((rep, index) => {
        const cy = startY + (index * spacing);
        const colorClass = getServiceClass(service);
        const repFailed = serviceFailures[service];

        // Find status
        const lastSeenDate = new Date(rep.LastSeen);
        const isActive = (new Date() - lastSeenDate) / 1000 < 3.0;
        let borderGlow = isActive ? `var(--color-${colorClass})` : '#64748b';
        if (repFailed) borderGlow = 'var(--color-danger)';

        nodesHtml += `
            <g class="replica-node ${colorClass} ${repFailed ? 'failed' : ''}">
                <circle cx="${cx}" cy="${cy}" r="11" class="rep-outer" style="stroke: ${borderGlow}; fill: #0f172a; stroke-width: 1.5;" />
                <circle cx="${cx}" cy="${cy}" r="8" class="rep-inner" style="fill: #1e293b;" />
                <text x="${textX}" y="${cy - 1}" class="rep-port" style="fill: #f8fafc; font-size: 9px; font-weight: bold; text-anchor: start;">
                    :${rep.Port % 1000}
                </text>
                <text x="${textX}" y="${cy + 9}" class="rep-activity" style="fill: #94a3b8; font-size: 8px; text-anchor: start;">
                    ${repFailed ? 'СБОЙ' : `${rep.LatencyMs.toFixed(0)}мс`}
                </text>
            </g>
        `;
    });

    groupElement.innerHTML = nodesHtml;
}

// Particle Game-Loop Renderer (Extreme performance, no DOM thrashing)
function globalAnimationLoop(timestamp) {
    if (!lastTime) lastTime = timestamp;
    const delta = timestamp - lastTime;
    lastTime = timestamp;

    for (let i = 0; i < activeParticles.length; i++) {
        const p = activeParticles[i];
        p.progress += delta / 1800; // Duration 1.8 seconds

        if (p.progress >= 1.0) {
            // Deactivate and recycle particle
            p.element.style.display = 'none';
            p.active = false;
            activeParticles.splice(i, 1);
            i--;
        } else {
            try {
                const length = p.progress * p.pathLength;
                const point = p.path.getPointAtLength(length);
                p.element.setAttribute('cx', point.x);
                p.element.setAttribute('cy', point.y);
            } catch (e) {
                // Ignore transient SVG length errors
            }
        }
    }
    requestAnimationFrame(globalAnimationLoop);
}

let lastTime = 0;

function triggerParticle(pathElement, cssClass) {
    if (!pathElement) return;

    // Find first available inactive particle in the pool
    const p = particlePool.find(item => !item.active);
    if (!p) return; // Pool exhausted, skip spawning to maintain 60 FPS

    p.active = true;
    p.progress = 0;
    p.path = pathElement;
    p.pathLength = pathElement.getTotalLength();
    
    p.element.setAttribute('class', cssClass);
    p.element.style.display = 'block';
    
    activeParticles.push(p);
}

// Spawner trigger function that runs on interval and scales with currentRate
function startParticleSpawner() {
    if (spawnIntervalId) {
        clearInterval(spawnIntervalId);
    }
    
    spawnIntervalId = setInterval(() => {
        if (currentRate <= 0) return;
        
        // Spawn probability based on current traffic rate
        const prob = Math.min(1.0, currentRate / 100);
        if (Math.random() < prob) {
            spawnFlowParticles();
        }
    }, 60);
}

function spawnFlowParticles() {
    // 1. Sim -> Auth
    if (replicaCounts.AuthService > 0) {
        triggerParticle(pathSimAuth, 'flow-particle');
    }
    // 2. Auth -> User & Auth -> Notify
    if (replicaCounts.AuthService > 0) {
        if (replicaCounts.UserService > 0) {
            triggerParticle(pathAuthUser, 'flow-particle auth');
        }
        if (replicaCounts.NotificationService > 0) {
            triggerParticle(pathAuthNotify, 'flow-particle auth');
        }
    }
    // 3. User -> Sim
    if (replicaCounts.UserService > 0) {
        triggerParticle(pathUserSim, 'flow-particle notify');
        
        // 4. Infrastructure paths from User Service
        const mode = serviceCacheMode['UserService'];
        const pathUserCache = document.getElementById('link-user-cache');
        const pathUserDb = document.getElementById('link-user-db');
        
        if (mode === 'Redis') {
            if (Math.random() < 0.8) triggerParticle(pathUserCache, 'flow-particle cache');
            else triggerParticle(pathUserDb, 'flow-particle db');
        } else if (mode === 'Local') {
            if (Math.random() < 0.8) {
                triggerLocalCacheFlash();
            } else {
                triggerParticle(pathUserDb, 'flow-particle db');
            }
        } else {
            triggerParticle(pathUserDb, 'flow-particle db');
        }
    }
}

function triggerLocalCacheFlash() {
    const group = document.querySelector('#group-UserService rect');
    if (group) {
        group.classList.remove('cache-flash-active');
        void group.offsetWidth; // trigger reflow
        group.classList.add('cache-flash-active');
    }
}
