#!/usr/bin/env python3
"""
QbitFetch Dashboard — Lightweight web UI for viewing download history
and controlling active downloads. Uses Python stdlib only.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from functools import partial
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

log = logging.getLogger("qbit-fetch")

# ---------------------------------------------------------------------------
# Download history — thread-safe JSON storage
# ---------------------------------------------------------------------------
MAX_HISTORY = 100


class DownloadHistory:
    """Thread-safe rolling history of completed downloads, persisted to JSON."""

    def __init__(self, history_file: Path):
        self._file = history_file
        self._lock = threading.Lock()
        self._entries: list[dict] = []
        self._load()

    def _load(self) -> None:
        """Load existing history from disk."""
        try:
            if self._file.exists():
                data = json.loads(self._file.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    self._entries = data[-MAX_HISTORY:]
        except Exception as e:
            log.warning("Could not load history from '%s': %s", self._file, e)
            self._entries = []

    def _save(self) -> None:
        """Write current history to disk."""
        try:
            self._file.write_text(
                json.dumps(self._entries, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as e:
            log.warning("Could not save history to '%s': %s", self._file, e)

    def add_entry(
        self,
        torrent_name: str,
        size_bytes: int,
        speed_bytes_per_sec: float,
        duration_sec: float,
        status: str = "completed",
        error: str = "",
    ) -> None:
        """Record a download (completed or failed)."""
        entry = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "epoch": time.time(),
            "name": torrent_name,
            "status": status,
            "size_bytes": size_bytes,
            "size_human": _format_size(size_bytes) if size_bytes > 0 else "\u2014",
            "speed_human": f"{_format_size(speed_bytes_per_sec)}/s" if speed_bytes_per_sec > 0 else "\u2014",
            "duration_sec": round(duration_sec, 1),
            "duration_human": _format_duration(duration_sec) if duration_sec > 0 else "\u2014",
            "error": error,
        }
        with self._lock:
            self._entries.append(entry)
            self._entries = self._entries[-MAX_HISTORY:]
            self._save()

    def get_entries(self, limit: int = MAX_HISTORY) -> list[dict]:
        """Return history entries (newest first), up to `limit` items."""
        with self._lock:
            return list(reversed(self._entries))[:limit]


# ---------------------------------------------------------------------------
# Download state — shared between main loop, progress monitor, and web server
# ---------------------------------------------------------------------------
class DownloadState:
    """Thread-safe state for the currently active download."""

    def __init__(self):
        self._lock = threading.Lock()
        self._active = False
        self._name = ""
        self._remote_size = 0
        self._current_bytes = 0
        self._speed = 0.0
        self._elapsed = 0.0
        self._paused = False
        self._pid = None
        # Signals from dashboard -> main loop
        self._pause_requested = False
        self._resume_requested = False

    def start(self, name: str, remote_size: int) -> None:
        with self._lock:
            self._active = True
            self._name = name
            self._remote_size = remote_size
            self._current_bytes = 0
            self._speed = 0.0
            self._elapsed = 0.0
            self._paused = False
            self._pid = None
            self._pause_requested = False
            self._resume_requested = False

    def update(self, current_bytes: int, speed: float, elapsed: float) -> None:
        with self._lock:
            self._current_bytes = current_bytes
            self._speed = speed
            self._elapsed = elapsed

    def set_pid(self, pid: int) -> None:
        with self._lock:
            self._pid = pid

    def set_paused(self, paused: bool) -> None:
        with self._lock:
            self._paused = paused
            self._pause_requested = False
            self._resume_requested = False

    def finish(self) -> None:
        with self._lock:
            self._active = False
            self._paused = False
            self._pid = None
            self._pause_requested = False
            self._resume_requested = False

    def request_pause(self) -> bool:
        with self._lock:
            if self._active and not self._paused:
                self._pause_requested = True
                return True
            return False

    def request_resume(self) -> bool:
        with self._lock:
            if self._active and self._paused:
                self._resume_requested = True
                return True
            return False

    def should_pause(self) -> bool:
        with self._lock:
            if self._pause_requested:
                return True
            return False

    def should_resume(self) -> bool:
        with self._lock:
            if self._resume_requested:
                return True
            return False

    def clear_pause_request(self) -> None:
        with self._lock:
            self._pause_requested = False

    def clear_resume_request(self) -> None:
        with self._lock:
            self._resume_requested = False

    def to_dict(self) -> dict:
        with self._lock:
            if not self._active:
                return {"active": False}
            pct = 0.0
            if self._remote_size > 0:
                pct = min((self._current_bytes / self._remote_size) * 100, 100.0)
            # ETA calculation
            eta_sec = 0.0
            eta_human = "\u2014"
            if self._speed > 0 and self._remote_size > 0:
                remaining = self._remote_size - self._current_bytes
                if remaining > 0:
                    eta_sec = remaining / self._speed
                    eta_human = _format_duration(eta_sec)
                else:
                    eta_human = "0s"

            return {
                "active": True,
                "name": self._name,
                "remote_size": self._remote_size,
                "remote_size_human": _format_size(self._remote_size) if self._remote_size > 0 else "\u2014",
                "current_bytes": self._current_bytes,
                "current_bytes_human": _format_size(self._current_bytes),
                "speed": self._speed,
                "speed_human": f"{_format_size(self._speed)}/s" if self._speed > 0 else "\u2014",
                "elapsed": round(self._elapsed, 1),
                "elapsed_human": _format_duration(self._elapsed),
                "percentage": round(pct, 1),
                "paused": self._paused,
                "eta_sec": round(eta_sec, 1),
                "eta_human": eta_human,
            }


def _format_size(num_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} PB"


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = int(minutes // 60)
    mins = minutes % 60
    return f"{hours}h {mins}m"


# ---------------------------------------------------------------------------
# Icon — loaded from icon.png at startup, served via /icon.png endpoint
# ---------------------------------------------------------------------------
_ICON_PNG: bytes = b""  # populated by _load_icon()


def _load_icon() -> None:
    """Try to load icon.png from the script's directory."""
    global _ICON_PNG
    icon_path = Path(__file__).parent / "icon.png"
    try:
        if icon_path.exists():
            _ICON_PNG = icon_path.read_bytes()
            log.info("Loaded icon: %s (%d bytes)", icon_path, len(_ICON_PNG))
    except Exception as e:
        log.warning("Could not load icon: %s", e)


_load_icon()


# ---------------------------------------------------------------------------
# Web server — serves dashboard HTML and JSON API
# ---------------------------------------------------------------------------
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>QbitFetch Dashboard</title>
<style>
  :root {
    --bg-primary: #0f1117;
    --bg-secondary: #161822;
    --bg-elevated: #1c1f2e;
    --bg-hover: #232738;
    --border: #2a2d3e;
    --text-primary: #e4e6f0;
    --text-secondary: #8b8fa3;
    --text-muted: #5c6072;
    --accent: #6c8aff;
    --accent-glow: rgba(108, 138, 255, 0.12);
    --green: #34d399;
    --green-bg: rgba(52, 211, 153, 0.08);
    --red: #f87171;
    --red-bg: rgba(248, 113, 113, 0.08);
    --cyan: #67e8f9;
    --amber: #fbbf24;
    --violet: #a78bfa;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg-primary);
    color: var(--text-primary);
    padding: 28px 32px;
    min-height: 100vh;
  }
  .header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 24px;
    padding-bottom: 20px;
    border-bottom: 1px solid var(--border);
  }
  .header h1 {
    font-size: 1.4rem;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: -0.3px;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .header h1 span { color: var(--accent); }
  .header h1 .logo {
    width: 32px;
    height: 32px;
    border-radius: 6px;
  }
  .header .status {
    font-size: 0.8rem;
    color: var(--text-secondary);
  }
  .header .status .dot {
    display: inline-block;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--green);
    margin-right: 6px;
    box-shadow: 0 0 6px var(--green);
    animation: pulse 2.5s ease-in-out infinite;
  }
  @keyframes pulse {
    0%, 100% { opacity: 1; box-shadow: 0 0 6px var(--green); }
    50% { opacity: 0.5; box-shadow: 0 0 2px var(--green); }
  }

  /* Active download card */
  .active-dl {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 20px 24px;
    margin-bottom: 24px;
  }
  .active-dl.hidden { display: none; }
  .active-dl-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 14px;
  }
  .active-dl-header .label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    font-weight: 600;
    color: var(--text-secondary);
  }
  .active-dl-header .label .indicator {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--amber);
    margin-right: 8px;
    box-shadow: 0 0 8px var(--amber);
    animation: pulse-amber 1.5s ease-in-out infinite;
  }
  .active-dl-header .label .indicator.paused {
    background: var(--text-muted);
    box-shadow: none;
    animation: none;
  }
  @keyframes pulse-amber {
    0%, 100% { opacity: 1; box-shadow: 0 0 8px var(--amber); }
    50% { opacity: 0.5; box-shadow: 0 0 3px var(--amber); }
  }
  .pause-btn {
    background: var(--bg-elevated);
    color: var(--text-secondary);
    border: 1px solid var(--border);
    padding: 6px 18px;
    border-radius: 8px;
    font-size: 0.78rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .pause-btn:hover { border-color: var(--accent); color: var(--text-primary); background: var(--accent-glow); }
  .pause-btn:disabled { opacity: 0.4; cursor: not-allowed; }
  .active-dl-name {
    font-size: 0.95rem;
    font-weight: 500;
    color: var(--text-primary);
    margin-bottom: 12px;
    word-break: break-word;
  }
  .progress-wrap {
    background: var(--bg-primary);
    border-radius: 6px;
    height: 8px;
    overflow: hidden;
    margin-bottom: 12px;
  }
  .progress-bar {
    height: 100%;
    background: linear-gradient(90deg, var(--accent), var(--cyan));
    border-radius: 6px;
    transition: width 0.8s ease;
    min-width: 0;
  }
  .progress-bar.paused {
    background: var(--text-muted);
  }
  .active-dl-stats {
    display: flex;
    gap: 28px;
    font-size: 0.82rem;
  }
  .active-dl-stats .stat-label {
    color: var(--text-muted);
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 2px;
  }
  .active-dl-stats .stat-value { font-weight: 500; }
  .active-dl-stats .stat-value.speed { color: var(--green); }
  .active-dl-stats .stat-value.size { color: var(--cyan); }
  .active-dl-stats .stat-value.time { color: var(--violet); }

  /* Table */
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.87rem;
  }
  thead th {
    text-align: left;
    padding: 11px 14px;
    background: var(--bg-secondary);
    color: var(--text-secondary);
    font-weight: 600;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    position: sticky;
    top: 0;
    border-bottom: 1px solid var(--border);
  }
  tbody tr {
    border-bottom: 1px solid var(--border);
    transition: background 0.15s ease;
  }
  tbody tr:hover { background: var(--bg-hover); }
  tbody td {
    padding: 12px 14px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  td.name {
    white-space: normal;
    word-break: break-word;
    max-width: 420px;
    color: var(--text-primary);
    font-weight: 450;
  }
  td.speed { color: var(--green); font-weight: 500; }
  td.size { color: var(--cyan); }
  td.duration { color: var(--violet); }
  td.time { color: var(--text-muted); font-size: 0.82rem; }
  .badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 10px;
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.4px;
  }
  .badge-completed { background: var(--green-bg); color: var(--green); border: 1px solid rgba(52, 211, 153, 0.2); }
  .badge-failed { background: var(--red-bg); color: var(--red); border: 1px solid rgba(248, 113, 113, 0.2); }
  .badge-skipped { background: rgba(250, 204, 21, 0.08); color: #eab308; border: 1px solid rgba(250, 204, 21, 0.2); }
  tr.row-failed { background: rgba(248, 113, 113, 0.03); }
  tr.row-failed:hover { background: rgba(248, 113, 113, 0.06); }
  tr.row-skipped { background: rgba(250, 204, 21, 0.03); }
  tr.row-skipped:hover { background: rgba(250, 204, 21, 0.06); }
  .error-tip {
    display: block;
    font-size: 0.73rem;
    color: var(--red);
    margin-top: 5px;
    opacity: 0.75;
    font-weight: 400;
  }

  /* Show more button */
  .show-more-wrap {
    text-align: center;
    padding: 20px;
  }
  .show-more-wrap.hidden { display: none; }
  .show-more-btn {
    background: var(--bg-elevated);
    color: var(--text-secondary);
    border: 1px solid var(--border);
    padding: 8px 28px;
    border-radius: 8px;
    font-size: 0.82rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease;
  }
  .show-more-btn:hover { border-color: var(--accent); color: var(--text-primary); background: var(--accent-glow); }

  .empty {
    text-align: center;
    padding: 80px 20px;
    color: var(--text-muted);
    font-size: 0.95rem;
  }
  .empty .icon { font-size: 2.5rem; margin-bottom: 16px; opacity: 0.6; }
  @media (max-width: 700px) {
    body { padding: 16px; }
    table { font-size: 0.8rem; }
    td, th { padding: 8px 6px; }
    .active-dl-stats { gap: 16px; flex-wrap: wrap; }
  }
</style>
</head>
<body>
  <div class="header">
    <h1><img class="logo" src="/icon.png" alt="QbitFetch">Qbit<span>Fetch</span></h1>
    <span class="status"><span class="dot"></span>Monitoring</span>
  </div>

  <div id="active-download" class="active-dl hidden">
    <div class="active-dl-header">
      <span class="label"><span class="indicator" id="dl-indicator"></span>Downloading</span>
      <button class="pause-btn" id="pause-btn" onclick="togglePause()">
        <span id="pause-icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg"><path d="M6 18.4V5.6C6 5.26863 6.26863 5 6.6 5H9.4C9.73137 5 10 5.26863 10 5.6V18.4C10 18.7314 9.73137 19 9.4 19H6.6C6.26863 19 6 18.7314 6 18.4Z"/><path d="M14 18.4V5.6C14 5.26863 14.2686 5 14.6 5H17.4C17.7314 5 18 5.26863 18 5.6V18.4C18 18.7314 17.7314 19 17.4 19H14.6C14.2686 19 14 18.7314 14 18.4Z"/></svg></span>
        <span id="pause-text">Pause</span>
      </button>
    </div>
    <div class="active-dl-name" id="dl-name"></div>
    <div class="progress-wrap">
      <div class="progress-bar" id="dl-progress" style="width: 0%"></div>
    </div>
    <div class="active-dl-stats">
      <div>
        <div class="stat-label">Progress</div>
        <div class="stat-value" id="dl-pct">0%</div>
      </div>
      <div>
        <div class="stat-label">Speed</div>
        <div class="stat-value speed" id="dl-speed">&mdash;</div>
      </div>
      <div>
        <div class="stat-label">Downloaded</div>
        <div class="stat-value size" id="dl-size">&mdash;</div>
      </div>
      <div>
        <div class="stat-label">Elapsed</div>
        <div class="stat-value time" id="dl-elapsed">&mdash;</div>
      </div>
      <div>
        <div class="stat-label">ETA</div>
        <div class="stat-value time" id="dl-eta">&mdash;</div>
      </div>
    </div>
  </div>

  <div id="content">
    <div class="empty"><div class="icon">...</div>Loading...</div>
  </div>

  <div id="show-more-wrap" class="show-more-wrap hidden">
    <button class="show-more-btn" onclick="showMore()">Show more</button>
  </div>

<script>
/* ---- Relative time ---- */
function timeAgo(epoch) {
  if (!epoch) return '';
  const now = Date.now() / 1000;
  const diff = Math.max(0, now - epoch);
  if (diff < 60) return 'just now';
  if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
  if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
  if (diff < 604800) return Math.floor(diff / 86400) + 'd ago';
  return new Date(epoch * 1000).toLocaleDateString();
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s || '';
  return d.innerHTML;
}

/* ---- History table ---- */
let allEntries = [];
let displayLimit = 20;

function renderTable(entries) {
  allEntries = entries || [];
  const show = allEntries.slice(0, displayLimit);
  const el = document.getElementById('content');
  const moreWrap = document.getElementById('show-more-wrap');

  if (show.length === 0) {
    el.innerHTML = '<div class="empty"><div class="icon">&#128230;</div>No downloads yet. Tag a torrent with <b>ftp</b> to get started.</div>';
    moreWrap.classList.add('hidden');
    return;
  }

  let html = '<table><thead><tr>';
  html += '<th>Status</th><th>Time</th><th>Name</th><th>Size</th><th>Speed</th><th>Duration</th>';
  html += '</tr></thead><tbody>';
  for (const e of show) {
    const st = e.status || 'completed';
    const rowClass = st === 'failed' ? ' class="row-failed"' : st === 'skipped' ? ' class="row-skipped"' : '';
    const badge = '<span class="badge badge-' + esc(st) + '">' + esc(st) + '</span>';
    const errorTip = (st === 'failed' && e.error) ? '<span class="error-tip">' + esc(e.error) + '</span>' : '';
    const relTime = timeAgo(e.epoch);
    const fullTime = esc(e.timestamp);
    html += '<tr' + rowClass + '>';
    html += '<td>' + badge + '</td>';
    html += '<td class="time" title="' + fullTime + '">' + esc(relTime) + '</td>';
    html += '<td class="name">' + esc(e.name) + errorTip + '</td>';
    html += '<td class="size">' + esc(e.size_human) + '</td>';
    html += '<td class="speed">' + esc(e.speed_human) + '</td>';
    html += '<td class="duration">' + esc(e.duration_human) + '</td>';
    html += '</tr>';
  }
  html += '</tbody></table>';
  el.innerHTML = html;

  // Show "show more" button if there are more entries
  if (allEntries.length > displayLimit) {
    moreWrap.classList.remove('hidden');
    moreWrap.querySelector('.show-more-btn').textContent =
      'Show more (' + (allEntries.length - displayLimit) + ' remaining)';
  } else {
    moreWrap.classList.add('hidden');
  }
}

function showMore() {
  displayLimit = MAX_DISPLAY;
  renderTable(allEntries);
}

const MAX_DISPLAY = 100;

/* ---- Active download status ---- */
let isPaused = false;

function updateStatus(data) {
  const card = document.getElementById('active-download');
  if (!data || !data.active) {
    card.classList.add('hidden');
    return;
  }
  card.classList.remove('hidden');
  document.getElementById('dl-name').textContent = data.name;
  document.getElementById('dl-pct').textContent = data.percentage + '%';
  document.getElementById('dl-speed').textContent = data.speed_human;
  document.getElementById('dl-size').textContent =
    data.current_bytes_human + (data.remote_size > 0 ? ' / ' + data.remote_size_human : '');
  document.getElementById('dl-elapsed').textContent = data.elapsed_human;
  document.getElementById('dl-eta').textContent = data.eta_human || '\u2014';

  const bar = document.getElementById('dl-progress');
  bar.style.width = data.percentage + '%';

  isPaused = data.paused;
  const indicator = document.getElementById('dl-indicator');
  const pauseIcon = document.getElementById('pause-icon');
  const pauseText = document.getElementById('pause-text');

  if (isPaused) {
    bar.classList.add('paused');
    indicator.classList.add('paused');
    pauseIcon.innerHTML = '&#9654;';
    pauseText.textContent = 'Resume';
  } else {
    bar.classList.remove('paused');
    indicator.classList.remove('paused');
    pauseIcon.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg"><path d="M6 18.4V5.6C6 5.26863 6.26863 5 6.6 5H9.4C9.73137 5 10 5.26863 10 5.6V18.4C10 18.7314 9.73137 19 9.4 19H6.6C6.26863 19 6 18.7314 6 18.4Z"/><path d="M14 18.4V5.6C14 5.26863 14.2686 5 14.6 5H17.4C17.7314 5 18 5.26863 18 5.6V18.4C18 18.7314 17.7314 19 17.4 19H14.6C14.2686 19 14 18.7314 14 18.4Z"/></svg>';
    pauseText.textContent = 'Pause';
  }
}

async function togglePause() {
  const btn = document.getElementById('pause-btn');
  btn.disabled = true;
  try {
    const action = isPaused ? 'resume' : 'pause';
    await fetch('/api/' + action, { method: 'POST' });
    // Immediately refresh status
    await refreshStatus();
  } catch (e) {
    console.error('Pause/resume failed:', e);
  }
  btn.disabled = false;
}

async function refreshStatus() {
  try {
    const r = await fetch('/api/status');
    const data = await r.json();
    updateStatus(data);
  } catch (e) {
    console.error('Status refresh failed:', e);
  }
}

/* ---- Periodic refresh ---- */
async function refresh() {
  try {
    const [histRes, statRes] = await Promise.all([
      fetch('/api/history?limit=' + MAX_DISPLAY),
      fetch('/api/status')
    ]);
    const histData = await histRes.json();
    const statData = await statRes.json();
    renderTable(histData);
    updateStatus(statData);
  } catch (e) {
    console.error('Refresh failed:', e);
  }
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"""


class DashboardHandler(BaseHTTPRequestHandler):
    """HTTP handler for the QbitFetch dashboard."""

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/" or parsed.path == "/index.html":
            self._serve_html()
        elif parsed.path == "/icon.png":
            self._serve_icon()
        elif parsed.path == "/api/history":
            self._serve_api_history(parsed.query)
        elif parsed.path == "/api/status":
            self._serve_api_status()
        else:
            self.send_error(404)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/pause":
            self._handle_pause()
        elif parsed.path == "/api/resume":
            self._handle_resume()
        else:
            self.send_error(404)

    def _serve_html(self):
        content = DASHBOARD_HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _serve_icon(self):
        if _ICON_PNG:
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(_ICON_PNG)))
            self.send_header("Cache-Control", "public, max-age=86400")
            self.end_headers()
            self.wfile.write(_ICON_PNG)
        else:
            self.send_error(404)

    def _serve_api_history(self, query_string: str = ""):
        history: DownloadHistory = self.server.history
        params = parse_qs(query_string)
        try:
            limit = int(params.get("limit", [20])[0])
            limit = max(1, min(limit, MAX_HISTORY))
        except (ValueError, IndexError):
            limit = 20
        data = json.dumps(history.get_entries(limit), ensure_ascii=False)
        self._send_json(data)

    def _serve_api_status(self):
        dl_state: DownloadState = self.server.dl_state
        data = json.dumps(dl_state.to_dict(), ensure_ascii=False)
        self._send_json(data)

    def _handle_pause(self):
        dl_state: DownloadState = self.server.dl_state
        ok = dl_state.request_pause()
        self._send_json(json.dumps({"ok": ok}))

    def _handle_resume(self):
        dl_state: DownloadState = self.server.dl_state
        ok = dl_state.request_resume()
        self._send_json(json.dumps({"ok": ok}))

    def _send_json(self, data: str):
        content = data.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format, *args):
        """Suppress default request logging to keep logs clean."""
        pass


def start_dashboard(history: DownloadHistory, dl_state: DownloadState,
                    port: int = 8686) -> threading.Thread:
    """Start the dashboard web server in a background daemon thread."""
    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    server.history = history
    server.dl_state = dl_state

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info("Dashboard started: http://0.0.0.0:%d", port)
    return thread
