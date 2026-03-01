#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
import shlex
import signal
import stat
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import paramiko
import qbittorrentapi

from dashboard import DownloadHistory, DownloadState, start_dashboard

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO, stream=sys.stdout)
log = logging.getLogger("qbit-fetch")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    # qBittorrent Web API
    qbit_host: str = "http://127.0.0.1:8080"
    qbit_user: str = "admin"
    qbit_pass: str = "adminadmin"
    # Tags
    source_tag: str = "ftp"
    done_tag: str = "ok"
    error_tag: str = "error"
    # SFTP (the machine where torrents are stored)
    sftp_host: str = "127.0.0.1"
    sftp_port: int = 22
    sftp_user: str = "user"
    sftp_pass: Optional[str] = None
    sftp_key_path: Optional[str] = None
    sftp_key_passphrase: Optional[str] = None
    # Local destination
    local_dir: Path = Path("./downloads_from_remote")
    # Looping
    poll_seconds: int = 30
    # If True, skip downloading when destination already exists
    skip_if_exists: bool = True
    # SSH keep-alive interval in seconds (0 = disabled)
    ssh_keepalive: int = 15
    # Number of parallel lftp connections/segments
    parallel: int = 4
    # Max consecutive failures before longer backoff
    max_backoff: int = 300
    # Dashboard web server port (0 = disabled)
    dashboard_port: int = 8686


def parse_qbit_host(raw_host: str) -> tuple[str, Optional[str], Optional[str]]:
    """
    Parse QBIT_HOST, stripping embedded credentials if present.
    Returns (clean_host, user_from_url, pass_from_url).
    e.g. 'https://user:pass@host/path' -> ('https://host/path', 'user', 'pass')
    """
    parsed = urlparse(raw_host)
    if parsed.username or parsed.password:
        # Rebuild URL without credentials
        clean = parsed._replace(
            netloc=parsed.hostname + (f":{parsed.port}" if parsed.port else "")
        ).geturl()
        return clean, parsed.username, parsed.password
    return raw_host, None, None


# ---------------------------------------------------------------------------
# qBittorrent
# ---------------------------------------------------------------------------
class QbitManager:
    """Manages a qBittorrent API connection with automatic reconnection."""

    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._client: Optional[qbittorrentapi.Client] = None

    @property
    def client(self) -> qbittorrentapi.Client:
        """Return a live qBittorrent client, reconnecting if necessary."""
        if self._client is None:
            self._connect()
        return self._client

    def _connect(self) -> None:
        """(Re)establish the qBittorrent API connection."""
        cfg = self._cfg
        self._client = qbittorrentapi.Client(
            host=cfg.qbit_host,
            username=cfg.qbit_user,
            password=cfg.qbit_pass,
        )
        self._client.auth_log_in()
        log.info("qBittorrent connected: %s", cfg.qbit_host)

    def reconnect(self) -> None:
        """Force a fresh reconnection on next access."""
        self._client = None


def is_complete(t) -> bool:
    try:
        return float(t.progress) >= 0.999999
    except Exception:
        return False


def torrent_content_remote_path(t) -> str:
    """
    Returns the full remote path to the torrent's root content (file or folder).
    Prefer content_path; fallback to save_path/name.
    """
    cp = getattr(t, "content_path", None)
    if cp:
        return str(cp)
    save_path = str(t.save_path).rstrip("/\\")
    return f"{save_path}/{t.name}"


# ---------------------------------------------------------------------------
# SFTP connection manager (with reconnect + keep-alive)
# ---------------------------------------------------------------------------
class SFTPManager:
    """Manages an SSH/SFTP connection with automatic reconnection."""

    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._ssh: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    @property
    def sftp(self) -> paramiko.SFTPClient:
        """Return a live SFTP client, reconnecting if necessary."""
        if not self._is_alive():
            self._connect()
        return self._sftp

    def _is_alive(self) -> bool:
        """Check whether the current SFTP session is still usable."""
        if self._sftp is None or self._ssh is None:
            return False
        transport = self._ssh.get_transport()
        if transport is None or not transport.is_active():
            return False
        # Probe the connection with a lightweight stat call
        try:
            self._sftp.stat(".")
            return True
        except Exception:
            return False

    def _connect(self) -> None:
        """(Re)establish the SSH + SFTP connection."""
        self.close()
        cfg = self._cfg
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        pkey = None
        if cfg.sftp_key_path:
            key_path = os.path.expanduser(cfg.sftp_key_path)
            key_loaders = [
                paramiko.RSAKey, paramiko.Ed25519Key,
                paramiko.ECDSAKey, paramiko.DSSKey,
            ]
            last_err = None
            for loader in key_loaders:
                try:
                    pkey = loader.from_private_key_file(
                        key_path, password=cfg.sftp_key_passphrase,
                    )
                    break
                except Exception as e:
                    last_err = e
            if pkey is None:
                raise RuntimeError(f"Failed to load key '{key_path}': {last_err}")

        ssh.connect(
            hostname=cfg.sftp_host,
            port=cfg.sftp_port,
            username=cfg.sftp_user,
            password=cfg.sftp_pass if not pkey else None,
            pkey=pkey,
            look_for_keys=False,
            allow_agent=False,
            timeout=20,
        )

        # Enable SSH keep-alive to prevent idle disconnects
        transport = ssh.get_transport()
        if transport and cfg.ssh_keepalive > 0:
            transport.set_keepalive(cfg.ssh_keepalive)

        self._ssh = ssh
        self._sftp = ssh.open_sftp()
        log.info("SFTP connected: %s@%s:%d", cfg.sftp_user, cfg.sftp_host, cfg.sftp_port)

    def close(self) -> None:
        """Cleanly close the SFTP and SSH sessions."""
        for resource in (self._sftp, self._ssh):
            try:
                if resource:
                    resource.close()
            except Exception:
                pass
        self._sftp = None
        self._ssh = None


# ---------------------------------------------------------------------------
# Download helpers — uses lftp for fast, resumable SFTP transfers
# ---------------------------------------------------------------------------
PART_SUFFIX = ".part"


def sftp_is_dir(sftp: paramiko.SFTPClient, remote_path: str) -> bool:
    st = sftp.stat(remote_path)
    return stat.S_ISDIR(st.st_mode)


def fix_permissions(local_path: Path) -> None:
    """Make downloaded files/dirs world-readable for Unraid compatibility."""
    try:
        if local_path.is_dir():
            local_path.chmod(0o777)
            for child in local_path.rglob("*"):
                if child.is_dir():
                    child.chmod(0o777)
                else:
                    child.chmod(0o666)
        else:
            local_path.chmod(0o666)
    except Exception as e:
        log.warning("Could not fix permissions on '%s': %s", local_path, e)


def format_size(num_bytes: float) -> str:
    """Format bytes into a human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} PB"


def format_speed(bytes_per_sec: float) -> str:
    """Format bytes/sec into a human-readable speed string."""
    return f"{format_size(bytes_per_sec)}/s"


def _get_disk_usage(path: Path) -> int:
    """
    Get actual disk usage (bytes written) for a file or directory.
    Uses st_blocks * 512 which reflects real bytes on disk, not apparent size.
    This is critical for lftp pget which pre-allocates files at full size.
    """
    total = 0
    try:
        if path.is_file():
            total = path.stat().st_blocks * 512
        elif path.is_dir():
            for f in path.rglob("*"):
                if f.is_file():
                    total += f.stat().st_blocks * 512
    except Exception:
        pass
    return total


def _progress_monitor(local_dir: Path, filename: str, remote_size: int,
                      is_dir: bool, stop_event: threading.Event,
                      dl_state: DownloadState = None,
                      interval: int = 10) -> None:
    """
    Background thread that monitors download progress by checking
    actual disk usage every `interval` seconds.
    Uses st_blocks (real bytes written) instead of apparent file size,
    which is critical because lftp pget pre-allocates files at full size.
    Also updates the shared DownloadState for the web dashboard.
    """
    prev_bytes = 0
    start_time = time.time()

    while not stop_event.wait(timeout=interval):
        # Calculate actual bytes written to disk
        target = local_dir / filename
        if is_dir:
            current_bytes = _get_disk_usage(target) if target.exists() else 0
        else:
            # Check the target file plus any lftp temp/status files
            current_bytes = 0
            for f in local_dir.glob(f"{filename}*"):
                if f.is_file():
                    current_bytes += f.stat().st_blocks * 512

        elapsed = time.time() - start_time
        speed = (current_bytes - prev_bytes) / interval if prev_bytes > 0 else 0
        prev_bytes = current_bytes

        # Update shared state for the dashboard
        if dl_state:
            dl_state.update(current_bytes, speed, elapsed)

        # Cap percentage at 100% (disk usage can slightly exceed due to block alignment)
        if remote_size > 0:
            pct = min((current_bytes / remote_size) * 100, 100.0)
            log.info("[LFTP] %s — %.1f%% (%s / %s) at %s",
                     filename, pct, format_size(current_bytes),
                     format_size(remote_size), format_speed(speed))
        elif current_bytes > 0:
            log.info("[LFTP] %s — %s downloaded at %s",
                     filename, format_size(current_bytes), format_speed(speed))


def lftp_download(cfg, remote_path: str, local_dir: Path, is_dir: bool,
                  sftp: paramiko.SFTPClient = None,
                  dl_state: DownloadState = None) -> tuple[int, float, float]:
    """
    Download a file or directory via lftp over SFTP.
    - Uses multiple parallel connections for maximum speed
    - Built-in resume support (continue partial transfers)
    - .part temp files for safe incomplete downloads
    - Live progress monitoring + summary at the end
    - Supports pause/resume via SIGSTOP/SIGCONT signals
    """
    local_dir.mkdir(parents=True, exist_ok=True)

    # Get remote size for progress reporting
    remote_size = 0
    filename = Path(remote_path.rstrip("/")).name
    if sftp:
        try:
            if is_dir:
                # Walk the remote dir to get total size
                def _remote_dir_size(path: str) -> int:
                    total = 0
                    for entry in sftp.listdir_attr(path):
                        child = path.rstrip("/") + "/" + entry.filename
                        if stat.S_ISDIR(entry.st_mode):
                            total += _remote_dir_size(child)
                        else:
                            total += entry.st_size
                    return total
                remote_size = _remote_dir_size(remote_path)
            else:
                remote_size = sftp.stat(remote_path).st_size
        except Exception:
            pass  # progress will just skip percentage

    # Build lftp connection string
    escaped_pass = cfg.sftp_pass.replace("\\", "\\\\").replace('"', '\\"') if cfg.sftp_pass else ""
    quoted_remote = shlex.quote(remote_path)

    lftp_settings = f"""set sftp:auto-confirm yes
set net:timeout 30
set net:max-retries 5
set net:reconnect-interval-base 5
set xfer:use-temp-file yes
set xfer:temp-file-name *.part
set cmd:trace false
open -u {shlex.quote(cfg.sftp_user)},{shlex.quote(escaped_pass)} sftp://{cfg.sftp_host}:{cfg.sftp_port}
"""

    if is_dir:
        lftp_commands = lftp_settings + (
            f"mirror --continue --parallel={cfg.parallel} --use-pget-n={cfg.parallel} "
            f"{quoted_remote} {shlex.quote(str(local_dir / filename))}\n"
            f"exit\n"
        )
    else:
        lftp_commands = lftp_settings + (
            f"pget -n {cfg.parallel} -c {quoted_remote} -o {shlex.quote(str(local_dir))}/\n"
            f"exit\n"
        )

    size_str = f" ({format_size(remote_size)})" if remote_size > 0 else ""
    log.info("[LFTP] Starting transfer: %s%s -> %s (parallel=%d)",
             filename, size_str, local_dir, cfg.parallel)

    # Update shared state: download is active
    if dl_state:
        dl_state.start(filename, remote_size)

    start_time = time.time()
    pause_total = 0.0  # total seconds spent paused

    # Start background progress monitor
    stop_event = threading.Event()
    monitor = threading.Thread(
        target=_progress_monitor,
        args=(local_dir, filename, remote_size, is_dir, stop_event, dl_state, 10),
        daemon=True,
    )
    monitor.start()

    # Run lftp via Popen (allows pause/resume via signals)
    proc = subprocess.Popen(
        ["lftp", "-c", lftp_commands],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Store PID in dl_state so the dashboard can trigger pause/resume
    if dl_state:
        dl_state.set_pid(proc.pid)

    # Drain stdout/stderr in background threads to prevent pipe buffer deadlock.
    # If lftp writes more output than the OS pipe buffer can hold (~64 KB),
    # it blocks waiting for someone to read — while we block waiting for it
    # to exit. Collecting output in threads avoids this.
    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    def _drain(stream, dest: list[str]) -> None:
        try:
            for line in stream:
                dest.append(line)
        except Exception:
            pass

    stdout_thread = threading.Thread(target=_drain, args=(proc.stdout, stdout_chunks), daemon=True)
    stderr_thread = threading.Thread(target=_drain, args=(proc.stderr, stderr_chunks), daemon=True)
    stdout_thread.start()
    stderr_thread.start()

    # Wait for lftp to finish, checking for pause/resume requests
    while proc.poll() is None:
        if dl_state and dl_state.should_pause():
            # User requested pause — send SIGSTOP to lftp process tree
            try:
                os.kill(proc.pid, signal.SIGSTOP)
                dl_state.set_paused(True)
                log.info("[LFTP] Download paused: %s", filename)
            except OSError as e:
                dl_state.clear_pause_request()
                log.warning("Could not pause lftp: %s", e)

        if dl_state and dl_state.should_resume():
            # User requested resume — send SIGCONT
            try:
                os.kill(proc.pid, signal.SIGCONT)
                dl_state.set_paused(False)
                log.info("[LFTP] Download resumed: %s", filename)
            except OSError as e:
                dl_state.clear_resume_request()
                log.warning("Could not resume lftp: %s", e)

        time.sleep(0.5)

    # Wait for drain threads and process cleanup
    stdout_thread.join(timeout=5)
    stderr_thread.join(timeout=5)
    proc.wait()

    # Stop progress monitor
    stop_event.set()
    monitor.join(timeout=5)

    stdout = "".join(stdout_chunks)
    stderr = "".join(stderr_chunks)

    elapsed = time.time() - start_time

    # Clear shared state
    if dl_state:
        dl_state.finish()

    # Check for errors
    if proc.returncode != 0:
        err = stderr.strip() if stderr else "unknown error"
        raise RuntimeError(f"lftp failed (exit {proc.returncode}): {err}")

    # Calculate final summary — use remote_size for accuracy if available,
    # since st_blocks can overshoot slightly due to block alignment
    target = local_dir / filename
    if remote_size > 0:
        total_bytes = remote_size
    elif target.exists():
        total_bytes = _get_disk_usage(target)
    else:
        total_bytes = 0

    avg_speed = total_bytes / elapsed if elapsed > 0 else 0
    log.info("[LFTP] Transfer complete: %s in %.1fs (avg %s)",
             format_size(total_bytes), elapsed, format_speed(avg_speed))

    return total_bytes, avg_speed, elapsed


# ---------------------------------------------------------------------------
# Tag management
# ---------------------------------------------------------------------------
def replace_tag(client: qbittorrentapi.Client, torrent_hash: str, old: str, new: str) -> None:
    try:
        client.torrents_create_tags(tags=new)
    except Exception:
        pass
    client.torrents_remove_tags(tags=old, torrent_hashes=torrent_hash)
    client.torrents_add_tags(tags=new, torrent_hashes=torrent_hash)


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------
def process_once(qbit_mgr: QbitManager, sftp_mgr: SFTPManager, cfg: Config,
                 history: DownloadHistory = None,
                 dl_state: DownloadState = None) -> int:
    processed = 0
    sftp = sftp_mgr.sftp  # auto-reconnects if dead
    qbit = qbit_mgr.client  # auto-reconnects if dead

    torrents = qbit.torrents_info(tag=cfg.source_tag)
    for t in torrents:
        torrent_tags = [tag.strip() for tag in t.tags.split(",") if tag.strip()]
        if cfg.source_tag not in torrent_tags:
            continue
        if not is_complete(t):
            continue

        remote_src = torrent_content_remote_path(t)
        base_name = Path(remote_src.rstrip("/")).name

        try:
            is_dir = sftp_is_dir(sftp, remote_src)
        except Exception as e:
            log.error("Could not stat remote '%s' (%s): %s", t.name, t.hash, e)
            sftp_mgr.close()
            continue

        if is_dir:
            # Directories go directly into local_dir
            local_dst = cfg.local_dir / base_name
        else:
            # Single files get wrapped in a folder (strip extension for folder name)
            folder_name = Path(base_name).stem
            wrapper_dir = cfg.local_dir / folder_name
            wrapper_dir.mkdir(parents=True, exist_ok=True)
            local_dst = wrapper_dir / base_name

        # Check for .part file (in-progress download from previous attempt).
        # If a .part file exists, the download was interrupted — don't skip,
        # let lftp resume it.
        part_file = local_dst.with_name(local_dst.name + PART_SUFFIX)
        has_partial = part_file.exists()

        # Skip check: for dirs, check if the dir exists; for single files,
        # check if the actual file exists inside the wrapper folder.
        # Never skip if a .part file exists — that means the previous
        # download was interrupted and needs to be resumed.
        if local_dst.exists() and cfg.skip_if_exists and not has_partial:
            log.info("[SKIP] %s -> '%s' already exists", t.name, local_dst)
            try:
                replace_tag(qbit, t.hash, cfg.source_tag, cfg.done_tag)
                log.info("[SKIP] %s -> tagged '%s'", t.name, cfg.done_tag)
            except Exception as e:
                log.error("Tag update failed for skipped '%s': %s", t.name, e)
            if history:
                history.add_entry(t.name, 0, 0, 0, status="skipped")
            continue
        if has_partial:
            log.info("[RESUME] %s -> partial download found, resuming", t.name)

        try:
            if is_dir:
                log.info("[DL] dir  %s -> %s", remote_src, local_dst)
                dl_bytes, dl_speed, dl_elapsed = lftp_download(
                    cfg, remote_src, cfg.local_dir, is_dir=True, sftp=sftp,
                    dl_state=dl_state)
            else:
                log.info("[DL] file %s -> %s", remote_src, local_dst)
                # Download into the wrapper directory
                dl_bytes, dl_speed, dl_elapsed = lftp_download(
                    cfg, remote_src, local_dst.parent, is_dir=False, sftp=sftp,
                    dl_state=dl_state)

            # Fix permissions for Unraid (fix the wrapper dir for single files)
            fix_target = local_dst if is_dir else local_dst.parent
            fix_permissions(fix_target)

        except Exception as e:
            error_msg = str(e)
            log.error("Download failed for '%s' (%s): %s", t.name, t.hash, error_msg)
            # Record failure in dashboard history
            if history:
                history.add_entry(t.name, 0, 0, 0, status="failed", error=error_msg)
            # Tag as error so it doesn't retry every poll cycle
            try:
                replace_tag(qbit, t.hash, cfg.source_tag, cfg.error_tag)
                log.info("[ERR] %s -> tagged '%s'", t.name, cfg.error_tag)
            except Exception as tag_err:
                log.error("Could not tag '%s' as error: %s", t.name, tag_err)
            # Force reconnect on next attempt
            sftp_mgr.close()
            continue

        try:
            replace_tag(qbit, t.hash, cfg.source_tag, cfg.done_tag)
        except Exception as e:
            log.error("Tag update failed for '%s' (%s): %s", t.name, t.hash, e)
            continue

        # Record success in dashboard history
        if history:
            history.add_entry(t.name, dl_bytes, dl_speed, dl_elapsed, status="completed")

        processed += 1
        log.info("[OK] %s -> downloaded and tagged '%s'", t.name, cfg.done_tag)

    return processed


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> int:
    # Handle SIGTERM for clean Docker shutdown
    signal.signal(signal.SIGTERM, lambda sig, frame: sys.exit(0))

    # --- qBittorrent host (pass as-is, credentials must stay in URL for reverse proxy) ---
    qbit_host = os.environ.get("QBIT_HOST", "http://127.0.0.1:8080")

    cfg = Config(
        qbit_host=qbit_host,
        qbit_user=os.environ.get("QBIT_USER", "admin"),
        qbit_pass=os.environ.get("QBIT_PASS", "adminadmin"),
        # Tags
        source_tag=os.environ.get("SOURCE_TAG", "ftp"),
        done_tag=os.environ.get("DONE_TAG", "ok"),
        error_tag=os.environ.get("ERROR_TAG", "error"),
        # SFTP
        sftp_host=os.environ.get("SFTP_HOST", "127.0.0.1"),
        sftp_port=int(os.environ.get("SFTP_PORT", "22")),
        sftp_user=os.environ.get("SFTP_USER", "user"),
        sftp_pass=os.environ.get("SFTP_PASS") or None,
        sftp_key_path=os.environ.get("SFTP_KEY") or None,
        sftp_key_passphrase=os.environ.get("SFTP_KEY_PASSPHRASE") or None,
        # Local
        local_dir=Path(os.environ.get("LOCAL_DIR", "./downloads_from_remote")),
        poll_seconds=int(os.environ.get("POLL_SECONDS", "30")),
        skip_if_exists=os.environ.get("SKIP_IF_EXISTS", "1") != "0",
        ssh_keepalive=int(os.environ.get("SSH_KEEPALIVE", "15")),
        parallel=int(os.environ.get("LFTP_PARALLEL", "4")),
        dashboard_port=int(os.environ.get("DASHBOARD_PORT", "8686")),
    )

    log.info("QBIT_HOST: %s", cfg.qbit_host)

    cfg.local_dir.mkdir(parents=True, exist_ok=True)
    if not os.access(cfg.local_dir, os.W_OK):
        log.critical("LOCAL_DIR '%s' is not writable!", cfg.local_dir)
        return 1

    # --- qBittorrent manager (handles reconnection automatically) ---
    qbit_mgr = QbitManager(cfg)
    try:
        _ = qbit_mgr.client  # initial connection test
    except Exception as e:
        log.critical("Could not connect/login to qBittorrent: %s", e)
        return 2

    # --- SFTP manager (handles reconnection automatically) ---
    sftp_mgr = SFTPManager(cfg)
    try:
        _ = sftp_mgr.sftp  # initial connection test
    except Exception as e:
        log.critical("Could not connect via SFTP: %s", e)
        return 3

    log.info("Connected to qBittorrent: %s", cfg.qbit_host)
    log.info("Connected via SFTP: %s@%s:%d", cfg.sftp_user, cfg.sftp_host, cfg.sftp_port)
    log.info("Watching tag '%s' -> download to '%s' -> retag '%s'", cfg.source_tag, cfg.local_dir, cfg.done_tag)
    log.info("SSH keep-alive: %ds | Poll interval: %ds", cfg.ssh_keepalive, cfg.poll_seconds)

    # --- Download history + state + web dashboard ---
    history = DownloadHistory(cfg.local_dir / ".qbitfetch_history.json")
    dl_state = DownloadState()
    if cfg.dashboard_port > 0:
        try:
            start_dashboard(history, dl_state, cfg.dashboard_port)
        except Exception as e:
            log.warning("Could not start dashboard on port %d: %s", cfg.dashboard_port, e)

    consecutive_failures = 0

    while True:
        try:
            n = process_once(qbit_mgr, sftp_mgr, cfg, history=history, dl_state=dl_state)
            consecutive_failures = 0  # reset on success
            if n == 0:
                log.info("No completed '%s' torrents. Sleeping %ds...", cfg.source_tag, cfg.poll_seconds)
            time.sleep(cfg.poll_seconds)

        except KeyboardInterrupt:
            log.info("Exiting (keyboard interrupt).")
            sftp_mgr.close()
            return 0

        except Exception as e:
            consecutive_failures += 1
            # Force qBittorrent reconnect on next attempt
            qbit_mgr.reconnect()
            backoff = min(cfg.poll_seconds * (2 ** consecutive_failures), cfg.max_backoff)
            log.error("Loop error (failure #%d): %s — retrying in %ds",
                      consecutive_failures, e, backoff)
            time.sleep(backoff)


if __name__ == "__main__":
    raise SystemExit(main())
