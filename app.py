#!/usr/bin/env python3
"""Streamlit GUI for mzML Converter."""

import os
import re
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import streamlit as st

from watch_and_convert import run_conversion, is_valid_mzml, is_blank_sample, dir_size, has_required_files
from pathlib import Path

# File logging setup
# Use __file__ to get the directory of this script
APP_DIR = Path(__file__).parent.resolve()
LOG_FILE = APP_DIR / "conversion.log"
DEFAULT_DOCKER_IMAGE = "mfreitas/tdf2mzml"


def log_to_file(message: str):
    """Append a log message to conversion.log with timestamp."""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        with open(str(LOG_FILE), "a") as f:
            f.write(f"{timestamp} INFO: [GUI] {message}\n")
            f.flush()  
            os.fsync(f.fileno()) 
    except Exception:
        pass  

# Page config
st.set_page_config(
    page_title="mzML Converter",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def _ensure_session_state_defaults() -> None:
    """Initialize all session-state keys used by the app."""
    defaults = {
        "statuses": dict,
        "logs": list,
        "progress": dict,
        "src_dir": lambda: os.path.abspath("."),
        "out_dir": lambda: os.path.abspath("."),
        "bg_lock": threading.Lock,
        "bg_statuses": dict,
        "bg_logs": list,
        "bg_progress": dict,
        "bg_expected_sizes": dict,
        "bg_output_paths": dict,
        "bg_errors": dict,
        "bg_queued_since": dict,
        "bg_control": lambda: {
            "stop": False,
            "auto_stop": False,
            "auto_running": False,
            "in_flight_names": [],
        },
    }
    for key, factory in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = factory()


_ensure_session_state_defaults()

# Shortcuts for cleaner code
bg_lock = st.session_state.bg_lock
bg_statuses = st.session_state.bg_statuses
bg_logs = st.session_state.bg_logs
bg_progress = st.session_state.bg_progress
bg_expected_sizes = st.session_state.bg_expected_sizes
bg_output_paths = st.session_state.bg_output_paths
bg_errors = st.session_state.bg_errors
bg_queued_since = st.session_state.bg_queued_since
bg_control = st.session_state.bg_control

# Helper functions
# Regex pattern to match blank samples (e.g., "Blank-02", "Blank_1", "Blank01")
BLANK_PATTERN = re.compile(r"Blank[-_]?\d+", re.IGNORECASE)


def normalized_dataset_name(name: str) -> str:
    """Normalize dataset dir names by trimming trailing whitespace."""
    return name.rstrip()


def is_dataset_dir_name(name: str) -> bool:
    """Return True for names ending in .d, allowing trailing whitespace."""
    return normalized_dataset_name(name).lower().endswith(".d")


def dataset_base_name(name: str) -> str:
    """Return dataset base name without trailing .d (robust to whitespace)."""
    normalized = normalized_dataset_name(name)
    if normalized.lower().endswith(".d"):
        return normalized[:-2]
    return normalized


@st.cache_data(ttl=1, show_spinner=False)
def list_d_folders(path: str, exclude_blanks: bool = True) -> list[str]:
    """List .d folders in path. Cached for 1 second."""
    try:
        entries = [d for d in os.listdir(path) if is_dataset_dir_name(d) and os.path.isdir(os.path.join(path, d))]
        if exclude_blanks:
            entries = [d for d in entries if not is_blank_sample(d)]
    except Exception:
        entries = []
    return sorted(entries)

@st.cache_data(ttl=5, show_spinner=False)
def list_subdirs(path: str) -> list[str]:
    """List subdirectories in path. Cached for 5 seconds."""
    try:
        return sorted([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))])
    except Exception:
        return []


def get_file_size(path: str) -> int:
    """Get file size, return 0 if file doesn't exist."""
    try:
        return os.path.getsize(path) if os.path.exists(path) else 0
    except OSError:
        return 0


def read_last_lines(path: Path, max_lines: int = 100, block_size: int = 8192) -> list[str]:
    """Read up to max_lines from end of file without loading whole file."""
    if max_lines <= 0:
        return []
    if not path.exists():
        return []

    try:
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            buffer = b""
            pos = file_size
            # Read backwards in chunks until enough lines are found.
            while pos > 0 and buffer.count(b"\n") <= max_lines:
                read_size = min(block_size, pos)
                pos -= read_size
                f.seek(pos)
                buffer = f.read(read_size) + buffer

        text = buffer.decode("utf-8", errors="replace")
        lines = [line for line in text.splitlines() if line.strip()]
        return lines[-max_lines:]
    except Exception:
        return []
    
@st.cache_data(ttl=10, show_spinner=False)
def get_mzml_status(d_folder_name: str, out_dir: str, validate_interval: int = 1) -> tuple[str, str]:
    """Check if mzML exists and whether it's valid. Cached for 10 seconds.
    Returns (status_code, display_text) where status_code is:
    - 'none': no mzML file
    - 'valid': mzML exists and is valid
    - 'invalid': mzML exists but is incomplete/invalid
    """
    base = dataset_base_name(d_folder_name)
    mzml_path = os.path.join(out_dir, base + ".mzML")
    
    try:
        if not os.path.exists(mzml_path):
            return "none", "—"
        
        size_mb = os.path.getsize(mzml_path) / (1024 * 1024)
        
        # File exists, check if valid (use 0 interval for quick UI check)
        if is_valid_mzml(mzml_path, validate_interval=0):
            return "valid", f"✅ Valid ({size_mb:.1f} MB)"
        else:
            return "invalid", f"⚠️ Incomplete ({size_mb:.1f} MB)"
    except (OSError, FileNotFoundError):
        # File was deleted during check (race condition)
        return "none", "—"


def status_badge(status: str) -> str:
    if status == "running":
        return "🔄 Running"
    elif status == "starting":
        return "🚀 Starting"
    elif status == "done":
        return "✅ Done"
    elif status == "queued":
        return "⏳ Queued"
    elif status == "waiting":
        return "⏳ Checking"
    elif status == "stopped":
        return "⏹️ Stopped"
    elif status.startswith("failed"):
        return f"❌ {status}"
    return status


@st.cache_data(ttl=10, show_spinner=False)
def is_docker_running() -> tuple[bool, str]:
    """Check if Docker daemon is running. Cached for 10 seconds.
    Returns (is_running, message)"""
    if not shutil.which("docker"):
        return False, "Docker is not installed"
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            return True, "Docker is running"
        else:
            return False, "Docker daemon is not running. Please start Docker Desktop."
    except subprocess.TimeoutExpired:
        return False, "Docker check timed out"
    except Exception as e:
        return False, f"Docker check failed: {e}"


def wait_for_stable_size(
    folder_path: str,
    check_interval: int = 10,
    stability_checks: int = 3,
    log_callback=None,
    stop_flag=None,
) -> bool:
    """Wait until folder size is stable (not being copied).
    
    Returns True if stable, False if stopped or error.
    """
    stable_count = 0
    last_size = -1
    
    while stable_count < stability_checks:
        # Check stop flag
        if stop_flag and stop_flag.get("stop"):
            return False
        
        try:
            current_size = dir_size(folder_path)
        except Exception as e:
            if log_callback:
                log_callback(f"⚠️ Error checking folder size: {e}")
            return False
        
        if current_size == last_size:
            stable_count += 1
            if log_callback:
                log_callback(f"   📊 Size stable: {current_size / (1024*1024):.1f} MB (check {stable_count}/{stability_checks})")
        else:
            stable_count = 0
            if log_callback and last_size >= 0:
                log_callback(f"   📊 Size changed: {last_size / (1024*1024):.1f} → {current_size / (1024*1024):.1f} MB, waiting...")
            elif log_callback:
                log_callback(f"   📊 Initial size: {current_size / (1024*1024):.1f} MB, checking stability...")
        
        last_size = current_size
        
        if stable_count < stability_checks:
            time.sleep(check_interval)
    
    return True


# Conversion worker

def convert_single_dataset(
    name: str, src: str, out: str, docker_image: str,
    stability_check_interval: int = 10, stability_checks: int = 3,
) -> bool:
    """Convert a single .d folder. Returns True on success."""
    # Check stop flag before starting
    if bg_control["stop"]:
        with bg_lock:
            bg_statuses[name] = "stopped"
        return False
    
    full = os.path.join(src, name)
    out_path_dir = out or src
    
    # Wait for folder size to stabilize (ensure copy is complete)
    with bg_lock:
        bg_statuses[name] = "waiting"
        msg = f"⏳ Waiting for {name} to finish copying..."
        bg_logs.append(msg)
        log_to_file(msg)
    
    def _stability_log(msg):
        with bg_lock:
            bg_logs.append(msg)
        log_to_file(msg)
    
    is_stable = wait_for_stable_size(
        full,
        check_interval=stability_check_interval,
        stability_checks=stability_checks,
        log_callback=_stability_log,
        stop_flag=bg_control,
    )
    
    if not is_stable:
        with bg_lock:
            bg_statuses[name] = "stopped"
            msg = f"⏹️ Skipped {name} (stopped or error)"
            bg_logs.append(msg)
            log_to_file(msg)
        return False
    
    # Calculate expected mzML size (~87% of .d folder size)
    d_size = dir_size(full)
    expected_mzml_size = int(d_size * 0.87)
    
    # Determine output file path
    base_name = dataset_base_name(name)
    out_file_path = os.path.join(out_path_dir, base_name + ".mzML")

    # Initialize error list for this dataset
    error_lines = []

    with bg_lock:
        bg_statuses[name] = "starting"
        bg_progress[name] = 0
        bg_expected_sizes[name] = expected_mzml_size
        bg_output_paths[name] = out_file_path
        bg_errors[name] = []  # Clear previous errors
        logs_to_add = [
            f"▶ Starting: {name}",
            f"   .d size: {d_size / (1024*1024):.1f} MB, expected mzML: ~{expected_mzml_size / (1024*1024):.1f} MB",
            f"   Input path: {full}",
            f"   Output path: {out_file_path}",
            f"   Docker image: {docker_image}",
            f"⏳ Launching Docker container...",
        ]
        for log_msg in logs_to_add:
            bg_logs.append(log_msg)
            log_to_file(log_msg)

    def _line_cb(line: str, dataset=name, expected=expected_mzml_size, mzml_path=out_file_path, errors=error_lines):
        with bg_lock:
            if bg_statuses.get(dataset) in ("starting", "queued", "waiting"):
                bg_statuses[dataset] = "running"
        with bg_lock:
            bg_logs.append(line)
        log_to_file(line)
        
        # Capture error/warning lines
        line_lower = line.lower()
        if any(kw in line_lower for kw in ["error", "exception", "failed", "traceback", "cannot", "unable", "warning", "fatal", "abort", "invalid", "denied", "refused"]):
            errors.append(line)
        
        # Update progress based on output file size
        if expected > 0:
            current_size = get_file_size(mzml_path)
            pct = min(99, int((current_size / expected) * 100))  # Cap at 99% until validated
            with bg_lock:
                bg_progress[dataset] = pct

    try:
        rc, out_file = run_conversion(
            full,
            out_path_dir,
            docker_image=docker_image,
            line_callback=_line_cb,
        )
    except Exception as e:
        rc, out_file = 99, ""
        error_lines.append(f"Exception: {e}")
        with bg_lock:
            bg_logs.append(f"❌ Exception: {e}")
        log_to_file(f"❌ Exception: {e}")

    # Wait a bit for filesystem to sync after Docker exits
    time.sleep(2)
    
    # Check each condition separately for debugging
    file_exists = out_file and os.path.exists(out_file)
    file_valid = file_exists and is_valid_mzml(out_file, validate_interval=2)
    
    # Log detailed status
    with bg_lock:
        if rc == 0:
            if not out_file:
                bg_logs.append(f"   ⚠️ Debug: out_file is empty")
                log_to_file(f"   ⚠️ Debug: out_file is empty")
            elif not file_exists:
                bg_logs.append(f"   ⚠️ Debug: file not found at {out_file}")
                log_to_file(f"   ⚠️ Debug: file not found at {out_file}")
            elif not file_valid:
                file_size = get_file_size(out_file) if file_exists else 0
                bg_logs.append(f"   ⚠️ Debug: file exists ({file_size/(1024*1024):.1f} MB) but failed validation (missing closing tag)")
                log_to_file(f"   ⚠️ Debug: file exists ({file_size/(1024*1024):.1f} MB) but failed validation")
    
    ok = rc == 0 and file_exists and file_valid

    with bg_lock:
        if ok:
            final_size = get_file_size(out_file)
            bg_statuses[name] = "done"
            bg_progress[name] = 100
            bg_errors[name] = []  # Clear errors on success
            msg = f"✅ Completed: {name} ({final_size / (1024*1024):.1f} MB)"
            bg_logs.append(msg)
            log_to_file(msg)
        else:
            bg_statuses[name] = f"failed (rc={rc})"
            # Store all captured error lines
            bg_errors[name] = error_lines if error_lines else [f"Conversion failed with exit code {rc}"]
            msg = f"❌ Failed: {name} (rc={rc})"
            bg_logs.append(msg)
            log_to_file(msg)
    
    return ok


def start_conversion(
    selected: list[str], src: str, out: str, docker_image: str,
    stability_check_interval: int = 10, stability_checks: int = 3,
    max_workers: int = 2,
):
    """Start parallel conversion of multiple datasets."""
    bg_control["stop"] = False  # Reset stop flag
    
    # Add immediate log entry
    with bg_lock:
        msg = f"🚀 Starting parallel conversion of {len(selected)} dataset(s) with {max_workers} worker(s)..."
        bg_logs.append(msg)
        log_to_file(msg)
    
    def orchestrator():
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    convert_single_dataset, name, src, out, docker_image,
                    stability_check_interval, stability_checks
                ): name for name in selected
            }
            
            completed = 0
            failed = 0
            for future in as_completed(futures):
                name = futures[future]
                try:
                    success = future.result()
                    if success:
                        completed += 1
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    with bg_lock:
                        bg_logs.append(f"❌ Worker exception for {name}: {e}")
                        log_to_file(f"❌ Worker exception for {name}: {e}")
                
                # Check if stop was requested
                if bg_control["stop"]:
                    # Cancel remaining futures
                    for f in futures:
                        f.cancel()
                    with bg_lock:
                        msg = f"⛔ Conversion stopped. Completed: {completed}, Failed: {failed}"
                        bg_logs.append(msg)
                        log_to_file(msg)
                        # Mark remaining queued items as stopped
                        for n in selected:
                            if bg_statuses.get(n) == "queued":
                                bg_statuses[n] = "stopped"
                    break
            
            # Final summary
            with bg_lock:
                msg = f"🏁 Conversion batch finished. Completed: {completed}, Failed: {failed}"
                bg_logs.append(msg)
                log_to_file(msg)
    
    t = threading.Thread(target=orchestrator, daemon=True)
    t.start()


def start_auto_conversion(
    src: str,
    out: str,
    docker_image: str,
    stability_check_interval: int = 3,
    stability_checks: int = 2,
    max_workers: int = 4,
    poll_interval: int = 30,
    queue_grace_seconds: int = 2,
):
    """Start continuous auto-conversion similar to watch_and_convert.py behavior."""
    if bg_control.get("auto_running"):
        return

    bg_control["stop"] = False
    bg_control["auto_stop"] = False
    bg_control["auto_running"] = True
    bg_control["in_flight_names"] = []
    bg_queued_since.clear()

    def _log(msg: str):
        with bg_lock:
            bg_logs.append(msg)
        log_to_file(msg)

    def auto_orchestrator():
        _log(
            f"🤖 Auto convert enabled (poll every {poll_interval}s, workers={max_workers})"
        )
        try:
            in_flight = {}
            next_scan_at = 0.0

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                while not bg_control.get("auto_stop"):
                    # Harvest finished jobs first so slots become available immediately.
                    done_futures = [f for f in list(in_flight.keys()) if f.done()]
                    for future in done_futures:
                        name = in_flight.pop(future)
                        try:
                            future.result()
                        except Exception as e:
                            _log(f"❌ Auto worker exception for {name}: {e}")

                    # Reconcile stale statuses every loop: only true in-flight jobs should
                    # remain in waiting/running during auto mode.
                    in_flight_names = set(in_flight.values())
                    bg_control["in_flight_names"] = sorted(in_flight_names)
                    with bg_lock:
                        for name, cur_status in list(bg_statuses.items()):
                            if name in in_flight_names:
                                continue
                            if cur_status in ("running", "waiting", "starting"):
                                base_name = dataset_base_name(name)
                                out_file = os.path.join(out or src, base_name + ".mzML")
                                if os.path.exists(out_file) and is_valid_mzml(out_file, validate_interval=0):
                                    bg_statuses[name] = "done"
                                else:
                                    bg_statuses[name] = "queued"

                    now = time.time()
                    if now >= next_scan_at and not bg_control.get("stop"):
                        try:
                            all_dirs = [
                                d
                                for d in os.listdir(src)
                                if is_dataset_dir_name(d)
                                and os.path.isdir(os.path.join(src, d))
                                and not is_blank_sample(d)
                            ]
                        except Exception as e:
                            _log(f"⚠️ Auto scan failed: {e}")
                            all_dirs = []

                        in_flight_names = set(in_flight.values())
                        candidates = []

                        for name in sorted(all_dirs):
                            full = os.path.join(src, name)
                            base_name = dataset_base_name(name)
                            out_file = os.path.join(out or src, base_name + ".mzML")

                            if not has_required_files(full, size_check_seconds=0):
                                continue

                            # Skip already valid conversions.
                            if os.path.exists(out_file) and is_valid_mzml(out_file, validate_interval=0):
                                with bg_lock:
                                    if bg_statuses.get(name) not in ("running", "queued", "waiting"):
                                        bg_statuses[name] = "done"
                                continue

                            if name in in_flight_names:
                                continue

                            candidates.append(name)

                        slots = max(0, int(max_workers) - len(in_flight))
                        if candidates:
                            with bg_lock:
                                newly_queued = []
                                for name in candidates:
                                    if name not in bg_queued_since:
                                        bg_queued_since[name] = now
                                        newly_queued.append(name)
                                    if bg_statuses.get(name) not in ("running", "waiting", "starting"):
                                        bg_statuses[name] = "queued"
                                if newly_queued:
                                    msg = f"🧭 Auto detected {len(newly_queued)} dataset(s): {', '.join(newly_queued)}"
                                    bg_logs.append(msg)
                                    log_to_file(msg)

                            ready_candidates = []
                            with bg_lock:
                                for name in candidates:
                                    queued_at = bg_queued_since.get(name, now)
                                    if (now - queued_at) >= max(0, int(queue_grace_seconds)):
                                        ready_candidates.append(name)

                            if ready_candidates and slots > 0:
                                to_submit = ready_candidates[:slots]
                                for name in to_submit:
                                    with bg_lock:
                                        bg_queued_since.pop(name, None)
                                    future = executor.submit(
                                        convert_single_dataset,
                                        name,
                                        src,
                                        out,
                                        docker_image,
                                        stability_check_interval,
                                        stability_checks,
                                    )
                                    in_flight[future] = name

                        next_scan_at = now + max(1, int(poll_interval))

                    # Keep stop actions responsive.
                    time.sleep(1)

                # Stop requested: cancel queued futures that have not started.
                for future in list(in_flight.keys()):
                    future.cancel()
        finally:
            bg_control["auto_running"] = False
            bg_control["in_flight_names"] = []
            bg_queued_since.clear()
            _log("🛑 Auto convert stopped")

    t = threading.Thread(target=auto_orchestrator, daemon=True)
    t.start()



# Merge background updates into session state (runs on every rerun)

with bg_lock:
    for k, v in bg_statuses.items():
        st.session_state.statuses[k] = v
    for k, v in bg_progress.items():
        st.session_state.progress[k] = v
    if bg_logs:
        st.session_state.logs.extend(bg_logs)
        bg_logs.clear()
    
    # Store expected sizes and paths in session state for progress calculation
    if "expected_sizes" not in st.session_state:
        st.session_state.expected_sizes = {}
    if "output_paths" not in st.session_state:
        st.session_state.output_paths = {}
    for k, v in bg_expected_sizes.items():
        st.session_state.expected_sizes[k] = v
    for k, v in bg_output_paths.items():
        st.session_state.output_paths[k] = v
    
    # Store errors in session state
    if "errors" not in st.session_state:
        st.session_state.errors = {}
    for k, v in bg_errors.items():
        st.session_state.errors[k] = v

# Update progress for running conversions based on current file sizes (live update on each refresh)
for name, status in st.session_state.statuses.items():
    if status == "running":
        expected = st.session_state.expected_sizes.get(name, 0)
        out_path = st.session_state.output_paths.get(name, "")
        if expected > 0 and out_path:
            current_size = get_file_size(out_path)
            pct = min(99, int((current_size / expected) * 100))
            st.session_state.progress[name] = pct



# UI Layout
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("# 🧪 mzML Converter")
st.caption("Convert Bruker `.d` folders to mzML format using Docker.")

# Check Docker status
docker_ok, docker_msg = is_docker_running()
if docker_ok:
    st.success(f"🐳 {docker_msg}")
else:
    st.error(f"🐳 {docker_msg}")


# Settings Section
with st.expander("⚙️ Settings", expanded=True):
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Source Directory**")
        
        # Initialize text input key if not set
        if "src_input" not in st.session_state:
            st.session_state["src_input"] = st.session_state.src_dir
        
        # Handle Up button first
        nav_col, up_col = st.columns([3, 1])
        with up_col:
            if st.button("⬆️ Up", key="src_up", use_container_width=True):
                parent = os.path.dirname(st.session_state.src_dir)
                if parent and parent != st.session_state.src_dir:
                    st.session_state.src_dir = parent
                    st.session_state["src_input"] = parent
                    st.rerun()
        with nav_col:
            subdirs = list_subdirs(st.session_state.src_dir)
            if subdirs:
                nav = st.selectbox("Navigate into", ["—"] + subdirs, key="src_nav", index=0)
                if nav != "—":
                    new_path = os.path.join(st.session_state.src_dir, nav)
                    st.session_state.src_dir = new_path
                    st.session_state["src_input"] = new_path
                    del st.session_state["src_nav"]
                    st.rerun()
        
        # Text input for manual path entry (no value= param, uses session state key)
        src_path = st.text_input("Source path", key="src_input", label_visibility="collapsed")
        if os.path.isdir(src_path) and src_path != st.session_state.src_dir:
            st.session_state.src_dir = src_path
        st.caption(f"📂 {st.session_state.src_dir}")

    with col2:
        st.markdown("**Output Directory**")
        
        # Initialize text input key if not set
        if "out_input" not in st.session_state:
            st.session_state["out_input"] = st.session_state.out_dir
        
        # Handle Up button first
        out_nav_col, out_up_col = st.columns([3, 1])
        with out_up_col:
            if st.button("⬆️ Up", key="out_up", use_container_width=True):
                parent = os.path.dirname(st.session_state.out_dir)
                if parent and parent != st.session_state.out_dir:
                    st.session_state.out_dir = parent
                    st.session_state["out_input"] = parent
                    st.rerun()
        with out_nav_col:
            out_subdirs = list_subdirs(st.session_state.out_dir)
            if out_subdirs:
                out_nav = st.selectbox("Navigate into", ["—"] + out_subdirs, key="out_nav", index=0)
                if out_nav != "—":
                    new_path = os.path.join(st.session_state.out_dir, out_nav)
                    st.session_state.out_dir = new_path
                    st.session_state["out_input"] = new_path
                    del st.session_state["out_nav"]
                    st.rerun()
        
        # Text input for manual path entry (no value= param, uses session state key)
        out_path = st.text_input("Output path", key="out_input", label_visibility="collapsed")
        if os.path.isdir(out_path) and out_path != st.session_state.out_dir:
            st.session_state.out_dir = out_path
        st.caption(f"📂 {st.session_state.out_dir}")

    st.divider()
    settings_col1, settings_col2 = st.columns(2)
    with settings_col1:
        docker_image = st.text_input(
            "Docker image",
            value=DEFAULT_DOCKER_IMAGE,
            disabled=True,
            help="Docker image is fixed for this app configuration.",
        )
    with settings_col2:
        max_workers = st.number_input("Parallel workers", min_value=1, max_value=8, value=1, help="Number of simultaneous conversions")



# Dataset Selection
st.markdown("### 📁 Available Datasets")
ds = list_d_folders(st.session_state.src_dir)
if not ds:
    st.warning("No `.d` folders found in the source directory.")
else:
    # Selection buttons
    btn_cols = st.columns([1, 1, 1, 4])
    with btn_cols[0]:
        if st.button("☑️ Select All", use_container_width=True):
            for name in ds:
                st.session_state[f"chk_{name}"] = True
            st.rerun()
    with btn_cols[1]:
        # Select only not-converted datasets
        not_converted = [name for name in ds if get_mzml_status(name, st.session_state.out_dir)[0] != "valid"]
        if st.button(f"☑️ Not Converted ({len(not_converted)})", use_container_width=True):
            # Clear all first, then select only not converted
            for name in ds:
                st.session_state[f"chk_{name}"] = name in not_converted
            st.rerun()
    with btn_cols[2]:
        if st.button("☐ Clear All", use_container_width=True):
            for name in ds:
                st.session_state[f"chk_{name}"] = False
            st.rerun()

    st.divider()

    selected = []
    for name in ds:
        with st.container():
            key = f"chk_{name}"
            # Initialize checkbox state if not set
            if key not in st.session_state:
                st.session_state[key] = False

            # Get mzML status for this dataset
            mzml_code, mzml_text = get_mzml_status(name, st.session_state.out_dir)

            # Single row: checkbox, name, mzML status badge
            col_chk, col_name, col_mzml = st.columns([0.03, 0.77, 0.2])
            with col_chk:
                checked = st.checkbox("sel", key=key, label_visibility="collapsed")
            with col_name:
                display_name = normalized_dataset_name(name)
                st.write(display_name)
                if display_name != name:
                    st.caption("⚠️ Folder name has trailing whitespace")
            with col_mzml:
                # Keep a placeholder so row heights remain stable.
                if mzml_code == "valid":
                    st.success(mzml_text.replace("✅ ", ""), icon="✅")
                elif mzml_code == "invalid":
                    st.warning(mzml_text.replace("⚠️ ", ""), icon="⚠️")
                else:
                    st.empty()

            # Conversion status row (only if there's an active/completed conversion)
            status = st.session_state.statuses.get(name)
            if bool(bg_control.get("auto_running")) and status in ("running", "starting", "waiting"):
                in_flight_names = set(bg_control.get("in_flight_names", []))
                if name not in in_flight_names:
                    status = "queued"
                    st.session_state.statuses[name] = "queued"
            if status:
                _, col_status, col_progress = st.columns([0.03, 0.3, 0.67])
                with col_status:
                    st.caption(status_badge(status))
                with col_progress:
                    if status == "running":
                        d_folder_path = os.path.join(st.session_state.src_dir, name)
                        base_name = dataset_base_name(name)
                        mzml_path = os.path.join(st.session_state.out_dir, base_name + ".mzML")

                        d_size = dir_size(d_folder_path)
                        expected_size = d_size * 0.87
                        current_size = get_file_size(mzml_path)

                        if expected_size > 0:
                            pct = min(99, int((current_size / expected_size) * 100))
                        else:
                            pct = 0
                        st.session_state.progress[name] = pct
                        st.progress(pct / 100, text=f"{pct}% ({current_size/(1024*1024):.0f}/{expected_size/(1024*1024):.0f} MB)")
                    elif status == "queued":
                        st.caption("⏳ In queue...")
                    elif status == "waiting":
                        st.caption("📊 Checking if copy is complete...")
                    elif status == "starting":
                        st.caption("🚀 Launching converter...")
                    elif status == "done":
                        st.progress(1.0, text="100%")
                    elif status.startswith("failed"):
                        error_msgs = st.session_state.errors.get(name, [])
                        if error_msgs:
                            with st.expander(f"🔍 Error details ({len(error_msgs)} lines)", expanded=False):
                                st.code("\n".join(error_msgs), language=None)
                        else:
                            st.caption("No error details captured")

            if checked:
                selected.append(name)

    st.markdown(f"**Selected:** {len(selected)} / {len(ds)}")



# Actions
st.markdown("### 🚀 Actions")

has_active = any(s in ("running", "queued", "waiting") for s in st.session_state.statuses.values())
auto_running = bool(bg_control.get("auto_running"))
action_col1, action_col2, action_col3, action_col4 = st.columns([1, 1, 1, 1])

if auto_running:
    st.info("🤖 Auto Convert is active. New eligible .d folders will be converted automatically.")

with action_col1:
    if st.button("▶️ Start Conversion", type="primary", use_container_width=True, disabled=has_active or auto_running or not docker_ok):
        if not docker_ok:
            st.error("Cannot start conversion: Docker is not running!")
        elif auto_running:
            st.warning("Auto Convert is running. Stop Auto Convert before manual start.")
        elif not selected:
            st.warning("Please select at least one dataset.")
        else:
            bg_control["stop"] = False
            for s in selected:
                st.session_state.statuses[s] = "queued"
                st.session_state.progress[s] = 0
            start_conversion(
                selected,
                st.session_state.src_dir,
                st.session_state.out_dir,
                docker_image,
                max_workers=max_workers,
            )
            st.rerun()

with action_col2:
    if not auto_running:
        if st.button("🤖 Start Auto Convert", use_container_width=True, disabled=not docker_ok):
            if not docker_ok:
                st.error("Cannot start Auto Convert: Docker is not running!")
            else:
                start_auto_conversion(
                    st.session_state.src_dir,
                    st.session_state.out_dir,
                    docker_image,
                    max_workers=max_workers,
                )
                st.toast("Auto Convert enabled", icon="🤖")
                st.rerun()
    else:
        if st.button("🛑 Stop Auto Convert", use_container_width=True):
            bg_control["auto_stop"] = True
            bg_control["stop"] = True
            st.toast("Stopping Auto Convert...", icon="🛑")
            st.rerun()

with action_col3:
    if st.button("⏹️ Stop Conversion", use_container_width=True, disabled=not has_active and not auto_running):
        bg_control["stop"] = True
        # Also stop auto mode, if active.
        bg_control["auto_stop"] = True
        st.toast("Stopping conversion after current file...", icon="⏹️")
        st.rerun()

with action_col4:
    if st.button("🔄 Reset Status", use_container_width=True, help="Clear all conversion statuses, progress bars, error messages, and caches. Use this to start fresh or retry failed conversions."):
        # Clear both session state and background stores
        bg_control["stop"] = True
        bg_control["auto_stop"] = True
        st.session_state.statuses = {}
        st.session_state.progress = {}
        st.session_state.errors = {}
        # Also clear background stores to prevent them from being copied back
        with bg_lock:
            bg_statuses.clear()
            bg_progress.clear()
            bg_errors.clear()
            bg_queued_since.clear()
            bg_expected_sizes.clear()
            bg_output_paths.clear()
        st.toast("Status reset!", icon="🔄")
        st.rerun()



# Conversion Logs
st.markdown("### 📋 Conversion Logs")

# Show refresh time and log file status (always visible)
_log_path = LOG_FILE
_log_exists = _log_path.exists()
st.caption(f"🕐 Last refresh: {datetime.now().strftime('%H:%M:%S')} | Log file: {'✅ exists' if _log_exists else '❌ not found'}")

# Tab for GUI logs vs file logs
log_tab1, log_tab2 = st.tabs(["📱 GUI Logs", "📄 conversion.log"])

with log_tab1:
    if st.session_state.logs:
        with st.expander(f"View logs ({len(st.session_state.logs)} lines)", expanded=has_active):
            log_col1, log_col2 = st.columns([4, 1])
            with log_col2:
                if st.button("🗑️ Clear Logs", use_container_width=True):
                    st.session_state.logs = []
                    st.rerun()
            # Show last 100 logs in reverse order (newest first)
            recent_logs = st.session_state.logs[-100:]
            st.code("\n".join(reversed(recent_logs)), language=None)
    else:
        st.caption("No GUI logs yet. Start a conversion to see output here.")

with log_tab2:
    # Check file existence fresh each time
    log_file_path = LOG_FILE
    file_exists = log_file_path.exists()
    
    if file_exists:
        st.caption(f"📄 Reading from: `{log_file_path}`")
        try:
            # Read only tail lines to avoid loading very large logs each rerun.
            lines = read_last_lines(log_file_path, max_lines=100)
            
            st.caption(f"{len(lines)} log lines")
            
            # Show newest first
            if lines:
                st.code("\n".join(reversed(lines)), language=None)
            else:
                st.info("Log file exists but is empty.")
        except Exception as e:
            st.error(f"Error reading log file: {e}")
    else:
        st.caption(f"No conversion.log found at `{log_file_path}`. Start a conversion to create it.")

st.divider()
st.caption("💡 This GUI uses `watch_and_convert.py` for conversion.")


# Auto-refresh 
# (should be at the end of the script to ensure all UI elements are defined before rerun)
log_recently_modified = False
try:
    log_mtime = LOG_FILE.stat().st_mtime
    log_recently_modified = (time.time() - log_mtime) < 10  # Modified in last 10 seconds
except Exception:
    pass

if has_active or auto_running or log_recently_modified:
    # Adaptive refresh lowers CPU usage while still keeping the UI responsive.
    if has_active:
        refresh_seconds = 1.0
    elif log_recently_modified:
        refresh_seconds = 2.0
    else:
        refresh_seconds = 3.0

    time.sleep(refresh_seconds)
    st.rerun()
