#!/usr/bin/env python3
"""Watch current directory for fully-copied .d folders and convert to mzML.

Detection strategy:
- Look for directories ending with `.d` in the watch directory.
- Ensure required files exist (e.g., `analysis.tdf` or `analysis.tdf_bin`).
- Consider a directory "complete" when its total size is stable across N checks.

Conversion strategy:
- Uses Docker with `mfreitas/tdf2mzml` image for conversion.

Usage: run in the directory to watch, or pass `--dir`.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, Callable

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

def is_blank_sample(name: str) -> bool:
    """Check if a folder name represents a blank sample."""
    return bool(BLANK_PATTERN.search(name))

def dir_size(path: str) -> int:
    total = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            try:
                fp = os.path.join(root, f)
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total

def has_required_files(path: str, size_check_seconds: int = 1) -> bool:
    """Return True if the directory contains expected TDF files.

    Heuristic: look for `analysis.tdf` or `analysis.tdf_bin`. As a conservative
    fallback consider any regular file present in the directory as indication
    that the folder contains data and should be considered by the watcher.
    """
    candidates = ("analysis.tdf", "analysis.tdf_bin")
    for c in candidates:
        if os.path.exists(os.path.join(path, c)):
            return True

    # Optional quick size-stability check (1s by default)
    if size_check_seconds and size_check_seconds > 0:
        try:
            first_size = dir_size(path)
            time.sleep(size_check_seconds)
            second_size = dir_size(path)
            logging.debug("Quick size check for %s: %d -> %d", path, first_size, second_size)
            if first_size == second_size and first_size > 0:
                logging.info("Quick size-stability check passed for %s", path)
                return True
        except Exception:
            logging.exception("Quick size-stability check failed for %s", path)

    # Fallback: any regular file inside the directory
    try:
        for entry in os.listdir(path):
            if os.path.isfile(os.path.join(path, entry)):
                logging.debug("Found regular file in %s; treating as ready", path)
                return True
    except OSError:
        return False

    return False


def is_valid_mzml(path: str, validate_interval: int = 1) -> bool:
    """Check if an mzML file is complete by verifying it ends with </mzML> or </indexedmzML>.
    
    Args:
        path: Path to the mzML file
        validate_interval: Optional wait time to ensure file is not being written
        
    Returns:
        True if the file exists and contains a valid closing tag
    """
    if not os.path.exists(path):
        return False
    
    if validate_interval > 0:
        time.sleep(validate_interval)
    
    try:
        # Read only the last 1KB to check for closing tag
        with open(path, "rb") as f:
            f.seek(0, 2)  # Seek to end
            file_size = f.tell()
            read_size = min(1024, file_size)
            f.seek(-read_size, 2)
            tail = f.read().decode("utf-8", errors="ignore")
        
        # Check for both regular mzML and indexed mzML formats
        return "</mzML>" in tail or "</indexedmzML>" in tail
    except Exception as e:
        logging.warning("Failed to validate mzML %s: %s", path, e)
        return False


def find_tdftools() -> Tuple[str, str]:
    """Return ('docker', image) if Docker is available, otherwise ('none','').

    This script uses Docker exclusively for conversion.
    """
    if shutil.which("docker"):
        return ("docker", "mfreitas/tdf2mzml")
    return ("none", "")


def _safe_line_callback(
    callback: Callable[[str], None] | None,
    message: str,
) -> None:
    """Call line callback while isolating callback failures."""
    if callback is None:
        return
    try:
        callback(message)
    except Exception:
        pass


def run_conversion(
    path: str,
    out_dir: str,
    docker_image: str = "mfreitas/tdf2mzml",
    dry_run: bool = False,
    line_callback: Callable[[str], None] | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> tuple[int, str]:
    """Run conversion using Docker image `docker_image`.

    Mounts the parent directory to `/data` inside the container and runs
    `tdf2mzml.py -i /data/<dir> -o /data/<basename>.mzML`.
    
    If line_callback is provided, it will be called with each stdout line.
    If should_stop is provided and returns True, the running Docker process is
    terminated and the function returns rc=130.
    """
    tool_type, _ = find_tdftools()
    if tool_type != "docker":
        logging.error("Docker is not available; cannot convert.")
        _safe_line_callback(
            line_callback,
            "❌ Docker is not available. Please start Docker Desktop.",
        )
        return 2, ""

    base_name = dataset_base_name(os.path.basename(os.path.normpath(path)))
    out_name = os.path.join(out_dir, base_name + ".mzML")

    if os.path.exists(out_name):
        # Check if existing mzML is valid
        if is_valid_mzml(out_name, validate_interval=0):
            logging.info("Skipping conversion; valid output exists: %s", out_name)
            _safe_line_callback(
                line_callback,
                f"⏭️ Skipping - valid output already exists: {out_name}",
            )
            return 0, out_name
        else:
            # Remove invalid/incomplete mzML to allow re-conversion
            logging.warning("Removing incomplete mzML before re-conversion: %s", out_name)
            _safe_line_callback(
                line_callback,
                f"🗑️ Removing incomplete mzML: {out_name}",
            )
            try:
                os.remove(out_name)
                _safe_line_callback(line_callback, "   ✓ Removed successfully")
            except OSError as e:
                logging.error("Failed to remove incomplete mzML %s: %s", out_name, e)
                _safe_line_callback(line_callback, f"❌ Failed to remove: {e}")
                return 3, ""

    if dry_run:
        logging.info("[dry-run] Would convert %s -> %s using Docker image %s", path, out_name, docker_image)
        return 0, out_name

    abs_path = os.path.abspath(path)
    parent = os.path.dirname(abs_path)
    container_path = os.path.join("/data", os.path.basename(abs_path))
    container_out = os.path.join("/data", os.path.basename(out_name))

    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{parent}:/data",
        docker_image,
        "tdf2mzml.py",
        "-i",
        container_path,
        "-o",
        container_out,
    ]
    cmd_str = " ".join(cmd)
    logging.info("Running Docker: %s", cmd_str)
    
    # Log the command to callback
    _safe_line_callback(
        line_callback,
        (
            "🐳 Docker command: docker run --rm -v "
            f"{parent}:/data {docker_image} tdf2mzml.py -i {container_path} -o {container_out}"
        ),
    )
    
    # Stream output in real-time so progress can be logged
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1) as proc:
        stop_triggered = False

        def _watch_stop() -> None:
            nonlocal stop_triggered
            while proc.poll() is None:
                if should_stop and should_stop():
                    stop_triggered = True
                    logging.warning("Stop requested; terminating Docker process for %s", path)
                    _safe_line_callback(
                        line_callback,
                        "⏹️ Stop requested. Terminating running Docker container...",
                    )
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                    return
                time.sleep(0.2)

        stop_thread = None
        if should_stop is not None:
            stop_thread = threading.Thread(target=_watch_stop, daemon=True)
            stop_thread.start()

        try:
            if proc.stdout:
                for line in proc.stdout:
                    if stop_triggered:
                        return 130, ""
                    text = line.rstrip()
                    logging.info(text)
                    _safe_line_callback(line_callback, text)
        except Exception as e:
            logging.exception("Error reading subprocess output for %s", path)
            _safe_line_callback(line_callback, f"❌ Error reading Docker output: {e}")
        rc = proc.wait()
        if stop_thread is not None and stop_thread.is_alive():
            stop_thread.join(timeout=0.5)
        if stop_triggered:
            return 130, ""
    logging.info("Docker exited with rc=%s", rc)
    if rc == 0:
        _safe_line_callback(line_callback, f"✅ Docker finished successfully (exit code: {rc})")
    else:
        _safe_line_callback(line_callback, f"❌ Docker exited with error (exit code: {rc})")
    # return rc and the expected output path on the host
    return rc, out_name


def expected_output_for_dir(dirpath: str, out_dir: str) -> str:
    base_name = dataset_base_name(os.path.basename(os.path.normpath(dirpath)))
    return os.path.join(out_dir, base_name + ".mzML")


def watch_directory(
    watch_dir: str,
    poll_interval: int = 30,
    stability_checks: int = 2,
    out_dir: str | None = None,
    dry_run: bool = False,
    docker_image: str = "mfreitas/tdf2mzml",
    validate_interval: int = 0,
    max_workers: int = 1,
):
    out_dir = out_dir or watch_dir
    os.makedirs(out_dir, exist_ok=True)

    known_processing = set()

    logging.info("Watching %s every %ss, stability=%s, max_workers=%d", watch_dir, poll_interval, stability_checks, max_workers)

    # initial snapshot: list detected .d dirs and their states (excluding blanks)
    all_dirs = [d for d in os.listdir(watch_dir) if is_dataset_dir_name(d) and os.path.isdir(os.path.join(watch_dir, d)) and not is_blank_sample(d)]
    pending = []
    done = []
    incomplete = []
    for d in all_dirs:
        p = os.path.join(watch_dir, d)
        expected = expected_output_for_dir(p, out_dir)
        # consider directory done only if expected mzML exists AND is valid
        if os.path.exists(expected):
            if is_valid_mzml(expected, validate_interval=validate_interval):
                done.append(d)
            else:
                incomplete.append(d)
                pending.append(d)
        elif has_required_files(p):
            pending.append(d)

    logging.info("Startup snapshot: total=%d pending=%d done=%d incomplete=%d", len(all_dirs), len(pending), len(done), len(incomplete))
    if pending:
        logging.info("Pending: %s", ", ".join(pending))
    if done:
        logging.info("Done: %s", ", ".join(done))

    sizes: Dict[str, Tuple[int, int]] = {}

    while True:
        all_dirs = [d for d in os.listdir(watch_dir) if is_dataset_dir_name(d) and os.path.isdir(os.path.join(watch_dir, d)) and not is_blank_sample(d)]
        # compute queue stats: done based on VALID mzML, in-progress tracked in-memory
        done_count = 0
        for d in all_dirs:
            p = os.path.join(watch_dir, d)
            expected = expected_output_for_dir(p, out_dir)
            if os.path.exists(expected) and is_valid_mzml(expected, validate_interval=validate_interval):
                done_count += 1
        in_progress_count = len(known_processing)

        # candidate directories that look ready (have required files and not already done/in-progress)
        cand = []
        for d in all_dirs:
            p = os.path.join(watch_dir, d)
            expected = expected_output_for_dir(p, out_dir)
            # Only skip if mzML exists AND is valid
            if os.path.exists(expected) and is_valid_mzml(expected, validate_interval=validate_interval):
                continue
            if p in known_processing:
                continue
            if has_required_files(p):
                cand.append(d)

        pending_count = len(cand)
        logging.info("Status: total=%d pending=%d in_progress=%d done=%d", len(all_dirs), pending_count, in_progress_count, done_count)
        for idx, name in enumerate(cand, start=1):
            full = os.path.join(watch_dir, name)
            if full in known_processing:
                continue

            try:
                cur_size = dir_size(full)
            except Exception as e:
                logging.exception("Error computing size for %s: %s", full, e)
                continue
            last_size, stable_count = sizes.get(full, (cur_size, 0))

            if cur_size == last_size:
                stable_count += 1
            else:
                stable_count = 0

            sizes[full] = (cur_size, stable_count)

            if stable_count >= stability_checks:
                logging.info("Detected stable directory: %s (size %d). Queue position: %d/%d", full, cur_size, idx, pending_count)
                # mark processing (in-memory only; do not rely on on-disk marker files)
                known_processing.add(full)

        # Collect directories ready for conversion
        ready_for_conversion = []
        for name in cand:
            full = os.path.join(watch_dir, name)
            if full in known_processing:
                last_size, stable_count = sizes.get(full, (0, 0))
                if stable_count >= stability_checks:
                    ready_for_conversion.append(full)

        # Process conversions in parallel using ThreadPoolExecutor
        if ready_for_conversion:
            logging.info("Starting parallel conversion of %d directories with %d workers", len(ready_for_conversion), max_workers)
            
            def convert_one(full_path: str) -> tuple[str, int, str]:
                """Convert a single directory. Returns (path, rc, output_path)."""
                logging.info("Starting conversion for %s", full_path)
                try:
                    rc, expected_out = run_conversion(full_path, out_dir, docker_image=docker_image, dry_run=dry_run)
                except Exception:
                    logging.exception("Conversion raised exception for %s", full_path)
                    rc, expected_out = 99, ""
                return full_path, rc, expected_out
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(convert_one, full): full for full in ready_for_conversion}
                
                for future in as_completed(futures):
                    full = futures[future]
                    try:
                        full_path, rc, expected_out = future.result()
                        
                        if rc == 0:
                            output_valid = (
                                bool(expected_out)
                                and os.path.exists(expected_out)
                                and is_valid_mzml(expected_out, validate_interval=validate_interval)
                            )
                            if output_valid:
                                logging.info("Conversion succeeded for %s; output: %s", full_path, expected_out)
                            else:
                                logging.error(
                                    "Conversion reported rc=0 but output is missing/invalid for %s (expected %s)",
                                    full_path,
                                    expected_out,
                                )
                                # treat as failure so it can be retried
                                sizes.pop(full_path, None)
                        else:
                            logging.error("Conversion failed (rc=%s) for %s", rc, full_path)
                            sizes.pop(full_path, None)
                        # Track only in-flight directories in memory.
                        known_processing.discard(full_path)
                    except Exception as e:
                        logging.exception("Worker exception for %s: %s", full, e)
                        known_processing.discard(full)
                        sizes.pop(full, None)

        time.sleep(poll_interval)


def parse_args():
    p = argparse.ArgumentParser(description="Watch for fully-copied .d folders and convert to mzML (Docker-only)")
    p.add_argument("--dir", default=".", help="Directory to watch")
    p.add_argument("--poll-interval", type=int, default=30, help="Seconds between scans")
    p.add_argument("--stability-checks", type=int, default=2, help="Number of identical-size checks before triggering conversion")
    p.add_argument("--out", default=None, help="Output directory for mzML files (defaults to watch dir)")
    p.add_argument("--docker-image", default="mfreitas/tdf2mzml", help="Docker image to use for tdf2mzml")
    p.add_argument("--max-workers", type=int, default=1, help="Number of parallel conversions (default: 1)")
    p.add_argument(
        "--log-file",
        default=None,
        help="Path to logfile (appends). Default: <watch_dir>/conversion.log",
    )
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging level")
    p.add_argument("--dry-run", action="store_true", help="Don't run conversion, only report candidates")
    p.add_argument("--validate-interval", type=int, default=0, help="Seconds to wait before validating mzML output (default: 0)")
    return p.parse_args()


def main():
    args = parse_args()
    # configure logging
    level = getattr(logging, args.log_level.upper(), logging.INFO)
    # default to a conversion.log inside the watched directory when not provided
    logfile = args.log_file if args.log_file else os.path.join(args.dir, "conversion.log")
    logging.basicConfig(level=level, filename=logfile, filemode="a", format="%(asctime)s %(levelname)s: %(message)s")
    logging.info("Logging to %s", logfile)

    try:
        watch_directory(
            args.dir,
            poll_interval=args.poll_interval,
            stability_checks=args.stability_checks,
            out_dir=args.out,
            dry_run=args.dry_run,
            docker_image=args.docker_image,
            validate_interval=args.validate_interval,
            max_workers=args.max_workers,
        )
    except KeyboardInterrupt:
        logging.info("Exiting on user interrupt")


if __name__ == "__main__":
    main()
