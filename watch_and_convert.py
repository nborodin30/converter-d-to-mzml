#!/usr/bin/env python3
"""Watch current directory for fully-copied .d folders and convert to mzML.

Detection strategy:
- Look for directories ending with `.d` in the watch directory.
- Ensure required files exist (e.g., `analysis.tdf` or `analysis.tdf_bin`).
- Consider a directory "complete" when its total size is stable across N checks.

Conversion strategy:
- Prefer a local `tdf2mzml.py` on PATH if available.
- Otherwise attempt a Docker fallback using `mfreitas/tdf2mzml` image.

Usage: run in the directory to watch, or pass `--dir`.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
import time
from typing import Dict, Tuple


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

def has_required_files(path: str) -> bool:
    """Return True if the directory contains expected TDF files.

    Heuristic: look for `analysis.tdf` or `analysis.tdf_bin`. As a conservative
    fallback consider any regular file present in the directory as indication
    that the folder contains data and should be considered by the watcher.
    """
    candidates = ("analysis.tdf", "analysis.tdf_bin")
    for c in candidates:
        if os.path.exists(os.path.join(path, c)):
            return True

    # Fallback: any regular file inside the directory
    try:
        for entry in os.listdir(path):
            if os.path.isfile(os.path.join(path, entry)):
                return True
    except OSError:
        return False

    return False

def find_tdftools() -> Tuple[str, str]:
    """Return ('docker', image) if Docker is available, otherwise ('none','').

    This script uses Docker exclusively for conversion.
    """
    if shutil.which("docker"):
        return ("docker", "mfreitas/tdf2mzml")
    return ("none", "")


def run_conversion(path: str, out_dir: str, docker_image: str = "mfreitas/tdf2mzml", dry_run: bool = False) -> tuple[int, str]:
    """Run conversion using Docker image `docker_image`.

    Mounts the parent directory to `/data` inside the container and runs
    `tdf2mzml.py -i /data/<dir> -o /data/<basename>.mzML`.
    """
    tool_type, _ = find_tdftools()
    if tool_type != "docker":
        logging.error("Docker is not available; cannot convert.")
        return 2, ""

    base_name = os.path.basename(os.path.normpath(path))
    # strip trailing .d from directory name for output filename
    root, ext = os.path.splitext(base_name)
    if ext.lower() == ".d":
        base_name = root
    out_name = os.path.join(out_dir, base_name + ".mzML")

    if os.path.exists(out_name):
        logging.info("Skipping conversion; output exists: %s", out_name)
        return 0, out_name

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
    logging.info("Running Docker: %s", " ".join(cmd))
    # Stream output in real-time so progress can be logged
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1) as proc:
        try:
            if proc.stdout:
                for line in proc.stdout:
                    logging.info(line.rstrip())
        except Exception:
            logging.exception("Error reading subprocess output for %s", path)
        rc = proc.wait()
    logging.info("Docker exited with rc=%s", rc)
    # return rc and the expected output path on the host
    return rc, out_name


def expected_output_for_dir(dirpath: str, out_dir: str) -> str:
    base_name = os.path.basename(os.path.normpath(dirpath))
    root, ext = os.path.splitext(base_name)
    if ext.lower() == ".d":
        base_name = root
    return os.path.join(out_dir, base_name + ".mzML")


def watch_directory(
    watch_dir: str,
    poll_interval: int = 30,
    stability_checks: int = 2,
    out_dir: str | None = None,
    dry_run: bool = False,
    docker_image: str = "mfreitas/tdf2mzml",
):
    out_dir = out_dir or watch_dir
    os.makedirs(out_dir, exist_ok=True)

    known_processing = set()

    logging.info("Watching %s every %ss, stability=%s", watch_dir, poll_interval, stability_checks)

    # initial snapshot: list detected .d dirs and their states
    all_dirs = [d for d in os.listdir(watch_dir) if d.endswith(".d") and os.path.isdir(os.path.join(watch_dir, d))]
    pending = []
    done = []
    for d in all_dirs:
        p = os.path.join(watch_dir, d)
        expected = expected_output_for_dir(p, out_dir)
        # consider directory done if expected mzML exists; otherwise if it has data, it's pending
        if os.path.exists(expected):
            done.append(d)
        elif has_required_files(p):
            pending.append(d)

    logging.info("Startup snapshot: total=%d pending=%d done=%d", len(all_dirs), len(pending), len(done))
    if pending:
        logging.info("Pending: %s", ", ".join(pending))
    if done:
        logging.info("Done: %s", ", ".join(done))

    sizes: Dict[str, Tuple[int, int]] = {}

    while True:
        all_dirs = [d for d in os.listdir(watch_dir) if d.endswith(".d") and os.path.isdir(os.path.join(watch_dir, d))]
        # compute queue stats: done based on presence of mzML, in-progress tracked in-memory
        done_count = 0
        for d in all_dirs:
            p = os.path.join(watch_dir, d)
            expected = expected_output_for_dir(p, out_dir)
            if os.path.exists(expected):
                done_count += 1
        in_progress_count = len(known_processing)

        # candidate directories that look ready (have required files and not already done/in-progress)
        cand = []
        for d in all_dirs:
            p = os.path.join(watch_dir, d)
            expected = expected_output_for_dir(p, out_dir)
            if os.path.exists(expected):
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
                logging.info("Starting conversion for %s", full)
                try:
                    rc, expected_out = run_conversion(full, out_dir, docker_image=docker_image, dry_run=dry_run)
                except Exception:
                    logging.exception("Conversion raised exception for %s", full)
                    rc, expected_out = 99, ""

                if rc == 0:
                    if expected_out and os.path.exists(expected_out):
                        logging.info("Conversion succeeded for %s; output: %s", full, expected_out)
                    else:
                        logging.error("Conversion reported rc=0 but output missing for %s (expected %s)", full, expected_out)
                        # treat as failure so it can be retried
                        known_processing.discard(full)
                        sizes.pop(full, None)
                        
                else:
                    logging.error("Conversion failed (rc=%s) for %s", rc, full)
                    # allow re-try later
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
    p.add_argument("--log-file", default=None, help="Path to logfile (appends). If omitted, logs go to stderr")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging level")
    p.add_argument("--dry-run", action="store_true", help="Don't run conversion, only report candidates")
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
        )
    except KeyboardInterrupt:
        logging.info("Exiting on user interrupt")


if __name__ == "__main__":
    main()
