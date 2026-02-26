# TDF → mzML Converter

Convert Bruker timsTOF `.d` folders to mzML format using Docker.

## Features

- **Streamlit GUI** (`app.py`) - Interactive web interface for selecting and converting files
- **CLI Watcher** (`watch_and_convert.py`) - Background watcher that auto-converts new files
- Blank sample filtering (excludes folders matching `Blank-01`, `Blank_2`, etc.)
- Real-time progress tracking and conversion logs
- Validation of output mzML files

## Requirements

- Python 3.9+
- Docker installed and running
- Streamlit (`pip install streamlit`)

## GUI Usage

Start the web interface:

```bash
streamlit run app.py
```

Features:
- Browse and select source/output directories
- Select multiple `.d` folders for conversion
- Real-time progress bars and status updates
- View conversion logs (GUI logs + `conversion.log` file)
- Filter out blank samples automatically

## CLI Watcher Usage

Run in the directory you want to watch:

```bash
python ./watch_and_convert.py
```

Or specify a directory:

```bash
python ./watch_and_convert.py --dir /path/to/watch
```

### CLI Options

- `--poll-interval`: Seconds between scans (default: 30)
- `--stability-checks`: Number of identical-size checks before triggering conversion (default: 2)
- `--out`: Output directory for mzML files (defaults to watch dir)
- `--docker-image`: Docker image to use (default: `mfreitas/tdf2mzml`)
- `--log-file`: Log file path (default: `<watch_dir>/conversion.log`)
- `--log-level`: Logging level (`DEBUG|INFO|WARNING|ERROR|CRITICAL`)

## Notes

- A `.d` folder is considered ready when its total size is stable for the configured number of checks and it contains `analysis.tdf` or `analysis.tdf_bin`.
- Conversion is skipped if a valid `.mzML` already exists.
- Expected mzML size is ~62% of the `.d` folder size.

## Installing tdf2mzml

**Docker (recommended)**: No local install needed. The converter runs `tdf2mzml.py` inside the `mfreitas/tdf2mzml` image.

Official source: https://github.com/mafreitas/tdf2mzml
