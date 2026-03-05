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
- Dependencies in `requirements.txt` (install via `pip install -r requirements.txt`)

## Quick Start

1. **Clone the repository**:
   ```bash
   git clone https://github.com/nborodin30/converter-d-to-mzml.git
   cd converter-d-to-mzml
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Make sure Docker is running** (start Docker Desktop)

4. **Launch the GUI**:
   ```bash
   streamlit run app.py
   ```

5. **In the browser**:
   - Navigate to your source directory containing `.d` folders
   - Select datasets to convert
   - Click "Start Conversion"
   - Monitor progress and logs in real-time

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

- `--dir`: Directory to watch (default: current directory)
- `--poll-interval`: Seconds between scans (default: 30)
- `--stability-checks`: Number of identical-size checks before triggering conversion (default: 2)
- `--out`: Output directory for mzML files (defaults to watch dir)
- `--docker-image`: Docker image to use (default: `mfreitas/tdf2mzml`)
- `--validate-interval`: Seconds to wait before validating mzML output (default: 0)
- `--log-file`: Log file path (default: `<watch_dir>/conversion.log`)
- `--log-level`: Logging level (`DEBUG|INFO|WARNING|ERROR|CRITICAL`)

## Notes

- A `.d` folder is considered ready when its total size is stable for the configured number of checks and it contains `analysis.tdf` or `analysis.tdf_bin`.
- Conversion is skipped if a valid `.mzML` already exists.
- Expected mzML size is ~87% of the `.d` folder size.

## Installing tdf2mzml

**Docker (recommended)**: No local install needed. The converter runs `tdf2mzml.py` inside the `mfreitas/tdf2mzml` image.

Official source: https://github.com/mafreitas/tdf2mzml
