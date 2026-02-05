# watch_and_convert

Lightweight watcher that detects completed TIMS `.d` folders and converts them to mzML via `tdf2mzml.py` or Docker.

## Requirements

- Python 3.9+
- Docker installed and available on `PATH`
- `tdf2mzml.py` installed and available on `PATH` (or use the Docker image `mfreitas/tdf2mzml`)

## Usage

Run in the directory you want to watch:

```
./watch_and_convert.py
```

Or specify a directory:

```
./watch_and_convert.py --dir /path/to/watch
```

## Options

- `--poll-interval`: Seconds between scans (default: 30)
- `--stability-checks`: Number of identical-size checks before triggering conversion (default: 2)
- `--out`: Output directory for mzML files (defaults to watch dir)
- `--docker-image`: Docker image to use (default: `mfreitas/tdf2mzml`)
- `--log-file`: Log file path (default: `<watch_dir>/conversion.log`)
- `--log-level`: Logging level (`DEBUG|INFO|WARNING|ERROR|CRITICAL`)
- `--dry-run`: Report candidates without converting

## Notes

- A `.d` folder is considered ready when its total size is stable for the configured number of checks and it contains `analysis.tdf` or `analysis.tdf_bin` (or any regular file as a fallback).
- Conversion is skipped if the expected `.mzML` already exists.

## Installing tdf2mzml.py

Two options:

1) **Docker (recommended)**: no local install needed. The script runs `tdf2mzml.py` inside the `mfreitas/tdf2mzml` image.
2) **Local install**: obtain `tdf2mzml.py` from the official tdf2mzml source and place it on your `PATH` (for example, a `bin/` directory already on `PATH`).
