
# MasPhD

This repository contains the research codebase for my PhD work on train delay prediction and ensemble modelling.  
It is designed to support experimentation, visualisation, and reproducible modelling using a clear and modular project structure.

The project follows modern Python best practices and uses a `src/` layout to separate experimental work from final, reusable code.

---

## Project Structure

```

MasPhD/
├─ README.md
├─ pyproject.toml
├─ requirements.txt
├─ .gitignore
├─ .env.example
├─ configs/
├─ data/
├─ notebooks/
├─ reports/
├─ src/
├─ scripts/
└─ tests/

```

### Folder Overview

- **`notebooks/`**  
  Jupyter notebooks used for testing ideas, exploration, and visualisation.

- **`src/masphd/`**  
  The main Python package containing final, reusable code such as utilities, feature engineering, models, and visualisation functions.

- **`scripts/`**  
  Runnable Python scripts for tasks such as training models, preprocessing data, and running experiments.

- **`tests/`**  
  Unit tests for validating core functionality.

- **`data/`**  
  Project datasets organised by processing stage:
  - `raw/` – original, immutable data  
  - `interim/` – intermediate processed data  
  - `processed/` – final datasets ready for modelling  

- **`reports/`**  
  Exported figures and tables used in papers and thesis chapters.

---

## Configuration and Secrets

Configuration files are separated from source code to improve clarity and security.

### `configs/`
- `config.yaml`  
  Non-sensitive configuration such as paths, parameters, and experiment settings.  
  This file **can be committed to Git**.

- `secrets.env`  
  Contains sensitive values such as passwords or API keys.  
  This file **must not be committed**.

### Environment Variables
Sensitive values are loaded via environment variables.

An example template is provided:

```

.env.example

```

You should create your own local version (for example `configs/secrets.env`) and define values such as:

```

DARWIN_TOPIC_USERNAME=your_username_here
DARWIN_TOPIC_PASSWORD=your_password_here

````

These files are excluded via `.gitignore`.

---

## Installation and Setup

### 1️⃣ Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
````

### 2️⃣ Install the project in editable mode

```bash
pip install -e .[dev]
or
pip install -e ".[dev]"

```

This installs the project and development tools and allows you to import the `masphd` package from notebooks, scripts, and tests.

---

## Usage

After installation, modules can be imported as:

```python
from masphd.utils import helpers
from masphd.models import train_model
```

Scripts can be run from the project root:

```bash
python scripts/train.py
```

Jupyter notebooks in the `notebooks/` directory will automatically recognise the `masphd` package.

---

## Running Real-Time Prediction (Darwin Stream)

The real-time predictor listens to the Darwin push feed, extracts station-to-station segments, builds features, runs the ensemble model, and stores predictions in the SQLite database.

### Run from terminal

From the project root:

```bash
python scripts/run_realtime_predict.py
```

### Common options

```bash
# Run for 10 minutes
python scripts/run_realtime_predict.py --minutes 10

# Run continuously (Ctrl+C to stop)
python scripts/run_realtime_predict.py --minutes -1

# Disable terminal output (recommended for long runs)
python scripts/run_realtime_predict.py --minutes -1 --no-print

# Custom cache size and weights file
python scripts/run_realtime_predict.py --minutes 60 --cache-size 1000 --weights model_weights.json
```

All predictions are written to the `predictions_all` and `predictions_actual` tables in the configured SQLite database.

---

## Enriching Predictions with Actual Arrival Times (HSP)

The HSP enrichment script post-processes stored real-time predictions by querying the HSP API and computing **actual arrival delays**.

Only **new, previously unprocessed predictions** are handled on each run.

### Run from terminal

```bash
python scripts/enrich_hsp_actuals.py
```

### Optional arguments

```bash
# print log
python scripts/enrich_hsp_actuals.py --verbose  

# Process only predictions before a given date
python scripts/enrich_hsp_actuals.py --before-date 2026-02-04

# Limit number of processed rows (useful for testing)
python scripts/enrich_hsp_actuals.py --limit-rows 100
```

Results are upserted into the `actual_arrivals_hsp` table, including:

* actual arrival delay
* main-journey flag
* full list of HSP locations (CRS sequence)

---

## Running Scripts in the Background (nohup)

For long-running jobs on servers, use `nohup` so the process continues after logout.

### Real-time predictor (recommended)

```bash
nohup python scripts/run_realtime_predict.py \
  --minutes -1 \
  --no-print \
  > realtime.log 2>&1 &
```

### HSP enrichment (daily or ad-hoc)

```bash
nohup python scripts/enrich_hsp_actuals.py \
  > enrich_hsp.log 2>&1 &
```

To stop a running process:

```bash
ps aux | grep run_realtime_predict
kill <PID>
```

---

## Notes

* Secrets and passwords must never be hard-coded or committed.
* Experimental work should remain in notebooks; reusable logic should be moved into `src/masphd/`.
* Outputs such as figures and tables should be saved to `reports/` for reproducibility.

---

## Author

Mas Golchehreh (PhD Researcher in Artificial Intelligence)

