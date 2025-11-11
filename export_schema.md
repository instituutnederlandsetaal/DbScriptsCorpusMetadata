# pg_schema_export

Export PostgreSQL schema objects into one file-per-object suitable for version control.

This script produces per-object SQL files (tables, functions, types, triggers, sequences, extensions, views) 
and optionally generates an apply script and a git commit. 
It aims to keep extension-created objects separate (extension stubs only) and to create readable, 
UTF-8 encoded SQL files ready for VCS.

---

## Quick summary

- Language: Python 3.8+  
- Requirements: `psycopg2` (or `psycopg2-binary`), `pg_dump` on PATH, `git` on PATH (optional)  
- Main script: `export_schema.py`  
- Default output folder: `pg_schema_export/` (or use `--outdir`)  
- Generates per-object SQL files and optionally `00_apply.sh` + `Makefile` and initial git commit

---

## What it exports (one file per object)

- `extensions/` — small `CREATE EXTENSION IF NOT EXISTS ...` stubs  
- `types/` — enums and composite types (one file each)  
- `sequences/` — per-sequence DDL (via `pg_dump -t`)  
- `tables/` — per-table DDL (via `pg_dump -t`, includes indexes and table-level constraints)  
- `views/` — per-view `CREATE OR REPLACE VIEW` files  
- `functions/` — user-defined functions (one file each; extension-owned functions are excluded)  
- `triggers/` — `CREATE TRIGGER ...` statements, one file each  
- `pg_dump_errors/` — any `pg_dump` stderr for failing dumps

---

## Files the script creates

- `pg_schema_export/` (default; or `--outdir <dir>`)  
  - `extensions/`, `types/`, `sequences/`, `tables/`, `views/`, `functions/`, `triggers/`  
  - `pg_dump_errors/` (if any)  
  - `00_apply.sh` (optional) — a small shell script that runs SQL files in a logical order  
  - `Makefile` (optional) — runs `./00_apply.sh` for `make apply`

---

## Installation

1. Use Python 3.8+ (recommended latest 3.x).
2. Create a virtualenv (optional but recommended):

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Unix/macOS
source .venv/bin/activate

--host                 Database host (overrides DB_CONFIG)
--port                 Database port
--user                 Database user
--password             Database password (not recommended on CLI)
--dbname               Database name (required if no DB_CONFIG)
--schemas              Comma-separated list of schemas to export (default: all non-system schemas)
--outdir               Output directory (default: pg_schema_export)
--include-owner        Include ownership statements in dumps (do not add --no-owner to pg_dump)
--include-privileges   Include GRANT/privileges (do not add --no-privileges to pg_dump)
--pretty-func-names    Include argument signature in function filenames (more readable)
--generate-makefile    Generate 00_apply.sh and Makefile in output dir
--init-git             Initialise a git repo in outdir and create an initial commit
--verbose              Enable debug logging (prints pg_dump commands, etc.)
