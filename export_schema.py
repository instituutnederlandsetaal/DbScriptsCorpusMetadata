#!/usr/bin/env python3
# ---------- High-level description ----------
# pg_schema_export.py: export Postgres schema objects into separate files suitable for version control.
# - Uses a mix of catalog queries + pg_dump -t to produce one file per object.
# - Designed to avoid dumping extension-owned objects -- we write extension stubs instead.
# - Writes textual files as UTF-8 (errors replaced) to avoid encoding crashes.
# - Optionally initialises a git repo and performs an initial commit.
#
# Requirements:
#  - Python 3.8+
#  - psycopg2 installed
#  - pg_dump available on PATH (Postgres client)
#  - Optional: database_config.DB_CONFIG module for connection parameters

from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Set, Optional

import psycopg2
from psycopg2 import sql

# --- Auto-commit settings ---
# GIT_CONFIG expected to contain dictionary keys:
#   AUTO_INIT_GIT (bool)            - whether to auto-init/commit when running with no CLI args
#   AUTO_GIT_USER_NAME (str)        - name to set for local git commits (only used when init-ing)
#   AUTO_GIT_USER_EMAIL (str)       - email to set for local git commits
#   AUTO_GIT_COMMIT_MESSAGE (str)   - default commit message used when auto-committing
#
# These live in `git_config.py` so we can edit them without modifying the main script.
from git_config import GIT_CONFIG

AUTO_INIT_GIT = GIT_CONFIG["AUTO_INIT_GIT"]
AUTO_GIT_USER_NAME = GIT_CONFIG["AUTO_GIT_USER_NAME"]
AUTO_GIT_USER_EMAIL = GIT_CONFIG["AUTO_GIT_USER_EMAIL"]
AUTO_GIT_COMMIT_MESSAGE = GIT_CONFIG["AUTO_GIT_COMMIT_MESSAGE"]

# --- Logging ---
# Logs both to console and to a file (export_schema.log) using UTF-8.
# Console handler writes to stdout so it's visible in PyCharm run console.
# File handler appends so repeated runs are recorded.
LOG_FILENAME = "export_schema.log"

logger = logging.getLogger("pg_schema_export")
logger.setLevel(logging.INFO)

# Formatter that matches your sample (YYYY-MM-DD HH:MM:SS)
fmt = logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# Console handler (stdout)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
logger.addHandler(ch)

# File handler (append mode)
fh = logging.FileHandler(LOG_FILENAME, mode="a", encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)

# Try to import DB_CONFIG if present
try:
    from database_config import DB_CONFIG
except KeyboardInterrupt:
    logger.error("Interrupted by user.")
    sys.exit(1)
except psycopg2.Error as e:
    logger.error("Database error: %s", e)
except UnicodeError as e:
    logger.error("Encoding error: %s", e)
except Exception as e:
    logger.error("Unexpected error: %s", e)


# ---------- Utilities ----------
def safe_filename(name: str) -> str:
    # Convert an object identifier like "schema.table.name" into a filesystem-friendly filename.
    # Keeps letters, numbers, dot, underscore, parentheses and dash; replaces other chars with '_'
    return re.sub(r"[^A-Za-z0-9_.()-]", "_", name)


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def run_pg_dump_and_write(pg_dump_conn_arg: str, args: List[str], out_path: Path, error_dir: Path) -> bool:
    # Uses pg_dump -s (schema-only) to dump a single object.
    #    - args: additional args like ['-t', 'schema.table']
    #    - Writes stdout to the target file (binary write), so encoding is preserved.
    #    - If pg_dump fails (non-zero exit), writes stderr to pg_dump_errors/<name>.err and returns False.
    #    - Important: pg_dump output is authoritative for table DDL (indexes, comments, defaults).
    cmd = ["pg_dump", "-s"] + args + [pg_dump_conn_arg]
    logger.debug("pg_dump: %s", " ".join(cmd))
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    except FileNotFoundError:
        raise RuntimeError("pg_dump not found on PATH.")
    if proc.returncode != 0:
        ensure_dir(error_dir)
        err_file = error_dir / (out_path.stem + ".err")
        err_file.write_text(proc.stderr.decode(errors="replace"))
        logger.warning("pg_dump failed for %s; stderr written to %s", args, err_file)
        return False
    out_path.write_bytes(proc.stdout)
    return True


def dsn_to_pg_dump_connarg(dsn: Dict[str, str]) -> str:
    # Converts a psycopg2-style dsn dict into a pg_dump --dbname argument, using a connection URI if host present.
    # Note: embedding passwords on the command line is not secure; prefer PGPASSWORD or .pgpass.
    dbname = dsn.get("dbname") or dsn.get("database")
    if not dbname:
        raise ValueError("dbname/database required")
    host = dsn.get("host")
    user = dsn.get("user")
    password = dsn.get("password")
    port = dsn.get("port")
    if host:
        uri = "postgresql://"
        if user:
            uri += user
            if password:
                uri += f":{password}"
            uri += "@"
        uri += host
        if port:
            uri += f":{port}"
        uri += f"/{dbname}"
        return f"--dbname={uri}"
    else:
        return f"--dbname={dbname}"


# --- Catalog helpers ---
def discover_target_schemas(conn, requested_schemas: Optional[List[str]] = None) -> List[str]:
    # Returns list of schemas to export.
    # If caller supplied a list (CLI --schemas) use that; otherwise list all non-system schemas.
    # We exclude 'pg_catalog', 'information_schema' and schemas starting with 'pg_t' (mainly pg_toast and pg_temp).
    # This avoids exporting temporary or system schemas.
    if requested_schemas:
        return requested_schemas
    q = """
    SELECT nspname
    FROM pg_namespace
    WHERE nspname NOT IN ('pg_catalog', 'information_schema')
      AND nspname NOT LIKE 'pg_t%' 
    ORDER BY nspname;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        rows = [r[0] for r in cur.fetchall()]
    return rows


def extension_schemas(conn) -> Dict[str, str]:
    # Returns mapping extension_name -> schema_name (where extension is installed).
    # We use this to avoid dumping extension-owned objects (e.g. pgcrypto functions) in user functions/.
    # Instead we write a small CREATE EXTENSION IF NOT EXISTS stub under extensions/.
    q = """
    SELECT e.extname, n.nspname
    FROM pg_extension e
    JOIN pg_namespace n ON e.extnamespace = n.oid;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return {row[0]: row[1] for row in cur.fetchall()}


# --- Dumpers ---
def dump_extensions_stub(conn, outdir: Path):
    # For each installed extension write a small stub file:
    #   CREATE EXTENSION IF NOT EXISTS extname [WITH SCHEMA schemaname];
    # We do NOT call `pg_dump --extension` since that sometimes inlines many other objects.
    # These stubs record the presence of extensions without stuffing 55k lines into one file.
    # NOTE: uses sql.Identifier(extname).as_string(conn) to correctly quote the extension identifier.
    ensure_dir(outdir)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT "
            "e.extname, "
            "n.nspname, "
            "e.extversion "
            "FROM pg_extension e "
            "JOIN pg_namespace n "
            "ON e.extnamespace = n.oid "
            "ORDER BY e.extname;"
        )
        for extname, nsp, extver in cur.fetchall():
            fname = safe_filename(f"{extname}.sql")
            p = outdir / fname
            content = (
                f"-- extension: {extname}\n"
                f"CREATE EXTENSION IF NOT EXISTS {sql.Identifier(extname).as_string(conn)}"
            )
            # try to specify schema when not default (CREATE EXTENSION ... WITH SCHEMA name)
            if nsp and nsp != "public":
                content += f" WITH SCHEMA {nsp}"
            content += ";\n"
            p.write_text(content, encoding="utf-8", errors="replace")
            logger.info("Wrote extension stub: %s", p.relative_to(Path.cwd()))


def dump_tables_and_related(
        pg_dump_conn_arg: str,
        conn: psycopg2.extensions.connection,
        schemas: List[str],
        outdir: Path,
        error_dir: Path,
        include_owner: bool,
        include_privs: bool
):
    # Queries pg_tables for tablenames in target schemas and then runs:
    #   pg_dump -s -t schema.table --no-owner --no-privileges
    # per table. This produces a table-level file (indexes, constraints, ownership).
    # Files are named schema.table.sql.
    # We pass --no-owner / --no-privileges unless the user requests them via CLI flags.
    ensure_dir(outdir)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname = ANY(%s)
            ORDER BY schemaname, tablename;
            """,
            (schemas,),
        )
        rows = cur.fetchall()
    for schemaname, tablename in rows:
        fn = outdir / safe_filename(f"{schemaname}.{tablename}.sql")
        flags = ["-t", f"{schemaname}.{tablename}"]
        if not include_owner:
            flags.append("--no-owner")
        if not include_privs:
            flags.append("--no-privileges")
        run_pg_dump_and_write(pg_dump_conn_arg, flags, fn, error_dir)


def dump_sequences(
        pg_dump_conn_arg: str,
        conn: psycopg2.extensions.connection,
        schemas: List[str],
        outdir: Path,
        error_dir: Path,
        include_owner: bool,
        include_privs: bool
):
    # Finds relkind = 'S' sequences and dumps each via pg_dump -t schema.sequence
    # This captures owned sequences and standalone sequences individually.
    ensure_dir(outdir)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'S' AND n.nspname = ANY(%s)
            ORDER BY n.nspname, c.relname;
            """,
            (schemas,),
        )
        rows = cur.fetchall()
    for nsp, relname in rows:
        fn = outdir / safe_filename(f"{nsp}.{relname}.sql")
        flags = ["-t", f"{nsp}.{relname}"]
        if not include_owner:
            flags.append("--no-owner")
        if not include_privs:
            flags.append("--no-privileges")
        run_pg_dump_and_write(pg_dump_conn_arg, flags, fn, error_dir)


def dump_types_sql(
        conn: psycopg2.extensions.connection,
        outdir: Path,
        schemas: List[str]
):
    # Dumps enum and composite types using direct catalog queries.
    # - Enums: builds CREATE TYPE ... AS ENUM ('a','b',...) and writes one file per enum.
    # - Composite types: fetches attributes from pg_attribute and builds a CREATE TYPE ... AS (col type, ...).
    # We avoid pg_dump -t for types because -t sometimes picks up relation-like names unexpectedly.
    ensure_dir(outdir)
    # 1) Enums
    with conn.cursor() as cur:
        cur.execute("""
            SELECT n.nspname AS schema_name,
                   t.typname AS type_name,
                   string_agg(quote_literal(e.enumlabel), ',' ORDER BY e.enumsortorder) AS labels
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            JOIN pg_enum e ON e.enumtypid = t.oid
            WHERE n.nspname = ANY(%s)
            GROUP BY n.nspname, t.typname
            ORDER BY n.nspname, t.typname;
        """, (schemas,))
        for schema_name, type_name, labels in cur.fetchall():
            ddl = f"CREATE TYPE {schema_name}.{type_name} AS ENUM ({labels});\n"
            fn = outdir / safe_filename(f"{schema_name}.{type_name}.sql")
            fn.write_text(ddl, encoding="utf-8", errors="replace")
            logger.info("Wrote enum type: %s", fn.relative_to(Path.cwd()))

    # 2) Composite types
    # For a composite type, pg_type.typrelid links to a pg_class whose attributes are the columns
    with conn.cursor() as cur:
        cur.execute("""
            SELECT n.nspname AS schema_name,
                   t.typname AS type_name,
                   a.attname,
                   format_type(a.atttypid, a.atttypmod) AS att_type,
                   a.attnum
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            JOIN pg_class c ON t.typrelid = c.oid
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0
            WHERE n.nspname = ANY(%s)
              AND t.typtype = 'c'
            ORDER BY n.nspname, t.typname, a.attnum;
        """, (schemas,))
        rows = cur.fetchall()

    # Group attributes per (schema_name, type_name)
    from collections import defaultdict
    groups = defaultdict(list)
    for schema_name, type_name, attname, att_type, attnum in rows:
        groups[(schema_name, type_name)].append((attnum, attname, att_type))

    for (schema_name, type_name), attrs in groups.items():
        attrs_sorted = sorted(attrs, key=lambda x: x[0])
        columns = ", ".join(f"{attname} {att_type}" for _, attname, att_type in attrs_sorted)
        ddl = f"CREATE TYPE {schema_name}.{type_name} AS ({columns});\n"
        fn = outdir / safe_filename(f"{schema_name}.{type_name}.sql")
        fn.write_text(ddl, encoding="utf-8", errors="replace")
        logger.info("Wrote composite type: %s", fn.relative_to(Path.cwd()))


def dump_functions(
        conn: psycopg2.extensions.connection,
        outdir: Path,
        schemas: List[str],
        ext_schema_names: Set[str],
        pretty_names: bool
):
    # Uses pg_get_functiondef(oid) to get the full CREATE FUNCTION text for each function/procedure.
    # - Filters by schema(s) provided.
    # - Skips functions whose schema is in ext_schema_names (extension-owned).
    # - If pretty_names option is used the filename includes a sanitized argument signature; otherwise uses schema.proname.
    # - Writes DDL as UTF-8.
    #
    # Notes:
    # - We intentionally do not include OIDs in filenames to keep them human-friendly.
    # - If you have overloaded functions with the same name and identical signatures across schemas,
    #   the filename might collide; you can enable the short-hash fallback if desired.
    ensure_dir(outdir)
    with conn.cursor() as cur:
        # select functions/procedures in target schemas
        # exclude functions whose namespace is an extension schema (we'll keep extension stubs instead)
        cur.execute(
            """
            SELECT p.oid, n.nspname, p.proname,
                   pg_get_function_identity_arguments(p.oid) AS args_sig,
                   pg_get_functiondef(p.oid) AS ddl
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = ANY(%s)
            ORDER BY n.nspname, p.proname, p.oid;
            """,
            (schemas,),
        )
        for oid, nsp, proname, args_sig, ddl in cur.fetchall():
            if nsp in ext_schema_names:
                # skip extension-owned functions
                logger.debug("Skipping extension-owned function: %s.%s", nsp, proname)
                continue
            if pretty_names:
                arg_fragment = args_sig.replace(", ", "_").replace(" ", "")
                fname_base = f"{nsp}.{proname}__{arg_fragment}"
            else:
                fname_base = f"{nsp}.{proname}"
            filename = safe_filename(fname_base) + ".sql"
            p = outdir / filename
            p.write_text(ddl + "\n", encoding="utf-8", errors="replace")
            logger.info("Wrote function: %s", p.relative_to(Path.cwd()))


def dump_views(
        conn: psycopg2.extensions.connection,
        outdir: Path,
        schemas: List[str]
):
    # Uses pg_get_viewdef to write CREATE OR REPLACE VIEW schema.view AS ... ; into files.
    # We add the CREATE OR REPLACE header to ensure idempotence.
    ensure_dir(outdir)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.oid, n.nspname, c.relname, pg_get_viewdef(c.oid, true) AS viewdef
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'v' AND n.nspname = ANY(%s)
            ORDER BY n.nspname, c.relname;
            """,
            (schemas,),
        )
        for oid, nsp, viewname, viewdef in cur.fetchall():
            # build SQL with create or replace
            header = f"CREATE OR REPLACE VIEW {nsp}.{viewname} AS\n"
            ddl = header + viewdef.rstrip() + ";\n"
            fn = outdir / safe_filename(f"{nsp}.{viewname}.sql")
            fn.write_text(ddl, encoding="utf-8", errors="replace")
            logger.info("Wrote view: %s", fn.relative_to(Path.cwd()))


def dump_triggers(conn: psycopg2.extensions.connection, outdir: Path, schemas: List[str]):
    # Uses pg_get_triggerdef(oid, true) to obtain trigger DDL (which includes CREATE TRIGGER ...).
    # Filename constructed as schema.table.triggername.sql (OID omitted).
    # If filename collisions are a worry, add a short hash of triggerddl (see optional snippet).
    ensure_dir(outdir)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.oid, n.nspname, c.relname, t.tgname, pg_get_triggerdef(t.oid, true) AS triggerddl
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE NOT t.tgisinternal AND n.nspname = ANY(%s)
            ORDER BY n.nspname, c.relname, t.tgname;
            """,
            (schemas,),
        )
        for oid, nsp, tablename, tgname, triggerddl in cur.fetchall():
            fn = outdir / safe_filename(f"{nsp}.{tablename}.{tgname}.sql")
            fn.write_text(triggerddl.rstrip() + ";\n", encoding="utf-8", errors="replace")
            logger.info("Wrote trigger: %s", fn.relative_to(Path.cwd()))


# --- Apply script / makefile / git ---
APPLY_ORDER = ["types", "extensions", "sequences", "tables", "views", "functions", "triggers", "other"]


def generate_apply_script(outdir: Path):
    # writes 00_apply.sh which iterates directories in APPLY_ORDER and psql -f each SQL file.
    # Useful to reapply the exported schema in a sensible order.
    script = outdir / "00_apply.sh"
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "if [ -z \"${PGDSN:-}\" ]; then",
        "  echo 'Set PGDSN, e.g.: export PGDSN=\"postgresql://user:pass@host:port/dbname\"'",
        "  exit 1",
        "fi",
        "",
        "apply_dir() {",
        "  d=$1",
        "  if [ -d \"$d\" ]; then",
        "    echo \"Applying $d\"",
        "    find \"$d\" -type f -name '*.sql' | sort | while read f; do",
        "      echo \"psql $PGDSN -f $f\"",
        "      psql \"$PGDSN\" -f \"$f\"",
        "    done",
        "  fi",
        "}",
        "",
    ]
    for part in APPLY_ORDER:
        lines.append(f"apply_dir \"{part}\"")
    script.write_text("\n".join(lines), encoding="utf-8", errors="replace")
    script.chmod(0o755)
    logger.info("Wrote apply script: %s", script.relative_to(Path.cwd()))


def generate_makefile(outdir: Path):
    # writes a tiny Makefile that calls 00_apply.sh when `make apply` run.
    mf = outdir / "Makefile"
    content = (
        ".PHONY: all apply\n\n"
        "all: apply\n\n"
        "apply:\n"
        "\t./00_apply.sh\n"
    )
    mf.write_text(content, encoding="utf-8", errors="replace")
    logger.info("Wrote Makefile: %s", mf.relative_to(Path.cwd()))


def init_git(
        outdir: Path,
        user_name: Optional[str] = None,
        user_email: Optional[str] = None,
        commit: bool = True,
        commit_message: str = "Initial schema export"
):
    # Initialises a git repo in outdir (if missing), writes/updates .gitignore, optionally stages and commits.
    # - Uses local git config user.name / user.email if provided.
    # - Uses `git -C <outdir>` so commands run in the repo root.
    # - Returns True on success, False on failure.
    #
    # Caveats:
    # - Ensure git is on PATH.
    # - If the folder is already a bare repo you may see errors; the function expects a normal work tree.
    # - Avoid committing secrets — .gitignore contains .env by default.
    try:
        # If already a git repo, skip init
        if (outdir / ".git").exists():
            logger.info("Directory already a git repository: %s", outdir)
        else:
            subprocess.run(["git", "init", str(outdir)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logger.info("Initialised git repository in %s", outdir)

        # local repo git config (only set if values provided)
        if user_name:
            subprocess.run(["git", "-C", str(outdir), "config", "user.name", user_name], check=True)
            logger.info("Set git user.name = %s", user_name)
        if user_email:
            subprocess.run(["git", "-C", str(outdir), "config", "user.email", user_email], check=True)
            logger.info("Set git user.email = %s", user_email)

        # Write .gitignore (append if exists)
        gitignore = outdir / ".gitignore"
        default_ignore = [
            "__pycache__/",
            "*.pyc",
            "pg_dump_errors/",
            ".env",
            "export_schema.log",
            "venv/",
            ".venv/",
            "*.sqlite3",
            "*.db"
        ]
        # If file exists, append only missing lines
        if gitignore.exists():
            existing = gitignore.read_text(encoding="utf-8", errors="replace").splitlines()
            to_add = [l for l in default_ignore if l not in existing]
            if to_add:
                gitignore.write_text("\n".join(existing + to_add) + "\n", encoding="utf-8", errors="replace")
        else:
            gitignore.write_text("\n".join(default_ignore) + "\n", encoding="utf-8", errors="replace")
        logger.info("Wrote .gitignore (%d entries)", len(default_ignore))

        if commit:
            # Stage everything (but not untracked large files if user wants to be selective)
            subprocess.run(["git", "-C", str(outdir), "add", "."], check=True)
            # Commit if there are staged changes
            # Use plumbing to check if there is anything to commit
            status_proc = subprocess.run(["git", "-C", str(outdir), "status", "--porcelain"], stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE, check=True)
            if status_proc.stdout.strip():
                subprocess.run(["git", "-C", str(outdir), "commit", "-m", commit_message], check=True)
                logger.info("Created initial commit in %s with message: %s", outdir, commit_message)
            else:
                logger.info("No changes to commit in %s", outdir)

            # Ensure there's a 'main' branch
            # If repo already has a branch named main, this is a no-op; otherwise create/set main to HEAD
            try:
                subprocess.run(["git", "-C", str(outdir), "rev-parse", "--verify", "main"], check=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                # 'main' exists; do nothing
            except subprocess.CalledProcessError:
                # create main branch pointing at current HEAD
                subprocess.run(["git", "-C", str(outdir), "branch", "-M", "main"], check=True)
                logger.info("Set branch 'main'")

        return True

    except FileNotFoundError:
        logger.warning("git not found on PATH; skipping git init/commit.")
        return False
    except subprocess.CalledProcessError as exception:
        logger.exception("git command failed: %s", exception)
        return False
    except Exception as exception:
        # Narrow catch: log and return False
        logger.exception("Unexpected error during git init: %s", exception)
        return False


# --- Orchestrator ---
def export_schema(dsn: Dict[str, str],
                  outdir: Path,
                  requested_schemas: Optional[List[str]],
                  include_owner: bool,
                  include_privs: bool,
                  pretty_func_names: bool,
                  generate_makefile_flag: bool,
                  init_git_flag: bool):
    # Orchestrates the whole export:
    # - Connects to DB using psycopg2 (data source name produced from DB_CONFIG or CLI).
    # - Discovers target schemas.
    # - Writes extension stubs, then tables, sequences, types, views, functions, triggers.
    # - Closes connection and optionally generates apply script/makefile and runs init_git.
    #
    # Notes:
    # - We call pg_dump per-table/sequence/type for precise DDL.
    # - We skip extension-owned functions (to avoid duplication).
    # - pg_dump stderr is saved under pg_dump_errors/ so the output dirs stay clean.
    ensure_dir(outdir)
    error_dir = outdir / "pg_dump_errors"
    pg_dump_conn_arg = dsn_to_pg_dump_connarg(dsn)

    conn = psycopg2.connect(**dsn)
    conn.autocommit = True

    try:
        schemas = discover_target_schemas(conn, requested_schemas)
        logger.info("Target schemas: %s", ", ".join(schemas))

        # determine extension schemas mapping extname -> schema
        ext_map = extension_schemas(conn)
        ext_schema_names = set(ext_map.values())
        logger.info("Extensions detected: %s", ", ".join(ext_map.keys()) or "<none>")

        # Dump extension create stubs
        dump_extensions_stub(conn, outdir / "extensions")

        # Tables
        dump_tables_and_related(pg_dump_conn_arg,
                                conn,
                                schemas,
                                outdir / "tables",
                                error_dir,
                                include_owner,
                                include_privs)

        # Sequences
        dump_sequences(pg_dump_conn_arg,
                       conn,
                       schemas,
                       outdir / "sequences",
                       error_dir,
                       include_owner,
                       include_privs)

        # Types
        dump_types_sql(conn, outdir / "types", schemas)

        # Views
        dump_views(conn, outdir / "views", schemas)

        # Functions (exclude ext schemas)
        dump_functions(conn, outdir / "functions", schemas, ext_schema_names, pretty_func_names)

        # Triggers
        dump_triggers(conn, outdir / "triggers", schemas)

    finally:
        conn.close()

    if generate_makefile_flag:
        generate_apply_script(outdir)
        generate_makefile(outdir)

    logger.info("Export completed into %s", outdir.resolve())


# --- CLI ---
def parse_args(argv=None):
    # defines CLI flags (see separate section below).
    p = argparse.ArgumentParser(description="Export Postgres schema objects into separate files.")
    p.add_argument("--host")
    p.add_argument("--port")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--dbname", required=(DB_CONFIG is None))
    p.add_argument("--schemas",
                   help="Comma-separated list of schemas to export. If omitted, export all non-system schemas.")
    p.add_argument("--outdir", default="pg_schema_export")
    p.add_argument("--include-owner", action="store_true")
    p.add_argument("--include-privileges", action="store_true")
    p.add_argument("--pretty-func-names", action="store_true")
    p.add_argument("--generate-makefile", action="store_true")
    p.add_argument("--init-git", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--git-commit-message", help="Custom git commit message when using --init-git")
    return p.parse_args(argv)


def build_dsn_from_args(args) -> Dict[str, str]:
    # builds a psycopg2-friendly dict from DB_CONFIG or CLI args.
    if DB_CONFIG:
        dsn = dict(DB_CONFIG)
        # override if CLI provided
        if args.host: dsn["host"] = args.host
        if args.port: dsn["port"] = args.port
        if args.user: dsn["user"] = args.user
        if args.password: dsn["password"] = args.password
        if args.dbname: dsn["dbname"] = args.dbname
        if "database" in dsn and "dbname" not in dsn:
            dsn["dbname"] = dsn["database"]
        return dsn
    else:
        dsn = {"dbname": args.dbname}
        if args.host: dsn["host"] = args.host
        if args.port: dsn["port"] = args.port
        if args.user: dsn["user"] = args.user
        if args.password: dsn["password"] = args.password
        return dsn


def main(argv=None):
    # main function:
    #  - Parses args, builds outdir, parses --schemas, builds dsn
    #  - Calls export_schema(...)
    #  - If AUTO_INIT_GIT and run with no CLI args (typical PyCharm F5), runs init_git with configured identity.
    #
    # Note: run with --verbose to enable DEBUG logging, which prints pg_dump calls and additional info.
    args = parse_args(argv)
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    outdir = Path(args.outdir).absolute()
    ensure_dir(outdir)

    schemas = None
    if args.schemas:
        schemas = [
            s.strip()
            for s in args.schemas.split(",")
            if s.strip() and not s.startswith("pg_temp")
        ]

    dsn = build_dsn_from_args(args)
    export_schema(
        dsn=dsn,
        outdir=outdir,
        requested_schemas=schemas,
        include_owner=args.include_owner,
        include_privs=args.include_privileges,
        pretty_func_names=args.pretty_func_names,
        generate_makefile_flag=args.generate_makefile,
        init_git_flag=args.init_git,
    )

    # ---------- AUTO GIT (for running in PyCharm / with no CLI args) ----------
    # Criteria: no CLI args passed (typical when pressing F5), or PyCharm's env var present.
    ran_with_no_cli = (len(sys.argv) == 1)
    pycharm_env = bool(os.environ.get("PYCHARM_HOSTED") or os.environ.get("PYCHARM_HOST"))

    if AUTO_INIT_GIT and (ran_with_no_cli or pycharm_env):
        logger.info("Auto git init/commit enabled. Running init_git() with configured identity.")
        # call init_git with the hard-coded identity and automatic commit
        init_git(
            outdir,
            user_name=AUTO_GIT_USER_NAME,
            user_email=AUTO_GIT_USER_EMAIL,
            commit=True,
            commit_message=AUTO_GIT_COMMIT_MESSAGE,
        )
    else:
        # If user explicitly requested --init-git on the CLI handle that too
        if args.init_git:
            commit_msg = args.git_commit_message or AUTO_GIT_COMMIT_MESSAGE or "Initial schema export"
            init_git(
                outdir,
                user_name=AUTO_GIT_USER_NAME,
                user_email=AUTO_GIT_USER_EMAIL,
                commit=True,
                commit_message=commit_msg,
            )
    logger.info("Done.")


if __name__ == "__main__":
    main()

# python export_schema.py --outdir pg_schema_export --init-git --git-commit-message "Test custom git commit message from command line"