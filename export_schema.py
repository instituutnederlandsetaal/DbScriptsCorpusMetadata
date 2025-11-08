#!/usr/bin/env python3
"""
pg_schema_export.py - exporter: one file per object, multi-schema, safe extension handling

Usage examples:
  ./export_schema.py --outdir schema_dump --generate-makefile --init-git
  ./export_schema.py --schemas core,factory,metrics --outdir schema_dump

Notes:
 - Requires Python 3.8+
 - Requires pg_dump on PATH and psycopg2 installed.
 - If you have database_config.DB_CONFIG it will be used automatically.
"""

from __future__ import annotations

import argparse
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Set, Optional

import psycopg2
from psycopg2 import sql

# --- Auto-commit settings ---
from git_config import GIT_CONFIG
AUTO_INIT_GIT = GIT_CONFIG["AUTO_INIT_GIT"]
AUTO_GIT_USER_NAME = GIT_CONFIG["AUTO_GIT_USER_NAME"]
AUTO_GIT_USER_EMAIL = GIT_CONFIG["AUTO_GIT_USER_EMAIL"]
AUTO_GIT_COMMIT_MESSAGE = GIT_CONFIG["AUTO_GIT_COMMIT_MESSAGE"]

# --- Logging ---
LOG_FILENAME = "export_schema.log"  # change this filename if you want

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
    from database_config import DB_CONFIG  # type: ignore
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
    return re.sub(r"[^A-Za-z0-9_.()-]", "_", name)


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def run_pg_dump_and_write(pg_dump_conn_arg: str, args: List[str], out_path: Path, error_dir: Path) -> bool:
    """
    Run pg_dump -s with args and write stdout to out_path.
    On failure write stderr into error_dir/<basename>.err and return False.
    """
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


# ---------- Catalog helpers ----------
def discover_target_schemas(conn, requested_schemas: Optional[List[str]] = None) -> List[str]:
    """
    If requested_schemas provided, return that list.
    Otherwise return all non-system schemas (exclude pg_catalog, information_schema, pg_toast*).
    """
    if requested_schemas:
        return requested_schemas
    q = """
    SELECT nspname
    FROM pg_namespace
    WHERE nspname NOT IN ('pg_catalog', 'information_schema')
      AND nspname NOT LIKE 'pg_toast%'
    ORDER BY nspname;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        rows = [r[0] for r in cur.fetchall()]
    return rows


def extension_schemas(conn) -> Dict[str, str]:
    """
    Return a mapping extname -> schema_name where the extension is installed.
    Used to avoid dumping extension-owned objects as "user functions".
    """
    q = """
    SELECT e.extname, n.nspname
    FROM pg_extension e
    JOIN pg_namespace n ON e.extnamespace = n.oid;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return {row[0]: row[1] for row in cur.fetchall()}


# ---------- Dumpers ----------
def dump_extensions_stub(conn, outdir: Path):
    """
    Create small CREATE EXTENSION IF NOT EXISTS files for each installed extension.
    (Avoid using pg_dump --extension to prevent dumping entire schemas.)
    """
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
    """
    Use pg_dump -t per table for each schema/table
    """
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
    """
    Dump enum and composite types via SQL into individual files (UTF-8).
    """
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
            fn = outdir / safe_filename(f"{nsp}.{tablename}.{tgname}__{oid}.sql")
            fn.write_text(triggerddl.rstrip() + ";\n", encoding="utf-8", errors="replace")
            logger.info("Wrote trigger: %s", fn.relative_to(Path.cwd()))


# ---------- Apply script / makefile / git ----------
APPLY_ORDER = ["types", "extensions", "sequences", "tables", "views", "functions", "triggers", "other"]


def generate_apply_script(outdir: Path):
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
    mf = outdir / "Makefile"
    content = (
        ".PHONY: all apply\n\n"
        "all: apply\n\n"
        "apply:\n"
        "\t./00_apply.sh\n"
    )
    mf.write_text(content, encoding="utf-8", errors="replace")
    logger.info("Wrote Makefile: %s", mf.relative_to(Path.cwd()))


def init_git(outdir: Path):
    try:
        subprocess.run(["git", "init", str(outdir)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        gi = outdir / ".gitignore"
        gi.write_text("__pycache__/\n*.pyc\npg_dump_errors/\n", encoding="utf-8", errors="replace")
        logger.info("Initialized git repo in %s", outdir)
    except Exception as exception:
        logger.warning("git init failed: %s", exception)


# ---------- Orchestrator ----------
def export_schema(dsn: Dict[str, str],
                  outdir: Path,
                  requested_schemas: Optional[List[str]],
                  include_owner: bool,
                  include_privs: bool,
                  pretty_func_names: bool,
                  generate_makefile_flag: bool,
                  init_git_flag: bool):
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
        dump_tables_and_related(pg_dump_conn_arg, conn, schemas, outdir / "tables", error_dir, include_owner,
                                include_privs)

        # Sequences
        dump_sequences(pg_dump_conn_arg, conn, schemas, outdir / "sequences", error_dir, include_owner, include_privs)

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
    if init_git_flag:
        init_git(outdir)

    logger.info("Export completed into %s", outdir.resolve())


# ---------- CLI ----------
def parse_args(argv=None):
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
    return p.parse_args(argv)


def build_dsn_from_args(args) -> Dict[str, str]:
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
    args = parse_args(argv)
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    outdir = Path(args.outdir).absolute()
    schemas = None
    if args.schemas:
        schemas = [s.strip() for s in args.schemas.split(",") if s.strip()]
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


if __name__ == "__main__":
    main()
