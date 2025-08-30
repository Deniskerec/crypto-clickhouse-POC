import os
import glob
import hashlib

import sqlparse  # robust SQL splitter
from dotenv import load_dotenv
from clickhouse_connect.driver.exceptions import Error as ChError

from db import ch_client

# Load env (CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE, ...)
load_dotenv()

DB = os.getenv("CH_DATABASE", "crypto")
MIG_TABLE = f"{DB}._migrations"  # internal registry of applied migrations
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")


# -------------------------- helpers --------------------------

def sha256(path: str) -> str:
    """Checksum file contents to detect edits after apply."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()


def ensure_database(client):
    """Create target database if missing."""
    client.command(f"CREATE DATABASE IF NOT EXISTS {DB}")


def table_exists(client, full_name: str) -> bool:
    """Check if db.table exists via system.tables."""
    db, tbl = full_name.split(".", 1)
    q = (
        "SELECT count() AS c FROM system.tables "
        f"WHERE database = '{db}' AND name = '{tbl}'"
    )
    res = client.query(q)
    fi = res.first_item
    # first_item may be a dict or a tuple depending on settings
    c = fi["c"] if isinstance(fi, dict) else fi[0]
    return c > 0

def ensure_migrations_table(client):
    """Create the _migrations registry table."""
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {MIG_TABLE}
        (
            version     UInt32,
            filename    String,
            checksum    String,
            applied_at  DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY (version, filename)
    """)


def load_applied(client):
    """Return {(version, filename): checksum} already applied."""
    if not table_exists(client, MIG_TABLE):
        return {}
    res = client.query(
        f"SELECT version, filename, checksum "
        f"FROM {MIG_TABLE} "
        f"ORDER BY version, filename"
    )
    return {(r[0], r[1]): r[2] for r in res.result_rows}


def parse_version(path: str) -> int:
    """
    Expect names like V1__create_trades_table.sql
    Returns the integer 1.
    """
    base = os.path.basename(path)
    assert base.startswith("V") and "__" in base and base.endswith(".sql"), \
        f"Bad migration name: {base}"
    return int(base[1: base.index("__")])


def run_sql_file_split(client, path: str):
    """
    Split a .sql file into individual statements and execute sequentially.
    Avoids 'Multi-statements are not allowed' errors.
    """
    sql_text = open(path, "r", encoding="utf-8").read()
    statements = [s.strip() for s in sqlparse.split(sql_text) if s.strip()]
    for i, stmt in enumerate(statements, 1):
        try:
            client.command(stmt)
        except Exception as ex:
            # Show which statement failed for easier debugging
            preview = stmt if len(stmt) < 800 else stmt[:800] + " …"
            raise RuntimeError(
                f"Failed executing statement #{i} in {os.path.basename(path)}:\n{preview}\nError: {ex}"
            ) from ex


def apply_sql_file(client, path: str, version: int, filename: str, checksum: str):
    """Execute all statements in the file and record the migration."""
    print(f"→ Applying V{version} {filename} …")
    run_sql_file_split(client, path)
    client.insert(
        MIG_TABLE,
        [(version, filename, checksum)],
        column_names=["version", "filename", "checksum"],
    )
    print(f"✓ Applied V{version} {filename}")


# -------------------------- entrypoint --------------------------

def migrate():
    client = ch_client()

    # Create DB and _migrations registry first
    ensure_database(client)
    ensure_migrations_table(client)

    applied = load_applied(client)

    # Discover V*__*.sql files and sort by version number
    files = sorted(
        glob.glob(os.path.join(SQL_DIR, "V*__*.sql")),
        key=parse_version
    )

    for path in files:
        version = parse_version(path)
        filename = os.path.basename(path)
        checksum = sha256(path)
        key = (version, filename)

        if key in applied:
            # Already applied — verify checksum matches (immutable migration policy)
            if applied[key] != checksum:
                raise RuntimeError(
                    f"Checksum mismatch for {filename}. It was already applied with different content.\n"
                    f"Do NOT edit applied migrations. Create a new one (e.g., V{version + 1}__...)."
                )
            print(f"= Skipping V{version} {filename} (already applied).")
            continue

        apply_sql_file(client, path, version, filename, checksum)

    print("All migrations up to date.")


if __name__ == "__main__":
    try:
        migrate()
    except ChError as ex:
        print(f"ClickHouse error: {ex}")
        raise