"""SQLite connection management.

The application runs as two (or more) uvicorn processes sharing one SQLite
database file.  Two kinds of connections are exposed:

* :func:`get_read_conn` -- a read-only connection (``mode=ro``) used for all
  relationship computation (graph traversal, group listing, CSV export).
  These connections never take a write lock.
* :func:`write_tx` -- a short-lived *deferred-immediate* write transaction.
  ``BEGIN IMMEDIATE`` acquires the reserved lock up front so that two
  concurrent adjudications are serialised; the second one waits on
  ``busy_timeout`` and then validates the whole partition constraint against
  the already-committed state of the first.  Either the whole adjudication
  commits or the database is rolled back to its previous state.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator

# Default location; overridable through the ER_DB_PATH environment variable so
# that tests and the docker-compose stack can point at different files.
DEFAULT_DB_PATH = os.environ.get(
    "ER_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "er.db")
)

# How long a second writer waits behind the first before SQLITE_BUSY is raised.
BUSY_TIMEOUT_MS = int(os.environ.get("ER_BUSY_TIMEOUT_MS", "10000"))

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA busy_timeout = %d;

CREATE TABLE IF NOT EXISTS records (
    id           INTEGER PRIMARY KEY,
    cohort       TEXT NOT NULL,
    source_id    TEXT NOT NULL,
    alias        TEXT NOT NULL,
    imported_at  TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (cohort, source_id)
);

CREATE TABLE IF NOT EXISTS groups_t (
    group_id     INTEGER PRIMARY KEY,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS record_groups (
    record_id    INTEGER PRIMARY KEY REFERENCES records(id),
    group_id     INTEGER NOT NULL REFERENCES groups_t(group_id)
);
CREATE INDEX IF NOT EXISTS idx_record_groups_group ON record_groups(group_id);

CREATE TABLE IF NOT EXISTS adjudications (
    id           INTEGER PRIMARY KEY,
    left_id      INTEGER NOT NULL REFERENCES records(id),
    right_id     INTEGER NOT NULL REFERENCES records(id),
    decision     TEXT NOT NULL CHECK (decision IN ('same', 'different')),
    active       INTEGER NOT NULL DEFAULT 1,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    revoked_at   TEXT
);
-- At most one *active* adjudication per unordered record pair.  Revoked rows
-- are kept as history and do not participate in the uniqueness rule.
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_pair
    ON adjudications(left_id, right_id) WHERE active = 1;
CREATE INDEX IF NOT EXISTS idx_adjudications_active
    ON adjudications(left_id, right_id) WHERE active = 1;
""" % (BUSY_TIMEOUT_MS,)


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys = ON")


def initialize(db_path: str | None = None) -> None:
    """Create the schema (idempotent). Safe to call from every process."""
    db_path = db_path or DEFAULT_DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=BUSY_TIMEOUT_MS / 1000.0)
    try:
        _apply_pragmas(conn)
        conn.executescript(SCHEMA)
    finally:
        conn.close()


def get_read_conn(db_path: str | None = None) -> sqlite3.Connection:
    """Return a strictly read-only connection (cannot take a write lock)."""
    db_path = db_path or DEFAULT_DB_PATH
    uri = f"file:{os.path.abspath(db_path)}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=BUSY_TIMEOUT_MS / 1000.0)
    _apply_pragmas(conn)
    # Belt and braces: refuse any statement that would mutate the database.
    conn.execute("PRAGMA query_only = ON")
    return conn


@contextmanager
def write_tx(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    """Open a write transaction with an immediate reserved lock.

    Commits on clean exit, rolls back on any exception.  All constraint
    validation for an adjudication happens inside this single transaction,
    which is what makes "two individually-legal decisions that together break
    the partition" impossible under concurrency.
    """
    db_path = db_path or DEFAULT_DB_PATH
    conn = sqlite3.connect(db_path, timeout=BUSY_TIMEOUT_MS / 1000.0)
    _apply_pragmas(conn)
    try:
        # Acquire the write lock before reading, so the validation below sees a
        # state that cannot change before the commit.
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
