import os
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

from config import DB_PATH


SCHEMA_SQL = (
    """
    CREATE TABLE IF NOT EXISTS purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chequeid INTEGER,
        file_path TEXT,
        date TEXT,
        created_at TEXT,
        product_name TEXT,
        quantity REAL DEFAULT 1,
        price REAL,
        discount REAL,
        category1 TEXT,
        category2 TEXT,
        category3 TEXT,
        organization TEXT,
        username TEXT,
        description TEXT
    );
    """
)


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    return sqlite3.connect(path)


def init_db(db_path: Optional[str] = None) -> None:
    with get_connection(db_path) as conn:
        conn.execute(SCHEMA_SQL)
        conn.commit()
    migrate_db(db_path)


def migrate_db(db_path: Optional[str] = None) -> None:
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='purchases'")
            if not cursor.fetchone():
                return
            
            # PRAGMA table_info returns tuples: (cid, name, type, notnull, dflt_value, pk)
            table_info = cursor.execute("PRAGMA table_info(purchases)").fetchall()
            columns = [row[1] for row in table_info]

            # Add missing columns one-by-one, do not abort on error
            if "username" not in columns:
                try:
                    cursor.execute("ALTER TABLE purchases ADD COLUMN username TEXT")
                except Exception:
                    pass
            if "description" not in columns:
                try:
                    cursor.execute("ALTER TABLE purchases ADD COLUMN description TEXT")
                except Exception:
                    pass
            if "quantity" not in columns:
                try:
                    cursor.execute("ALTER TABLE purchases ADD COLUMN quantity REAL DEFAULT 1")
                except Exception:
                    pass
            else:
                quantity_column = next((row for row in table_info if row[1] == "quantity"), None)
                if quantity_column and quantity_column[2].upper() not in {"REAL", "NUMERIC"}:
                    try:
                        cursor.execute("DROP TABLE IF EXISTS purchases_tmp")
                        cursor.execute(
                            """
                            CREATE TABLE IF NOT EXISTS purchases_tmp (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                chequeid INTEGER,
                                file_path TEXT,
                                date TEXT,
                                created_at TEXT,
                                product_name TEXT,
                                quantity REAL DEFAULT 1,
                                price REAL,
                                discount REAL,
                                category1 TEXT,
                                category2 TEXT,
                                category3 TEXT,
                                organization TEXT,
                                username TEXT,
                                description TEXT
                            )
                            """
                        )
                        cursor.execute(
                            """
                            INSERT INTO purchases_tmp (
                                id, chequeid, file_path, date, created_at, product_name,
                                quantity, price, discount, category1, category2, category3,
                                organization, username, description
                            )
                            SELECT
                                id, chequeid, file_path, date, created_at, product_name,
                                CAST(quantity AS REAL), price, discount, category1, category2, category3,
                                organization, username, description
                            FROM purchases
                            """
                        )
                        cursor.execute("DROP TABLE purchases")
                        cursor.execute("ALTER TABLE purchases_tmp RENAME TO purchases")
                    except Exception:
                        cursor.execute("DROP TABLE IF EXISTS purchases_tmp")
            conn.commit()
        except Exception:
            # Ignore migration errors to avoid startup crash; inserts will still surface issues
            pass
        
        create_indexes(conn)


def create_indexes(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    try:
        existing_indexes = [row[0] for row in cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'").fetchall()]
        
        if "idx_date_username_org" not in existing_indexes:
            cursor.execute("CREATE INDEX idx_date_username_org ON purchases(date, username, organization)")
        
        if "idx_username" not in existing_indexes:
            cursor.execute("CREATE INDEX idx_username ON purchases(username)")
        
        conn.commit()
    except Exception as e:
        pass


def get_next_cheque_id(db_path: Optional[str] = None) -> int:
    with get_connection(db_path) as conn:
        cur = conn.execute("SELECT MAX(chequeid) FROM purchases")
        row = cur.fetchone()
        max_id = row[0] if row and row[0] is not None else 0
        return max_id + 1


def insert_purchase(record: Dict, db_path: Optional[str] = None) -> int:
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            (
                "INSERT INTO purchases (chequeid, file_path, date, created_at, product_name, quantity, price, "
                "discount, category1, category2, category3, organization, username, description) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            (
                record.get("chequeid"),
                record.get("file_path"),
                record.get("date"),
                record.get("created_at", datetime.now(timezone.utc).isoformat()),
                record.get("product_name"),
                float(record.get("quantity", 1) or 1),
                float(record.get("price", 0) or 0),
                float(record.get("discount", 0) or 0),
                record.get("category1"),
                record.get("category2"),
                record.get("category3"),
                record.get("organization"),
                record.get("username"),
                record.get("description"),
            ),
        )
        conn.commit()
        return cursor.lastrowid


def bulk_insert_purchases(records: List[Dict], db_path: Optional[str] = None) -> None:
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.executemany(
            (
                "INSERT INTO purchases (chequeid, file_path, date, created_at, product_name, quantity, price, "
                "discount, category1, category2, category3, organization, username, description) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            [
                (
                    rec.get("chequeid"),
                    rec.get("file_path"),
                    rec.get("date"),
                    rec.get("created_at", datetime.now(timezone.utc).isoformat()),
                    rec.get("product_name"),
                    float(rec.get("quantity", 1) or 1),
                    float(rec.get("price", 0) or 0),
                    float(rec.get("discount", 0) or 0),
                    rec.get("category1"),
                    rec.get("category2"),
                    rec.get("category3"),
                    rec.get("organization"),
                    rec.get("username"),
                    rec.get("description"),
                )
                for rec in records
            ],
        )
        conn.commit()


def fetch_all_purchases(db_path: Optional[str] = None) -> List[Tuple]:
    with get_connection(db_path) as conn:
        cur = conn.execute(
            "SELECT id, chequeid, file_path, date, created_at, product_name, quantity, price, discount, "
            "category1, category2, category3, organization, username, description FROM purchases ORDER BY id ASC"
        )
        return cur.fetchall()


def check_duplicate_cheque(date: str, username: str, organization: str, total_sum: float, db_path: Optional[str] = None) -> bool:
    with get_connection(db_path) as conn:
        cur = conn.execute(
            """SELECT chequeid, SUM(price) as cheque_sum
               FROM purchases
               WHERE date = ? AND username = ? AND organization = ?
               GROUP BY chequeid
               HAVING ABS(SUM(price) - ?) < 0.01
               LIMIT 1""",
            (date, username, organization, total_sum)
        )
        result = cur.fetchone()
        return result is not None

