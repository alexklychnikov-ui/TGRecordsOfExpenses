"""Database manager with analytics functions."""
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db.db_manager import (
    get_connection, init_db, bulk_insert_purchases,
    get_next_cheque_id, check_duplicate_cheque
)
from config import DB_PATH


logger = logging.getLogger(__name__)


def _norm_ymd(date_str: str) -> str:
    """Convert DD.MM.YYYY or DD-MM-YYYY to YYYY-MM-DD for correct string compare."""
    try:
        if not date_str:
            return date_str
        s = str(date_str)
        if "." in s and len(s) >= 10:
            return f"{s[6:10]}-{s[3:5]}-{s[0:2]}"
        if "-" in s and len(s) >= 10 and s[4] == "-":  # already ISO-like
            return s[:10]
        if "-" in s and len(s) >= 10 and s[2] == "-":  # DD-MM-YYYY
            return f"{s[6:10]}-{s[3:5]}-{s[0:2]}"
        return s[:10]
    except Exception:
        return date_str


_DATE_EXPR_SQL = (
    "CASE "
    "WHEN date LIKE '__.__.____%' THEN substr(date,7,4)||'-'||substr(date,4,2)||'-'||substr(date,1,2) "
    "WHEN date LIKE '__-__-____%' THEN substr(date,7,4)||'-'||substr(date,4,2)||'-'||substr(date,1,2) "
    "WHEN date LIKE '____-__-__%' THEN substr(date,1,10) "
    "ELSE date END"
)


def fetch_by_period(start_date: str, end_date: str, username: str, db_path: Optional[str] = None) -> List[Dict]:
    with get_connection(db_path) as conn:
        ymd_start = _norm_ymd(start_date)
        ymd_end = _norm_ymd(end_date)
        cur = conn.execute(
            f"SELECT * FROM purchases WHERE ({_DATE_EXPR_SQL}) >= ? AND ({_DATE_EXPR_SQL}) <= ? AND username = ? "
            "ORDER BY date DESC",
            (ymd_start, ymd_end, username)
        )
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]


def fetch_by_category(level: int, name: str, username: str, db_path: Optional[str] = None) -> List[Dict]:
    category_field = f"category{level}"
    with get_connection(db_path) as conn:
        cur = conn.execute(
            f"SELECT * FROM purchases WHERE {category_field} = ? AND username = ? ORDER BY date DESC",
            (name, username)
        )
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]


def fetch_by_organization(organization: str, username: str, db_path: Optional[str] = None) -> List[Dict]:
    org = (organization or "").strip()
    variants = {org, org.lower(), org.upper(), org.title()}
    likes = [f"%{v}%" for v in variants if v]
    if not likes:
        return []
    placeholders = " OR ".join(["organization LIKE ?"] * len(likes))
    with get_connection(db_path) as conn:
        cur = conn.execute(
            f"SELECT * FROM purchases WHERE username = ? AND ({placeholders}) ORDER BY date DESC",
            (username, *likes),
        )
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]


def fetch_by_product_name(product_name: str, username: str, db_path: Optional[str] = None) -> List[Dict]:
    like = f"%{product_name}%"
    with get_connection(db_path) as conn:
        cur = conn.execute(
            "SELECT * FROM purchases WHERE product_name LIKE ? AND username = ? ORDER BY date DESC",
            (like, username)
        )
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]


def fetch_by_description(description: str, username: str, db_path: Optional[str] = None) -> List[Dict]:
    like = f"%{description}%"
    with get_connection(db_path) as conn:
        cur = conn.execute(
            "SELECT * FROM purchases WHERE description LIKE ? AND username = ? ORDER BY date DESC",
            (like, username)
        )
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]


def get_cheque_by_id(chequeid: int, username: str, db_path: Optional[str] = None) -> List[Dict]:
    with get_connection(db_path) as conn:
        cur = conn.execute(
            "SELECT * FROM purchases WHERE chequeid = ? AND username = ? ORDER BY id",
            (chequeid, username)
        )
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]


def get_last_cheque(username: str, db_path: Optional[str] = None) -> List[Dict]:
    with get_connection(db_path) as conn:
        cur = conn.execute(
            "SELECT MAX(chequeid) FROM purchases WHERE username = ?",
            (username,)
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return []
        
        return get_cheque_by_id(row[0], username, db_path)


def get_max_chequeid(username: str, db_path: Optional[str] = None) -> Optional[int]:
    with get_connection(db_path) as conn:
        cur = conn.execute(
            "SELECT MAX(chequeid) FROM purchases WHERE username = ?",
            (username,)
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None


def get_summary(start_date: str, end_date: str, username: str, db_path: Optional[str] = None) -> Dict:
    with get_connection(db_path) as conn:
        ymd_start = _norm_ymd(start_date)
        ymd_end = _norm_ymd(end_date)
        cur = conn.execute(
            f"""SELECT 
                COUNT(*) as count,
                SUM(price) as total,
                COUNT(DISTINCT chequeid) as cheque_count
            FROM purchases 
            WHERE ({_DATE_EXPR_SQL}) >= ? AND ({_DATE_EXPR_SQL}) <= ? AND username = ?""",
            (ymd_start, ymd_end, username)
        )
        row = cur.fetchone()
        return {
            "count": row[0] if row else 0,
            "total": round(row[1], 2) if row and row[1] else 0.0,
            "cheque_count": row[2] if row else 0
        }


def update_record(record_id: int, field: str, value: str, db_path: Optional[str] = None) -> bool:
    allowed_fields = ["price", "discount", "description", "product_name", "quantity", "category1", "category2", "category3", "organization", "date"]
    if field not in allowed_fields:
        raise ValueError(f"Field '{field}' is not allowed for update")
    
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE purchases SET {field} = ? WHERE id = ?",
            (value, record_id)
        )
        conn.commit()
        return cursor.rowcount > 0


def update_field_by_cheque(chequeid: int, field: str, value: str, username: str, db_path: Optional[str] = None) -> int:
    allowed_fields = ["price", "discount", "description", "product_name", "quantity", "category1", "category2", "category3", "organization", "date"]
    if field not in allowed_fields:
        raise ValueError(f"Field '{field}' is not allowed for update")
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE purchases SET {field} = ? WHERE chequeid = ? AND username = ?",
            (value, chequeid, username)
        )
        conn.commit()
        return cursor.rowcount


def update_description_by_cheque(chequeid: int, description: str, username: str, db_path: Optional[str] = None) -> int:
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE purchases SET description = ? WHERE chequeid = ? AND username = ?",
            (description, chequeid, username)
        )
        conn.commit()
        return cursor.rowcount


def update_description_by_organization(organization: str, description: str, username: str, db_path: Optional[str] = None) -> int:
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE purchases SET description = ? WHERE organization = ? AND username = ?",
            (description, organization, username)
        )
        conn.commit()
        return cursor.rowcount


def find_exact_category1(search_value: str, username: str, db_path: Optional[str] = None) -> Optional[str]:
    """
    Находит точное значение category1 в базе данных по поисковому значению.
    Сначала ищет точное совпадение, затем без учета регистра.
    
    Args:
        search_value: Значение для поиска
        username: Имя пользователя для фильтрации
        db_path: Путь к базе данных
    
    Returns:
        Точное значение category1 из базы или None, если не найдено
    """
    if not search_value:
        logger.warning(f"find_exact_category1: пустое значение поиска для username={username}")
        return None
        
    search_value_clean = search_value.strip()
    search_lower = search_value_clean.lower()
    
    logger.info(f"find_exact_category1: поиск '{search_value_clean}' (lower: '{search_lower}') для username={username}")
    
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # Получаем все категории пользователя
        cursor.execute(
            "SELECT DISTINCT category1 FROM purchases WHERE category1 IS NOT NULL AND username = ?",
            (username,)
        )
        rows = cursor.fetchall()
        all_categories = [row[0].strip() for row in rows if row[0]]
        
        logger.info(f"find_exact_category1: найдено {len(all_categories)} категорий для username={username}: {all_categories[:10]}")
        
        # Сначала пробуем точное совпадение
        for category in all_categories:
            if category == search_value_clean:
                logger.info(f"find_exact_category1: найдено точное совпадение '{category}'")
                return category
        
        # Если не найдено, ищем без учета регистра
        for category in all_categories:
            if category.lower() == search_lower:
                logger.info(f"find_exact_category1: найдено совпадение без учета регистра '{category}' для '{search_value_clean}'")
                return category
        
        logger.warning(f"find_exact_category1: категория '{search_value_clean}' не найдена среди {len(all_categories)} категорий")
        return None


def merge_category1_groups(source_value: str, target_value: str, username: str, db_path: Optional[str] = None) -> Tuple[int, bool]:
    """
    Объединяет группы категорий первого уровня.
    
    Args:
        source_value: Значение category1, которое нужно заменить
        target_value: Значение category1, на которое нужно заменить
        username: Имя пользователя для фильтрации
        db_path: Путь к базе данных
    
    Returns:
        Tuple[int, bool]: (количество обновленных записей, True если source_value найден)
    """
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # Проверяем существование source_value
        cursor.execute(
            "SELECT COUNT(*) FROM purchases WHERE category1 = ? AND username = ?",
            (source_value, username)
        )
        count = cursor.fetchone()[0]
        
        if count == 0:
            return (0, False)
        
        # Выполняем обновление
        cursor.execute(
            "UPDATE purchases SET category1 = ? WHERE category1 = ? AND username = ?",
            (target_value, source_value, username)
        )
        conn.commit()
        
        return (cursor.rowcount, True)


def get_category_stats(level: int, start_date: Optional[str] = None, end_date: Optional[str] = None, username: Optional[str] = None, db_path: Optional[str] = None) -> List[Dict]:
    category_field = f"category{level}"
    with get_connection(db_path) as conn:
        query = f"""SELECT 
            {category_field} as category,
            COUNT(*) as count,
            SUM(price) as total
        FROM purchases 
        WHERE {category_field} IS NOT NULL"""
        params = []
        
        if start_date and end_date:
            query += " AND date >= ? AND date <= ?"
            params.extend([start_date, end_date])
        
        if username:
            query += " AND username = ?"
            params.append(username)
        
        query += f" GROUP BY {category_field} ORDER BY total DESC"
        
        cur = conn.execute(query, params)
        rows = cur.fetchall()
        return [{"category": row[0], "count": row[1], "total": round(row[2], 2)} for row in rows]


def get_grouped_stats(field: str, start_date: str, end_date: str, username: str, db_path: Optional[str] = None) -> List[Dict]:
    allowed_fields = {"category1", "category2", "category3", "organization", "description"}
    if field not in allowed_fields:
        raise ValueError(f"Unsupported group field: {field}")
    with get_connection(db_path) as conn:
        ymd_start = _norm_ymd(start_date)
        ymd_end = _norm_ymd(end_date)
        query = f"""
            SELECT {field} as group_name,
                   COUNT(*) as count,
                   COUNT(DISTINCT chequeid) as cheque_count,
                   SUM(price) as total
            FROM purchases
            WHERE ({_DATE_EXPR_SQL}) >= ? AND ({_DATE_EXPR_SQL}) <= ? AND username = ? AND {field} IS NOT NULL
            GROUP BY {field}
            ORDER BY total DESC NULLS LAST
        """
        # SQLite doesn't support NULLS LAST syntax; ignore error with fallback
        try:
            cur = conn.execute(query, (ymd_start, ymd_end, username))
        except Exception:
            query = query.replace(" NULLS LAST", "")
            cur = conn.execute(query, (ymd_start, ymd_end, username))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in rows]
        for r in results:
            r["total"] = round(r.get("total") or 0.0, 2)
        return results


def get_grouped_stats_filtered(field: str, start_date: str, end_date: str, username: str, filters: Dict[str, str], db_path: Optional[str] = None) -> List[Dict]:
    allowed_fields = {"category1", "category2", "category3", "organization", "description"}
    if field not in allowed_fields:
        raise ValueError(f"Unsupported group field: {field}")
    ymd_start = _norm_ymd(start_date)
    ymd_end = _norm_ymd(end_date)
    params: List = [ymd_start, ymd_end, username]
    where = [f"({_DATE_EXPR_SQL}) >= ?", f"({_DATE_EXPR_SQL}) <= ?", "username = ?"]
    for k, v in (filters or {}).items():
        if k in allowed_fields:
            where.append(f"{k} = ?")
            params.append("" if v is None else str(v))
    where_clause = " AND ".join(where)
    with get_connection(db_path) as conn:
        query = f"""
            SELECT {field} as group_name,
                   COUNT(*) as count,
                   COUNT(DISTINCT chequeid) as cheque_count,
                   SUM(price) as total
            FROM purchases
            WHERE {where_clause} AND {field} IS NOT NULL
            GROUP BY {field}
            ORDER BY total DESC
        """
        logger.info(
            "SQL[get_grouped_stats_filtered]: %s | params=%s",
            " ".join(query.split()),
            params,
        )
        cur = conn.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in rows]
        for r in results:
            r["total"] = round(r.get("total") or 0.0, 2)
        return results


def add_item_to_cheque(chequeid: int, product_name: str, price: float, username: str, quantity: float = 1.0, discount: float = 0.0, db_path: Optional[str] = None) -> int:
    if not product_name or price is None:
        raise ValueError("product_name and price are required")
    with get_connection(db_path) as conn:
        # try to inherit date/organization/file_path from existing rows of the cheque
        cur = conn.execute(
            "SELECT date, organization, file_path FROM purchases WHERE chequeid = ? AND username = ? ORDER BY id DESC LIMIT 1",
            (chequeid, username)
        )
        row = cur.fetchone()
        if row:
            date_val, organization, file_path = row
        else:
            date_val = datetime.now().strftime("%d.%m.%Y")
            organization = None
            file_path = None
        cursor = conn.cursor()
        cursor.execute(
            (
                "INSERT INTO purchases (chequeid, file_path, date, created_at, product_name, quantity, price, discount, "
                "category1, category2, category3, organization, username, description) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            (
                chequeid,
                file_path,
                date_val,
                datetime.now().isoformat(),
                product_name,
                float(quantity or 1),
                float(price),
                float(discount or 0.0),
                None,
                None,
                None,
                organization,
                username,
                None,
            ),
        )
        conn.commit()
        return cursor.lastrowid


def delete_cheque(chequeid: int, username: str, db_path: Optional[str] = None) -> Tuple[int, Optional[str]]:
    with get_connection(db_path) as conn:
        cur = conn.execute(
            "SELECT file_path FROM purchases WHERE chequeid = ? AND username = ? LIMIT 1",
            (chequeid, username)
        )
        row = cur.fetchone()
        file_path = row[0] if row and row[0] else None
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM purchases WHERE chequeid = ? AND username = ?",
            (chequeid, username)
        )
        conn.commit()
        return cursor.rowcount, file_path

