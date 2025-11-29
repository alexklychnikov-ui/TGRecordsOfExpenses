"""Telegram bot logic and command routing."""
import os
import sys
import json
import logging
import re
from datetime import datetime, timezone, timedelta
import asyncio
from typing import Optional, Tuple, List, Dict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    Message,
    FSInputFile,
    BufferedInputFile,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from aiogram import F

from config import TELEGRAM_BOT_TOKEN, CHEQUE_DIR, DB_DIR, OPENAI_API_KEY
from db.db_manager import init_db, get_next_cheque_id, bulk_insert_purchases, check_duplicate_cheque
from parser.cheque_parser import parse_cheque_with_gpt
from parser.parse_receipt import extract_receipt_text
from openai import OpenAI

from aiAssistant.core.context_manager import ContextManager
from aiAssistant.core.ai_client import AIClient
from aiAssistant.core.date_helpers import (
    get_last_n_days,
    get_current_week,
    get_current_month,
    get_yesterday,
    get_previous_month,
    get_previous_year,
    normalize_to_current_month_if_same_month_wrong_year,
    _parse_ddmmyyyy,
    parse_period_string,
)
from aiAssistant.db import db_manager as ai_db
from aiAssistant.reports.report_builder import ReportBuilder
from Export2Excel.exporter import export_to_excel, export_grouped_to_excel, _export_filtered_to_excel
from aiAssistant.charts.chart_builder import create_pie_chart
from aiAssistent_economy import (
    should_handle_economy_request,
    process_economy_request,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

logging.getLogger("aiogram.dispatcher").setLevel(logging.CRITICAL)
logging.getLogger("aiogram.event").setLevel(logging.CRITICAL)
logging.getLogger("httpx").setLevel(logging.WARNING)

context_manager = ContextManager()
ai_client = AIClient()
report_builder = ReportBuilder()

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()


def _normalize_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    base = str(value).strip()
    if not base:
        return ""
    candidates = {base}
    conversions = [
        ("latin1", "utf-8"),
        ("latin1", "cp1251"),
        ("cp1251", "utf-8"),
        ("utf-8", "cp1251"),
    ]
    for enc, dec in conversions:
        try:
            converted = base.encode(enc, errors="ignore").decode(dec, errors="ignore")
            if converted:
                candidates.add(converted)
        except Exception:
            continue
    try:
        raw_bytes = bytes([ord(ch) & 0xFF for ch in base])
        decoded = raw_bytes.decode("cp1251", errors="ignore")
        if decoded:
            candidates.add(decoded)
    except Exception:
        pass
    def score(text: str) -> int:
        return sum(0x0400 <= ord(ch) <= 0x04FF for ch in text)
    best = max(candidates, key=score)
    return best.lower()


def _normalize_date_token(token: str) -> Optional[str]:
    cleaned = token.replace("/", ".").replace("-", ".").strip()
    parts = cleaned.split(".")
    if len(parts) != 3:
        return None
    day, month, year = parts
    if len(year) == 2:
        year = f"20{year}"
    try:
        dt = datetime(int(year), int(month), int(day))
        return dt.strftime("%d.%m.%Y")
    except ValueError:
        return None


def extract_period_from_message(message: str) -> Tuple[Optional[str], Optional[str]]:
    text = (message or "").strip()
    if not text:
        return None, None
    
    range_match = re.search(
        r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})[^0-9]{0,10}(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})",
        text,
        flags=re.IGNORECASE,
    )
    if range_match:
        start_raw, end_raw = range_match.groups()
        start_norm = _normalize_date_token(start_raw)
        end_norm = _normalize_date_token(end_raw)
        if start_norm and end_norm:
            return start_norm, end_norm
    
    single_match = re.search(
        r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\b",
        text,
        flags=re.IGNORECASE,
    )
    if single_match:
        date_norm = _normalize_date_token(single_match.group(1))
        if date_norm:
            return date_norm, date_norm
    
    parsed = parse_period_string(text)
    if parsed:
        return parsed
    
    return None, None


def resolve_period_for_message(user_id: int, user_message: str) -> Tuple[str, str]:
    detected_start, detected_end = extract_period_from_message(user_message)
    if detected_start and detected_end:
        return detected_start, detected_end
    
    last_query = context_manager.get_last_query(user_id)
    if last_query:
        params = last_query.get("params") or {}
        last_start = params.get("start_date")
        last_end = params.get("end_date")
        if last_start and last_end:
            return last_start, last_end
    
    return get_current_month()


def aggregate_category2_by_category1(records: list[dict], category1_value: str) -> list[dict]:
    target = _normalize_text(category1_value)
    grouped: dict[str, dict] = {}
    for item in records:
        if _normalize_text(item.get("category1")) != target:
            continue
        group_name = (item.get("category2") or "Без категории2").strip()
        bucket = grouped.setdefault(
            group_name,
            {"group_name": group_name, "count": 0, "total": 0.0, "cheque_ids": set()},
        )
        bucket["count"] += 1
        try:
            bucket["total"] += float(item.get("price") or 0.0)
        except Exception:
            pass
        chequeid = item.get("chequeid")
        if chequeid is not None:
            bucket["cheque_ids"].add(chequeid)
    result = []
    for data in grouped.values():
        result.append(
            {
                "group_name": data["group_name"],
                "count": data["count"],
                "cheque_count": len(data["cheque_ids"]),
                "total": round(data["total"], 2),
            }
        )
    result.sort(key=lambda x: x["total"], reverse=True)
    return result


def ensure_dirs() -> None:
    os.makedirs(CHEQUE_DIR, exist_ok=True)
    os.makedirs(DB_DIR, exist_ok=True)


def get_user_cheque_dir(username: Optional[str] = None, chat_id: Optional[int] = None) -> str:
    if username:
        safe_username = username.replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace('"', "_").replace("<", "_").replace(">", "_").replace("|", "_")
        user_dir = os.path.join(CHEQUE_DIR, safe_username)
    elif chat_id:
        user_dir = os.path.join(CHEQUE_DIR, f"user_{chat_id}")
    else:
        user_dir = os.path.join(CHEQUE_DIR, "unknown")
    os.makedirs(user_dir, exist_ok=True)
    return user_dir


SAVE_CALLBACK = "cheque_save"
DELETE_CALLBACK = "cheque_delete"
RETRY_CALLBACK = "cheque_retry"
EDIT_ITEM_PREFIX = "edit_item_"
EDIT_FIELD_PREFIX = "edit_field_"
DELETE_ITEM_PREFIX = "delete_item_"
BACK_TO_CHEQUE = "back_to_cheque"
ADD_ITEM_FIELD_PREFIX = "add_item_field_"
CANCEL_ADD_ITEM = "cancel_add_item"
NEW_CHEQUE_ORG_PREFIX = "new_cheque_org_"
NEW_CHEQUE_DATE_PREFIX = "new_cheque_date_"
NEW_CHEQUE_DATE_TODAY = "new_cheque_date_today"
NEW_CHEQUE_DATE_YESTERDAY = "new_cheque_date_yesterday"
NEW_CHEQUE_DATE_CUSTOM = "new_cheque_date_custom"


def build_pending_actions_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💾 Сохранить", callback_data=SAVE_CALLBACK),
                InlineKeyboardButton(text="🗑️ Удалить", callback_data=DELETE_CALLBACK),
            ],
            [
                InlineKeyboardButton(
                    text="❌ Не верно. Сделать по-другому", callback_data=RETRY_CALLBACK
                )
            ],
        ]
    )


def build_cheque_items_keyboard(items: List[Dict]) -> InlineKeyboardMarkup:
    """Создает клавиатуру с кнопками редактирования для каждой позиции."""
    keyboard = []
    
    # Кнопки редактирования для каждой позиции
    for idx in range(len(items)):
        keyboard.append([
            InlineKeyboardButton(
                text=f"✏️ Позиция {idx + 1}",
                callback_data=f"{EDIT_ITEM_PREFIX}{idx}"
            )
        ])
    
    # Кнопки действий с чеком
    keyboard.append([
        InlineKeyboardButton(text="💾 Сохранить чек", callback_data=SAVE_CALLBACK),
        InlineKeyboardButton(text="🗑️ Удалить чек", callback_data=DELETE_CALLBACK),
    ])
    keyboard.append([
        InlineKeyboardButton(
            text="❌ Не верно. Сделать по-другому",
            callback_data=RETRY_CALLBACK
        )
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def build_cheque_actions_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру только с кнопками действий для чека (без кнопок позиций)."""
    keyboard = [
        [
            InlineKeyboardButton(text="💾 Сохранить чек", callback_data=SAVE_CALLBACK),
            InlineKeyboardButton(text="🗑️ Удалить чек", callback_data=DELETE_CALLBACK),
        ],
        [
            InlineKeyboardButton(
                text="❌ Не верно. Сделать по-другому",
                callback_data=RETRY_CALLBACK
            )
        ]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def build_add_item_keyboard(add_state: Optional[Dict] = None) -> InlineKeyboardMarkup:
    """Создает клавиатуру для добавления новой позиции."""
    product_name = add_state.get("product_name", "") if add_state else ""
    price = add_state.get("price") if add_state else None
    
    keyboard = [
        [InlineKeyboardButton(
            text=f"✏️ Название товара: {product_name[:20] if product_name else '—'}",
            callback_data=f"{ADD_ITEM_FIELD_PREFIX}product_name"
        )],
        [InlineKeyboardButton(
            text=f"💰 Цена товара: {price:.2f} ₽" if price is not None else "💰 Цена товара: —",
            callback_data=f"{ADD_ITEM_FIELD_PREFIX}price"
        )],
        [InlineKeyboardButton(
            text="❌ Отмена",
            callback_data=CANCEL_ADD_ITEM
        )],
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def build_edit_item_keyboard(item_index: int, item: Dict) -> InlineKeyboardMarkup:
    """Создает клавиатуру для редактирования конкретной позиции."""
    price = float(item.get("price", 0) or 0)
    quantity = float(item.get("quantity", 1) or 1)
    description = item.get("description") or ""
    
    keyboard = [
        [InlineKeyboardButton(
            text=f"💰 Цена: {price:.2f} ₽",
            callback_data=f"{EDIT_FIELD_PREFIX}{item_index}_price"
        )],
        [InlineKeyboardButton(
            text=f"🔢 Количество: {quantity} шт.",
            callback_data=f"{EDIT_FIELD_PREFIX}{item_index}_quantity"
        )],
        [InlineKeyboardButton(
            text="✏️ Название",
            callback_data=f"{EDIT_FIELD_PREFIX}{item_index}_product_name"
        )],
        [InlineKeyboardButton(
            text="🏷️ Категория",
            callback_data=f"{EDIT_FIELD_PREFIX}{item_index}_category1"
        )],
        [InlineKeyboardButton(
            text=f"📝 Описание: {description[:20] if description else '—'}",
            callback_data=f"{EDIT_FIELD_PREFIX}{item_index}_description"
        )],
        [InlineKeyboardButton(
            text="❌ Удалить позицию",
            callback_data=f"{DELETE_ITEM_PREFIX}{item_index}"
        )],
        [InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=BACK_TO_CHEQUE
        )],
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def build_new_cheque_setup_keyboard(new_cheque_state: Optional[Dict] = None) -> InlineKeyboardMarkup:
    """Создает клавиатуру для настройки нового чека (организация и дата)."""
    organization = new_cheque_state.get("organization", "") if new_cheque_state else ""
    date = new_cheque_state.get("date", "") if new_cheque_state else ""
    
    keyboard = [
        [InlineKeyboardButton(
            text=f"🏢 Организация: {organization[:25] if organization else '—'}",
            callback_data=f"{NEW_CHEQUE_ORG_PREFIX}set"
        )],
        [InlineKeyboardButton(
            text=f"📅 Дата чека: {date if date else '—'}",
            callback_data=f"{NEW_CHEQUE_DATE_PREFIX}select"
        )],
    ]
    
    # Если организация и дата заполнены, показываем кнопку "Начать добавление позиций"
    if organization and date:
        keyboard.append([InlineKeyboardButton(
            text="✅ Начать добавление позиций",
            callback_data="new_cheque_start_add"
        )])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def build_new_cheque_date_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру для выбора даты чека."""
    today = datetime.now().strftime("%d.%m.%Y")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")
    
    keyboard = [
        [InlineKeyboardButton(
            text=f"📅 Сегодня ({today})",
            callback_data=NEW_CHEQUE_DATE_TODAY
        )],
        [InlineKeyboardButton(
            text=f"📅 Вчера ({yesterday})",
            callback_data=NEW_CHEQUE_DATE_YESTERDAY
        )],
        [InlineKeyboardButton(
            text="✏️ Ввести вручную",
            callback_data=NEW_CHEQUE_DATE_CUSTOM
        )],
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def build_new_cheque_actions_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру с кнопками управления новым чеком."""
    keyboard = [
        [
            InlineKeyboardButton(text="💾 Сохранить чек", callback_data=SAVE_CALLBACK),
            InlineKeyboardButton(text="➕ Добавить позицию", callback_data="new_cheque_add_item")
        ],
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def discard_pending_cheque(user_id: int, remove_file: bool = True) -> None:
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        return
    if remove_file:
        file_path = pending.get("file_path")
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as exc:
                logger.warning(f"Failed to remove pending cheque file {file_path}: {exc}")
    context_manager.clear_pending_cheque(user_id)


def prepare_pending_cheque(user_id: int, username: str, local_path: str, items: list) -> tuple[int, list, str, float]:
    chequeid = get_next_cheque_id()
    now_iso = datetime.now(timezone.utc).isoformat()
    processed_items = []
    for item in items:
        processed = dict(item)
        processed["chequeid"] = chequeid
        processed["file_path"] = local_path
        processed.setdefault("created_at", now_iso)
        processed.setdefault("username", username)
        quantity = processed.get("quantity", 1)
        try:
            processed["quantity"] = float(quantity) if quantity not in (None, "") else 1
        except Exception:
            processed["quantity"] = 1
        price = processed.get("price", 0)
        try:
            processed["price"] = float(price or 0)
        except Exception:
            processed["price"] = 0.0
        discount = processed.get("discount", 0)
        try:
            processed["discount"] = float(discount or 0)
        except Exception:
            processed["discount"] = 0.0
        processed_items.append(processed)
    
    existing = context_manager.get_pending_cheque(user_id)
    if existing:
        if existing.get("file_path") != local_path:
            discard_pending_cheque(user_id, remove_file=True)
        else:
            context_manager.clear_pending_cheque(user_id)
    
    context_manager.set_pending_cheque(
        user_id,
        {
            "items": processed_items,
            "file_path": local_path,
            "username": username,
            "chequeid": chequeid,
            "created_at": now_iso,
        },
    )
    
    preview_text = report_builder.format_cheque(processed_items)
    total_sum = sum(item.get("price", 0.0) for item in processed_items)
    return chequeid, processed_items, preview_text, total_sum


def _should_refresh_cache(user_message: str) -> bool:
    """
    Проверяет, нужно ли обновить кеш на основе ключевых слов в сообщении.
    
    Args:
        user_message: Сообщение пользователя
    
    Returns:
        True если нужно игнорировать кеш и делать новый запрос
    """
    if not user_message:
        return False
    
    refresh_keywords = ["пересчитай", "обнови", "заново", "снова", "пересчитать", "обновить", "refresh", "recalculate"]
    user_lower = user_message.lower()
    
    return any(keyword in user_lower for keyword in refresh_keywords)


def refresh_last_query(user_id: int, username: str, context_manager: ContextManager) -> str:
    """
    Обновляет последний запрос: берет параметры из кеша, выполняет запрос заново в БД и обновляет результат в кеше.
    
    Args:
        user_id: ID пользователя
        username: Username пользователя для БД
        context_manager: Менеджер контекста
    
    Returns:
        Текстовое сообщение для пользователя
    """
    last_query = context_manager.get_last_query(user_id)
    
    # Если запроса нет в кеше, используем запрос по умолчанию "за текущий месяц"
    if not last_query:
        start_date, end_date = get_current_month()
        result = ai_db.get_grouped_stats("category1", start_date, end_date, username)
        context_manager.set_last_query(
            user_id,
            "get_grouped_by_category1",
            {"start_date": start_date, "end_date": end_date, "field": "category1"},
            result,
            username,
        )
        return f"✅ Обновлен запрос по умолчанию (группировка по категориям за текущий месяц). Найдено групп: {len(result)}"
    
    query_type = last_query.get("type", "")
    params = last_query.get("params", {})
    query_username = last_query.get("username", username)
    
    result = []
    message = ""
    
    # Обработка различных типов запросов
    if query_type.startswith("get_grouped_by_"):
        # Определяем поле для группировки
        field_map = {
            "get_grouped_by_category1": "category1",
            "get_grouped_by_category2": "category2",
            "get_grouped_by_category3": "category3",
            "get_grouped_by_organization": "organization",
            "get_grouped_by_description": "description"
        }
        field = field_map.get(query_type, params.get("field", "category1"))
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        
        if not start_date or not end_date:
            start_date, end_date = get_current_month()
        
        result = ai_db.get_grouped_stats(field, start_date, end_date, query_username)
        context_manager.set_last_query(
            user_id,
            query_type,
            {"start_date": start_date, "end_date": end_date, "field": field},
            result,
            query_username,
        )
        message = f"✅ Обновлен запрос группировки по '{field}' за период {start_date} - {end_date}. Найдено групп: {len(result)}"
    
    elif query_type == "fetch_by_period":
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        
        if not start_date or not end_date:
            start_date, end_date = get_current_month()
        
        result = ai_db.fetch_by_period(start_date, end_date, query_username)
        context_manager.set_last_query(
            user_id,
            "fetch_by_period",
            {"start_date": start_date, "end_date": end_date},
            result,
            query_username,
        )
        message = f"✅ Обновлен запрос за период {start_date} - {end_date}. Найдено записей: {len(result)}"
    
    elif query_type == "summary_period":
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        
        if not start_date or not end_date:
            start_date, end_date = get_current_month()
        
        result = ai_db.get_summary(start_date, end_date, query_username)
        context_manager.set_last_query(
            user_id,
            "summary_period",
            {"start_date": start_date, "end_date": end_date},
            result,
            query_username,
        )
        message = f"✅ Обновлена сводка за период {start_date} - {end_date}. Найдено записей: {len(result) if isinstance(result, list) else 1}"
    
    else:
        # Для неизвестных типов запросов используем запрос по умолчанию
        start_date, end_date = get_current_month()
        result = ai_db.get_grouped_stats("category1", start_date, end_date, query_username)
        context_manager.set_last_query(
            user_id,
            "get_grouped_by_category1",
            {"start_date": start_date, "end_date": end_date, "field": "category1"},
            result,
            query_username,
        )
        message = f"✅ Обновлен запрос (тип '{query_type}' не поддерживается, использован запрос по умолчанию). Найдено групп: {len(result)}"
    
    return message


def execute_tool_call(tool_name: str, arguments: dict, username: str, user_id: int, user_message: str = "", need_excel: bool = False, need_chart: bool = False) -> tuple[str, list, dict]:
    try:
        if "username" not in arguments:
            arguments["username"] = username
        
        
        photos_to_send = []
        extra_outputs = {
            "excel_path": None,
            "chart_data": None,
            "chart_field": None
        }
        
        def normalize_period_to_current_month(start_date: str, end_date: str) -> tuple[str, str]:
            """Нормализует период к текущему месяцу, если месяц/год не совпадают."""
            if not start_date or not end_date:
                return get_current_month()
            
            from datetime import datetime
            now = datetime.now()
            current_month = now.month
            current_year = now.year
            
            ds = _parse_ddmmyyyy(start_date)
            de = _parse_ddmmyyyy(end_date)
            
            # Если месяц или год не совпадают с текущими - исправляем на текущий месяц
            if ds and de:
                if ds.month != current_month or de.month != current_month or ds.year != current_year or de.year != current_year:
                    corrected = get_current_month()
                    return corrected
            
            return normalize_to_current_month_if_same_month_wrong_year(start_date, end_date)
        
        if tool_name == "get_last_n_days":
            n = arguments.get("n", 7)
            start_date, end_date = get_last_n_days(n)
            result = ai_db.fetch_by_period(start_date, end_date, username)
            summary = f"📅 За последние {n} дней ({start_date} - {end_date}):\n\n"
            text = summary + report_builder.format_purchases_list(result)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Report_{user_id}.xlsx")
                from config import DB_PATH
                export_to_excel(DB_PATH, output_path, username, start_date, end_date)
                extra_outputs["excel_path"] = output_path
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_current_week":
            start_date, end_date = get_current_week()
            result = ai_db.fetch_by_period(start_date, end_date, username)
            summary = f"📅 За текущую неделю ({start_date} - {end_date}):\n\n"
            text = summary + report_builder.format_purchases_list(result)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Report_{user_id}.xlsx")
                from config import DB_PATH
                export_to_excel(DB_PATH, output_path, username, start_date, end_date)
                extra_outputs["excel_path"] = output_path
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_current_month":
            start_date, end_date = get_current_month()
            result = ai_db.fetch_by_period(start_date, end_date, username)
            summary = f"📅 За текущий месяц ({start_date} - {end_date}):\n\n"
            text = summary + report_builder.format_purchases_list(result)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Report_{user_id}.xlsx")
                from config import DB_PATH
                export_to_excel(DB_PATH, output_path, username, start_date, end_date)
                extra_outputs["excel_path"] = output_path
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_yesterday":
            start_date, end_date = get_yesterday()
            result = ai_db.fetch_by_period(start_date, end_date, username)
            context_manager.set_last_query(
                user_id,
                "fetch_by_period",
                {"start_date": start_date, "end_date": end_date},
                result,
                username,
            )
            summary = f"📅 За вчера ({start_date}):\n\n"
            text = "" if (need_excel or need_chart) else summary + report_builder.format_purchases_list(result)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Report_{user_id}.xlsx")
                from config import DB_PATH
                export_to_excel(DB_PATH, output_path, username, start_date, end_date)
                extra_outputs["excel_path"] = output_path
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_previous_month":
            start_date, end_date = get_previous_month()
            result = ai_db.fetch_by_period(start_date, end_date, username)
            context_manager.set_last_query(
                user_id,
                "fetch_by_period",
                {"start_date": start_date, "end_date": end_date},
                result,
                username,
            )
            summary = f"📅 За прошлый месяц ({start_date} - {end_date}):\n\n"
            text = "" if (need_excel or need_chart) else summary + report_builder.format_purchases_list(result)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Report_{user_id}.xlsx")
                from config import DB_PATH
                export_to_excel(DB_PATH, output_path, username, start_date, end_date)
                extra_outputs["excel_path"] = output_path
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_previous_year":
            start_date, end_date = get_previous_year()
            result = ai_db.fetch_by_period(start_date, end_date, username)
            context_manager.set_last_query(
                user_id,
                "fetch_by_period",
                {"start_date": start_date, "end_date": end_date},
                result,
                username,
            )
            summary = f"📅 За прошлый год ({start_date} - {end_date}):\n\n"
            text = "" if (need_excel or need_chart) else summary + report_builder.format_purchases_list(result)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Report_{user_id}.xlsx")
                from config import DB_PATH
                export_to_excel(DB_PATH, output_path, username, start_date, end_date)
                extra_outputs["excel_path"] = output_path
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "fetch_by_period":
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            result = ai_db.fetch_by_period(start_date, end_date, username)
            context_manager.set_last_query(
                user_id,
                "fetch_by_period",
                {"start_date": start_date, "end_date": end_date},
                result,
                username,
            )
            text = "" if (need_excel or need_chart) else report_builder.format_purchases_list(result)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Report_{user_id}.xlsx")
                from config import DB_PATH
                export_to_excel(DB_PATH, output_path, username, start_date, end_date)
                extra_outputs["excel_path"] = output_path
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_summary_last_n_days":
            n = arguments.get("n", 7)
            if n == 1:
                start_date, end_date = get_yesterday()
            else:
                start_date, end_date = get_last_n_days(n)
            result = ai_db.get_summary(start_date, end_date, username)
            summary = f"📅 За последние {n} дней ({start_date} - {end_date}):\n\n"
            context_manager.set_last_query(
                user_id,
                "summary_period",
                {"start_date": start_date, "end_date": end_date},
                result,
                username,
            )
            text = "" if (need_excel or need_chart) else summary + report_builder.format_summary(result)
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_summary_week":
            start_date, end_date = get_current_week()
            result = ai_db.get_summary(start_date, end_date, username)
            summary = f"📅 За текущую неделю ({start_date} - {end_date}):\n\n"
            context_manager.set_last_query(
                user_id,
                "summary_period",
                {"start_date": start_date, "end_date": end_date},
                result,
                username,
            )
            text = "" if (need_excel or need_chart) else summary + report_builder.format_summary(result)
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_summary_month":
            start_date, end_date = get_current_month()
            result = ai_db.get_summary(start_date, end_date, username)
            summary = f"📅 За текущий месяц ({start_date} - {end_date}):\n\n"
            context_manager.set_last_query(
                user_id,
                "summary_period",
                {"start_date": start_date, "end_date": end_date},
                result,
                username,
            )
            text = "" if (need_excel or need_chart) else summary + report_builder.format_summary(result)
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_summary":
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            result = ai_db.get_summary(start_date, end_date, username)
            context_manager.set_last_query(
                user_id,
                "summary_period",
                {"start_date": start_date, "end_date": end_date},
                result,
                username,
            )
            text = "" if (need_excel or need_chart) else report_builder.format_summary(result)
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_cheque_by_id":
            result = ai_db.get_cheque_by_id(**arguments)
            if result:
                chequeid = result[0].get("chequeid")
                if chequeid:
                    context_manager.set_last_cheque(user_id, chequeid)
                if result[0].get("file_path"):
                    photos_to_send.append(result[0]["file_path"])
            text = report_builder.format_cheque(result)
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_last_cheque":
            result = ai_db.get_last_cheque(**arguments)
            if result:
                chequeid = result[0].get("chequeid")
                if chequeid:
                    context_manager.set_last_cheque(user_id, chequeid)
                if result[0].get("file_path"):
                    photos_to_send.append(result[0]["file_path"])
            text = report_builder.format_cheque(result)
            return text, photos_to_send, extra_outputs

        elif tool_name == "delete_cheque":
            chequeid = arguments.get("chequeid")
            if not chequeid:
                chequeid = context_manager.get_last_cheque(user_id)
            if not chequeid:
                chequeid = ai_db.get_max_chequeid(username)
            if not chequeid:
                return "", photos_to_send, extra_outputs
            rows, file_path = ai_db.delete_cheque(chequeid, username)
            if rows > 0 and file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as _:
                    pass
            if rows > 0:
                return f"✅ Удалено записей: {rows}", photos_to_send, extra_outputs
            return "", photos_to_send, extra_outputs
        
        elif tool_name == "add_item_to_cheque":
            chequeid = arguments.get("chequeid")
            if not chequeid:
                chequeid = context_manager.get_last_cheque(user_id)
            if not chequeid:
                chequeid = ai_db.get_max_chequeid(username)
            if not chequeid:
                return "", photos_to_send, extra_outputs
            product_name = arguments.get("product_name")

            def to_float(val, default=0.0):
                if val is None:
                    return default
                try:
                    if isinstance(val, (int, float)):
                        return float(val)
                    return float(str(val).replace(" ", "").replace(",", "."))
                except Exception:
                    return default

            price = to_float(arguments.get("price"), 0.0)
            quantity = to_float(arguments.get("quantity", 1.0), 1.0)
            discount = to_float(arguments.get("discount", 0.0), 0.0)
            try:
                record_id = ai_db.add_item_to_cheque(
                    chequeid=chequeid,
                    product_name=product_name,
                    price=price,
                    username=username,
                    quantity=quantity,
                    discount=discount
                )
                return f"✅ Добавлена позиция в чек {chequeid}: {product_name}, цена {price} ₽", photos_to_send, extra_outputs
            except ValueError as e:
                return f"❌ Ошибка: {str(e)}", photos_to_send, extra_outputs
            except Exception as e:
                logger.error(f"Error adding item to cheque: {e}")
                return f"❌ Ошибка добавления позиции: {str(e)}", photos_to_send, extra_outputs
        
        elif tool_name == "fetch_by_category":
            result = ai_db.fetch_by_category(**arguments)
            text = report_builder.format_purchases_list(result)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Report_{user_id}.xlsx")
                from config import DB_PATH
                export_to_excel(DB_PATH, output_path, username)
                extra_outputs["excel_path"] = output_path
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "fetch_by_organization":
            result = ai_db.fetch_by_organization(**arguments)
            text = report_builder.format_purchases_list(result)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Report_{user_id}.xlsx")
                from config import DB_PATH
                export_to_excel(DB_PATH, output_path, username)
                extra_outputs["excel_path"] = output_path
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "fetch_by_product_name":
            result = ai_db.fetch_by_product_name(**arguments)
            text = report_builder.format_purchases_list(result)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Report_{user_id}.xlsx")
                from config import DB_PATH
                export_to_excel(DB_PATH, output_path, username)
                extra_outputs["excel_path"] = output_path
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "fetch_by_description":
            result = ai_db.fetch_by_description(**arguments)
            text = report_builder.format_purchases_list(result)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Report_{user_id}.xlsx")
                from config import DB_PATH
                export_to_excel(DB_PATH, output_path, username)
                extra_outputs["excel_path"] = output_path
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "update_description_by_cheque":
            chequeid = arguments.get("chequeid")
            if not chequeid:
                chequeid = context_manager.get_last_cheque(user_id)
            if not chequeid:
                chequeid = ai_db.get_max_chequeid(username)
            if not chequeid:
                return "", photos_to_send, extra_outputs
            arguments["chequeid"] = chequeid
            rows = ai_db.update_description_by_cheque(**arguments)
            if rows > 0:
                return report_builder.format_update_result(True, rows), photos_to_send, extra_outputs
            return "", photos_to_send, extra_outputs
        
        elif tool_name == "update_description_by_organization":
            rows = ai_db.update_description_by_organization(**arguments)
            return report_builder.format_update_result(True, rows), photos_to_send, extra_outputs
        
        elif tool_name == "update_record":
            safe_args = {k: arguments[k] for k in ("record_id", "field", "value") if k in arguments}
            # normalize numeric values like '123,45' -> '123.45'
            try:
                field = safe_args.get("field")
                val = safe_args.get("value")
                if isinstance(val, str) and field in {"price", "discount", "quantity"}:
                    v = val.replace(" ", "").replace(",", ".")
                    safe_args["value"] = v
            except Exception:
                pass
            # First try: update by internal record ID
            success = ai_db.update_record(**safe_args)
            if success:
                return report_builder.format_update_result(True, 1), photos_to_send, extra_outputs
            
            # Fallback: treat record_id as position number in the last viewed cheque
            try:
                position_num = int(safe_args.get("record_id")) if safe_args.get("record_id") is not None else None
            except Exception:
                position_num = None
            
            if position_num and position_num > 0:
                # Get last viewed cheque for this user
                last_chequeid = context_manager.get_last_cheque(user_id)
                if not last_chequeid:
                    # Try to get max chequeid as fallback
                    last_chequeid = ai_db.get_max_chequeid(username)
                
                if last_chequeid:
                    # Get all records from the cheque
                    cheque_records = ai_db.get_cheque_by_id(last_chequeid, username)
                    if cheque_records and len(cheque_records) >= position_num:
                        # Position numbers are 1-based, so subtract 1 for index
                        target_record = cheque_records[position_num - 1]
                        record_id = target_record.get("id")
                        if record_id:
                            # Update the specific record by its internal ID
                            success = ai_db.update_record(record_id=record_id, field=safe_args.get("field"), value=safe_args.get("value"))
                            if success:
                                return report_builder.format_update_result(True, 1), photos_to_send, extra_outputs
            
            return report_builder.format_update_result(False, 0), photos_to_send, extra_outputs
        
        elif tool_name == "update_field_by_cheque":
            chequeid = arguments.get("chequeid")
            if not chequeid:
                chequeid = context_manager.get_last_cheque(user_id)
            if not chequeid:
                chequeid = ai_db.get_max_chequeid(username)
            if not chequeid:
                return "", photos_to_send, extra_outputs
            field = arguments.get("field")
            value = arguments.get("value")
            rows = ai_db.update_field_by_cheque(chequeid=chequeid, field=field, value=value, username=username)
            if rows > 0:
                return report_builder.format_update_result(True, rows), photos_to_send, extra_outputs
            return "", photos_to_send, extra_outputs
        
        elif tool_name == "get_grouped_by_category1":
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            if start_date and end_date:
                start_date, end_date = normalize_period_to_current_month(start_date, end_date)
            else:
                start_date, end_date = resolve_period_for_message(user_id, user_message)
            result = []
            should_refresh = _should_refresh_cache(user_message)
            last_query = context_manager.get_last_query(user_id)
            if (
                not should_refresh
                and last_query
                and last_query.get("type") == "get_grouped_by_category1"
                and last_query.get("params", {}).get("start_date") == start_date
                and last_query.get("params", {}).get("end_date") == end_date
            ):
                result = last_query.get("result", [])
            if not result:
                result = ai_db.get_grouped_stats("category1", start_date, end_date, username)
            
            context_manager.set_last_query(user_id, "get_grouped_by_category1", 
                                          {"start_date": start_date, "end_date": end_date, "field": "category1"}, 
                                          result, username)
            
            # Если запрошен график/Excel, не выводим текстовый ответ
            text = "" if (need_chart or need_excel) else report_builder.format_grouped_stats(result, "category1")
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Grouped_{user_id}.xlsx")
                export_grouped_to_excel(result, output_path, "category1")
                extra_outputs["excel_path"] = output_path
            if need_chart and result:
                extra_outputs["chart_data"] = result
                extra_outputs["chart_field"] = "category1"
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_grouped_by_category2":
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            if start_date and end_date:
                start_date, end_date = normalize_period_to_current_month(start_date, end_date)
            else:
                start_date, end_date = resolve_period_for_message(user_id, user_message)
            result = []
            should_refresh = _should_refresh_cache(user_message)
            last_query = context_manager.get_last_query(user_id)
            if (
                not should_refresh
                and last_query
                and last_query.get("type") == "get_grouped_by_category2"
                and last_query.get("params", {}).get("start_date") == start_date
                and last_query.get("params", {}).get("end_date") == end_date
            ):
                result = last_query.get("result", [])
            if not result:
                result = ai_db.get_grouped_stats("category2", start_date, end_date, username)
            
            context_manager.set_last_query(user_id, "get_grouped_by_category2", 
                                          {"start_date": start_date, "end_date": end_date, "field": "category2"}, 
                                          result, username)
            
            text = "" if (need_chart or need_excel) else report_builder.format_grouped_stats(result, "category2")
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Grouped_{user_id}.xlsx")
                export_grouped_to_excel(result, output_path, "category2")
                extra_outputs["excel_path"] = output_path
            if need_chart and result:
                extra_outputs["chart_data"] = result
                extra_outputs["chart_field"] = "category2"
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_grouped_by_category3":
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            if start_date and end_date:
                start_date, end_date = normalize_period_to_current_month(start_date, end_date)
            else:
                start_date, end_date = resolve_period_for_message(user_id, user_message)
            result = []
            should_refresh = _should_refresh_cache(user_message)
            last_query = context_manager.get_last_query(user_id)
            if (
                not should_refresh
                and last_query
                and last_query.get("type") == "get_grouped_by_category3"
                and last_query.get("params", {}).get("start_date") == start_date
                and last_query.get("params", {}).get("end_date") == end_date
            ):
                result = last_query.get("result", [])
            if not result:
                result = ai_db.get_grouped_stats("category3", start_date, end_date, username)
            
            context_manager.set_last_query(user_id, "get_grouped_by_category3", 
                                          {"start_date": start_date, "end_date": end_date, "field": "category3"}, 
                                          result, username)
            
            text = "" if (need_chart or need_excel) else report_builder.format_grouped_stats(result, "category3")
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Grouped_{user_id}.xlsx")
                export_grouped_to_excel(result, output_path, "category3")
                extra_outputs["excel_path"] = output_path
            if need_chart and result:
                extra_outputs["chart_data"] = result
                extra_outputs["chart_field"] = "category3"
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_grouped_by_organization":
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            if start_date and end_date:
                start_date, end_date = normalize_period_to_current_month(start_date, end_date)
            else:
                start_date, end_date = resolve_period_for_message(user_id, user_message)
            result = []
            should_refresh = _should_refresh_cache(user_message)
            last_query = context_manager.get_last_query(user_id)
            if (
                not should_refresh
                and last_query
                and last_query.get("type") == "get_grouped_by_organization"
                and last_query.get("params", {}).get("start_date") == start_date
                and last_query.get("params", {}).get("end_date") == end_date
            ):
                result = last_query.get("result", [])
            if not result:
                result = ai_db.get_grouped_stats("organization", start_date, end_date, username)
            
            context_manager.set_last_query(user_id, "get_grouped_by_organization", 
                                          {"start_date": start_date, "end_date": end_date, "field": "organization"}, 
                                          result, username)
            
            text = "" if (need_chart or need_excel) else report_builder.format_grouped_stats(result, "organization")
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Grouped_{user_id}.xlsx")
                export_grouped_to_excel(result, output_path, "organization")
                extra_outputs["excel_path"] = output_path
            if need_chart and result:
                extra_outputs["chart_data"] = result
                extra_outputs["chart_field"] = "organization"
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "get_grouped_by_description":
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            if start_date and end_date:
                start_date, end_date = normalize_period_to_current_month(start_date, end_date)
            else:
                start_date, end_date = resolve_period_for_message(user_id, user_message)
            result = []
            should_refresh = _should_refresh_cache(user_message)
            last_query = context_manager.get_last_query(user_id)
            if (
                not should_refresh
                and last_query
                and last_query.get("type") == "get_grouped_by_description"
                and last_query.get("params", {}).get("start_date") == start_date
                and last_query.get("params", {}).get("end_date") == end_date
            ):
                result = last_query.get("result", [])
            if not result:
                result = ai_db.get_grouped_stats("description", start_date, end_date, username)
            
            context_manager.set_last_query(user_id, "get_grouped_by_description", 
                                          {"start_date": start_date, "end_date": end_date, "field": "description"}, 
                                          result, username)
            
            text = "" if (need_chart or need_excel) else report_builder.format_grouped_stats(result, "description")
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Grouped_{user_id}.xlsx")
                export_grouped_to_excel(result, output_path, "description")
                extra_outputs["excel_path"] = output_path
            if need_chart and result:
                extra_outputs["chart_data"] = result
                extra_outputs["chart_field"] = "description"
            return text, photos_to_send, extra_outputs

        elif tool_name == "get_grouped_stats_filtered":
            field = arguments.get("field")
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            filters = arguments.get("filters", {})
            if start_date and end_date:
                start_date, end_date = normalize_period_to_current_month(start_date, end_date)
            else:
                start_date, end_date = resolve_period_for_message(user_id, user_message)
            result = []
            last_query = context_manager.get_last_query(user_id)
            if (
                last_query
                and last_query.get("type") == "get_grouped_stats_filtered"
                and last_query.get("params", {}).get("start_date") == start_date
                and last_query.get("params", {}).get("end_date") == end_date
                and last_query.get("params", {}).get("field") == field
                and last_query.get("params", {}).get("filters") == filters
            ):
                result = last_query.get("result", [])
            if not result:
                result = ai_db.get_grouped_stats_filtered(field, start_date, end_date, username, filters)
            
            context_manager.set_last_query(user_id, "get_grouped_stats_filtered", 
                                          {"start_date": start_date, "end_date": end_date, "field": field, "filters": filters}, 
                                          result, username)
            
            text = "" if (need_chart or need_excel) else report_builder.format_grouped_stats(result, field)
            if need_excel:
                output_path = os.path.join(DB_DIR, f"Grouped_{user_id}.xlsx")
                export_grouped_to_excel(result, output_path, field)
                extra_outputs["excel_path"] = output_path
            if need_chart and result:
                extra_outputs["chart_data"] = result
                extra_outputs["chart_field"] = field
            return text, photos_to_send, extra_outputs
        
        elif tool_name == "export_all_to_excel":
            output_path = os.path.join(DB_DIR, "Report.xlsx")
            db_path = os.path.join(os.path.dirname(os.path.dirname(PROJECT_ROOT)), ".dbData", "receipts.db") if False else None
            # use configured DB path inside aiAssistant db layer
            from config import DB_PATH
            export_to_excel(DB_PATH, output_path, username)
            extra_outputs["excel_path"] = output_path
            return f"✅ Выгрузка завершена: {output_path}", photos_to_send, extra_outputs

        elif tool_name == "export_to_excel_by_period":
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            output_path = os.path.join(DB_DIR, "Report.xlsx")
            from config import DB_PATH
            if start_date and end_date:
                start_date, end_date = normalize_to_current_month_if_same_month_wrong_year(start_date, end_date)
            export_to_excel(DB_PATH, output_path, username, start_date, end_date)
            extra_outputs["excel_path"] = output_path
            return f"✅ Выгрузка за период завершена: {output_path}", photos_to_send, extra_outputs
        
        elif tool_name == "export_group_items_to_excel":
            group_value = arguments.get("group_value")
            if not group_value:
                return "❌ Не указано значение группы для выгрузки", photos_to_send, extra_outputs
            
            last_query = context_manager.get_last_query(user_id)
            if not last_query:
                return "❌ Нет данных из предыдущего запроса. Сначала выполните запрос группировки.", photos_to_send, extra_outputs
            
            query_type = last_query.get("type", "")
            if not query_type.startswith("get_grouped_by"):
                return "❌ Последний запрос не был запросом группировки.", photos_to_send, extra_outputs
            
            # Определяем поле группировки
            field_map = {
                "get_grouped_by_category1": "category1",
                "get_grouped_by_category2": "category2",
                "get_grouped_by_category3": "category3",
                "get_grouped_by_organization": "organization",
                "get_grouped_by_description": "description"
            }
            field = field_map.get(query_type)
            if not field:
                field = last_query.get("params", {}).get("field")
            
            # Берем даты из кешированного запроса
            params = last_query.get("params", {})
            start_date = params.get("start_date")
            end_date = params.get("end_date")
            query_username = last_query.get("username", username)
            
            if not start_date or not end_date:
                return "❌ Не удалось определить период из предыдущего запроса.", photos_to_send, extra_outputs
            
            # Получаем детальные записи за период из кеша
            from config import DB_PATH
            result = ai_db.fetch_by_period(start_date, end_date, query_username, DB_PATH)
            
            # Фильтруем по значению группы (точное совпадение)
            group_value_norm = (group_value or "").strip().lower()
            filtered_result = [
                r for r in result if (r.get(field) or "").strip().lower() == group_value_norm
            ]
            
            if not filtered_result:
                return f"❌ Не найдено записей для группы '{group_value}' за период {start_date} - {end_date}", photos_to_send, extra_outputs
            
            # Создаем временный файл с отфильтрованными данными
            output_path = os.path.join(DB_DIR, f"GroupItems_{user_id}.xlsx")
            _export_filtered_to_excel(filtered_result, output_path)
            extra_outputs["excel_path"] = output_path
            return f"✅ Выгружено {len(filtered_result)} записей для '{group_value}' за период {start_date} - {end_date}: {output_path}", photos_to_send, extra_outputs
        
        else:
            return f"Функция {tool_name} не поддерживается", photos_to_send, extra_outputs
    
    except Exception as e:
        logger.error(f"Error executing tool {tool_name}: {e}")
        return f"Ошибка выполнения: {str(e)}", [], {}


@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    context_manager.clear_context(user_id)
    await message.answer(
        "Привет! Я твой AI-ассистент по финансам. 💰\n\n"
        "Задай вопрос или пришли фото чека. 📸\n\n"
        "Примеры команд:\n"
        "• Покажи последний чек\n"
        "• Покажи чек номер 5\n"
        "• Общая сумма за последние 7 дней\n"
        "• Статистика по категориям за октябрь\n"
        "• Добавь комментарий к чеку 12"
    )


@dp.message(Command("clear"))
async def cmd_clear(message: Message):
    user_id = message.from_user.id
    context_manager.clear_context(user_id)
    await message.answer("🔄 Контекст диалога очищен")


@dp.message(F.photo)
async def handle_photo(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"
    
    if message.caption and message.caption.startswith("📸"):
        return
    
    ensure_dirs()
    init_db()
    
    user_dir = get_user_cheque_dir(username, user_id)
    
    await message.answer("📥 Фото получено. ⏳ Идёт распознавание чека...")
    try:
        file = await asyncio.wait_for(bot.get_file(message.photo[-1].file_id), timeout=30)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        local_path = os.path.join(user_dir, f"cheque_{ts}.jpg")
        await asyncio.wait_for(bot.download_file(file.file_path, local_path), timeout=45)
    except asyncio.TimeoutError:
        await message.answer("⏰ Не удалось скачать фото от Telegram (таймаут)")
        return
    except Exception as e:
        await message.answer(f"⚠️ Ошибка скачивания фото: {e}")
        return
    try:
        logger.info("Start parse task (photo)")
        parse_task = asyncio.create_task(asyncio.to_thread(parse_cheque_with_gpt, local_path, message.caption, False))
        done, pending = await asyncio.wait({parse_task}, timeout=120)
        if not done:
            logger.warning("Parse timeout (photo)")
            await message.answer("⏰ Превышено время распознавания. Попробуйте ещё раз позже")
            try:
                os.remove(local_path)
            except Exception:
                pass
            return
        items = parse_task.result()
        logger.info(f"Parsed items count (photo): {len(items) if items else 0}")
        await message.answer(f"🔍 Распознавание завершено: {len(items) if items else 0} позиций.")
    except Exception as e:
        await message.answer(f"❌ Ошибка парсинга: {e}")
        try:
            os.remove(local_path)
        except Exception:
            pass
        return
    
    if not items:
        await message.answer("❌ Не удалось распознать позиции в чеке")
        try:
            os.remove(local_path)
        except Exception:
            pass
        return
    
    chequeid, processed_items, preview_text, total_sum = prepare_pending_cheque(
        user_id=user_id,
        username=username,
        local_path=local_path,
        items=items,
    )
    
    info_message = (
        f"🧾 Черновик чека № {chequeid}\n"
        f"📦 Позиций: {len(processed_items)}\n"
        f"💳 Сумма: {total_sum:.2f} ₽\n\n"
        "Проверь список ниже и выбери действие:"
    )
    await message.answer(info_message)
    await message.answer(
        preview_text,
        reply_markup=build_cheque_actions_keyboard(),
    )


@dp.message(F.document)
async def handle_document(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"
    
    if not message.document.mime_type or not message.document.mime_type.startswith("image/"):
        await message.answer("⚠️ Пришли изображение чека")
        return
    
    ensure_dirs()
    init_db()
    
    user_dir = get_user_cheque_dir(username, user_id)
    
    await message.answer("📥 Документ получен. ⏳ Идёт распознавание чека...")
    try:
        file = await asyncio.wait_for(bot.get_file(message.document.file_id), timeout=30)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        ext = os.path.splitext(message.document.file_name or ".jpg")[1]
        local_path = os.path.join(user_dir, f"cheque_{ts}{ext}")
        await asyncio.wait_for(bot.download_file(file.file_path, local_path), timeout=45)
    except asyncio.TimeoutError:
        await message.answer("⏰ Не удалось скачать документ от Telegram (таймаут)")
        return
    except Exception as e:
        await message.answer(f"⚠️ Ошибка скачивания документа: {e}")
        return
    try:
        logger.info("Start parse task (document)")
        parse_task = asyncio.create_task(asyncio.to_thread(parse_cheque_with_gpt, local_path, message.document.file_name, False))
        done, pending = await asyncio.wait({parse_task}, timeout=120)
        if not done:
            logger.warning("Parse timeout (document)")
            await message.answer("⏰ Превышено время распознавания. Попробуйте ещё раз позже")
            try:
                os.remove(local_path)
            except Exception:
                pass
            return
        items = parse_task.result()
        logger.info(f"Parsed items count (document): {len(items) if items else 0}")
        await message.answer(f"🔍 Распознавание завершено: {len(items) if items else 0} позиций.")
    except Exception as e:
        await message.answer(f"❌ Ошибка парсинга: {e}")
        try:
            os.remove(local_path)
        except Exception:
            pass
        return
    
    if not items:
        await message.answer("❌ Не удалось распознать позиции в чеке")
        try:
            os.remove(local_path)
        except Exception:
            pass
        return
    
    chequeid, processed_items, preview_text, total_sum = prepare_pending_cheque(
        user_id=user_id,
        username=username,
        local_path=local_path,
        items=items,
    )
    
    info_message = (
        f"🧾 Черновик чека № {chequeid}\n"
        f"📦 Позиций: {len(processed_items)}\n"
        f"💳 Сумма: {total_sum:.2f} ₽\n\n"
        "Проверь список ниже и выбери действие:"
    )
    await message.answer(info_message)
    await message.answer(
        preview_text,
        reply_markup=build_cheque_actions_keyboard(),
    )


@dp.callback_query(F.data == SAVE_CALLBACK)
async def callback_save_cheque(call: CallbackQuery):
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        await call.answer("Нет чека для сохранения", show_alert=True)
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return
    
    items = pending["items"]
    username = pending["username"]
    chequeid = pending["chequeid"]
    
    cheque_date = items[0].get("date")
    cheque_organization = items[0].get("organization")
    total_sum = sum(float(item.get("price", 0) or 0) for item in items)
    
    if cheque_date and cheque_organization:
        try:
            is_duplicate = await asyncio.to_thread(
                check_duplicate_cheque,
                cheque_date,
                username,
                cheque_organization,
                total_sum,
            )
        except Exception as exc:
            logger.error(f"Duplicate check failed: {exc}")
            is_duplicate = False
        if is_duplicate:
            await call.answer("⚠️ Этот чек уже внесён в базу данных", show_alert=True)
            return
    
    try:
        await asyncio.to_thread(bulk_insert_purchases, items)
    except Exception as exc:
        logger.error(f"DB insert failed for cheque {chequeid}: {exc}")
        await call.answer(f"Ошибка сохранения: {exc}", show_alert=True)
        return
    
    context_manager.clear_pending_cheque(user_id)
    context_manager.set_last_cheque(user_id, chequeid)
    
    await call.answer("Чек сохранён", show_alert=False)
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    
    await call.message.answer(f"💾 Сохранено позиций: {len(items)} (чек {chequeid})")
    try:
        cheque_records = await asyncio.to_thread(ai_db.get_cheque_by_id, chequeid, username)
        cheque_text = report_builder.format_cheque(cheque_records)
        await call.message.answer(cheque_text, parse_mode=None)
    except Exception as exc:
        logger.error(f"Failed to fetch saved cheque {chequeid}: {exc}")


@dp.callback_query(F.data == DELETE_CALLBACK)
async def callback_delete_cheque(call: CallbackQuery):
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        await call.answer("Черновик уже удалён", show_alert=False)
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return
    
    discard_pending_cheque(user_id, remove_file=True)
    await call.answer("Черновик удалён", show_alert=False)
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await call.message.answer("🗑️ Черновик удалён. Отправьте новый чек, если нужно.")


@dp.callback_query(F.data == RETRY_CALLBACK)
async def callback_retry_cheque(call: CallbackQuery):
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return
    
    file_path = pending.get("file_path")
    if not file_path or not os.path.isfile(file_path):
        context_manager.clear_pending_cheque(user_id)
        await call.answer("Файл чека не найден, отправьте фото заново", show_alert=True)
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return
    
    await call.answer()
    await call.message.answer("🔄 Идёт повторное распознавание чека...")
    
    try:
        receipt_text = await asyncio.to_thread(extract_receipt_text, file_path)
    except Exception as exc:
        logger.error(f"OCR retry failed: {exc}")
        await call.message.answer(f"❌ Не удалось извлечь текст: {exc}")
        return
    
    try:
        new_items = await asyncio.to_thread(
            parse_cheque_with_gpt,
            file_path,
            None,
            False,
            receipt_text,
        )
    except Exception as exc:
        logger.error(f"Retry parsing with text failed: {exc}")
        await call.message.answer(f"❌ Ошибка повторного распознавания: {exc}")
        return
    
    if not new_items:
        await call.message.answer("⚠️ Повторное распознавание не дало результатов. Попробуйте другое фото.")
        return
    
    username = pending.get("username") or f"user_{user_id}"
    chequeid, processed_items, preview_text, total_sum = prepare_pending_cheque(
        user_id=user_id,
        username=username,
        local_path=file_path,
        items=new_items,
    )
    
    summary = (
        f"🔄 Обновлённый черновик чека № {chequeid}\n"
        f"📦 Позиций: {len(processed_items)}\n"
        f"💳 Сумма: {total_sum:.2f} ₽"
    )
    await call.message.answer(summary)
    await call.message.answer(
        preview_text,
        reply_markup=build_cheque_items_keyboard(processed_items),
    )


def classify_product_categories(product_name: str) -> Dict[str, str]:
    """Классифицирует товар по категориям через GPT."""
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        clf_resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Классифицируй товар по трём уровням категорий. Верни ТОЛЬКО JSON-объект "
                        "с полями category1, category2, category3. Без пояснений."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Наименование: {product_name}",
                },
            ],
            temperature=0.0,
        )
        ctext = clf_resp.choices[0].message.content.strip()
        if ctext.startswith("```"):
            m2 = re.search(r"```(?:json)?\s*([\s\S]*?)```", ctext, flags=re.IGNORECASE)
            if m2:
                ctext = m2.group(1).strip()
        if ctext.lower().startswith("json\n"):
            ctext = ctext.split("\n", 1)[1]
        obj = json.loads(ctext)
        if isinstance(obj, dict):
            cat1 = (obj.get("category1") or "").strip()
            cat2 = (obj.get("category2") or "").strip()
            cat3 = (obj.get("category3") or "").strip()
            if cat1 or cat2 or cat3:
                return {"category1": cat1, "category2": cat2, "category3": cat3}
    except Exception as e:
        logger.error(f"Error classifying categories: {e}")
    
    # Возвращаем значения по умолчанию
    return {"category1": "Прочее", "category2": "Прочее", "category3": "Прочее"}


def create_new_cheque_pending(user_id: int, username: str) -> Dict:
    """Создает новый pending_cheque для ручного создания чека."""
    chequeid = get_next_cheque_id()
    now_iso = datetime.now(timezone.utc).isoformat()
    
    pending = {
        "items": [],
        "file_path": None,
        "username": username,
        "chequeid": chequeid,
        "created_at": now_iso,
        "new_cheque_state": {
            "organization": "",
            "date": "",
        }
    }
    
    context_manager.set_pending_cheque(user_id, pending)
    return pending


async def add_item_to_pending_cheque(user_id: int, product_name: str, price: float) -> None:
    """Добавляет новую позицию в pending_cheque с автоматической классификацией категорий."""
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        raise ValueError("Нет активного черновика чека")
    
    items = pending.get("items", [])
    username = pending.get("username")
    chequeid = pending.get("chequeid")
    now_iso = datetime.now(timezone.utc).isoformat()
    
    # Если это новый чек (нет позиций), используем данные из new_cheque_state
    if not items:
        new_cheque_state = pending.get("new_cheque_state", {})
        date = new_cheque_state.get("date", datetime.now().strftime("%d.%m.%Y"))
        organization = new_cheque_state.get("organization", "")
        file_path = None
    else:
        # Получаем данные из первой позиции чека
        first_item = items[0]
        date = first_item.get("date")
        organization = first_item.get("organization")
        file_path = first_item.get("file_path")
    
    # Классифицируем категории через AI
    categories = await asyncio.to_thread(classify_product_categories, product_name)
    
    # Создаем новую позицию
    new_item = {
        "chequeid": chequeid,
        "file_path": file_path,
        "date": date,
        "created_at": now_iso,
        "product_name": product_name,
        "quantity": 1.0,
        "price": float(price),
        "discount": 0.0,
        "category1": categories.get("category1", "Прочее"),
        "category2": categories.get("category2", "Прочее"),
        "category3": categories.get("category3", "Прочее"),
        "organization": organization,
        "username": username,
        "description": None,
    }
    
    # Добавляем в список позиций
    items.append(new_item)
    
    # Очищаем состояние добавления
    if "add_state" in pending:
        pending.pop("add_state", None)
    
    # Обновляем pending_cheque
    context_manager.set_pending_cheque(user_id, pending)


async def refresh_cheque_display(user_id: int, message: Message) -> None:
    """Обновляет отображение чека с актуальными данными."""
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        return
    
    items = pending["items"]
    if not items:
        return
    
    preview_text = report_builder.format_cheque(items)
    keyboard = build_cheque_items_keyboard(items)
    
    try:
        await message.edit_text(preview_text, reply_markup=keyboard)
    except Exception:
        # Если не удалось отредактировать, отправляем новое сообщение
        await message.answer(preview_text, reply_markup=keyboard)


@dp.callback_query(F.data.startswith(EDIT_ITEM_PREFIX))
async def callback_edit_item(call: CallbackQuery):
    """Открывает меню редактирования позиции."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    items = pending["items"]
    if not items:
        await call.answer("Нет позиций для редактирования", show_alert=True)
        return
    
    # Извлекаем индекс позиции
    try:
        item_index = int(call.data.replace(EDIT_ITEM_PREFIX, ""))
        if item_index < 0 or item_index >= len(items):
            await call.answer("Неверный номер позиции", show_alert=True)
            return
    except ValueError:
        await call.answer("Ошибка обработки запроса", show_alert=True)
        return
    
    item = items[item_index]
    item_name = item.get("product_name", "N/A")
    
    edit_text = f"✏️ Редактирование позиции #{item_index + 1}\n\n{item_name}"
    keyboard = build_edit_item_keyboard(item_index, item)
    
    try:
        await call.message.edit_text(edit_text, reply_markup=keyboard)
    except Exception:
        await call.message.answer(edit_text, reply_markup=keyboard)


@dp.callback_query(F.data.startswith(EDIT_FIELD_PREFIX))
async def callback_edit_field(call: CallbackQuery):
    """Начинает редактирование конкретного поля позиции."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    items = pending["items"]
    if not items:
        await call.answer("Нет позиций для редактирования", show_alert=True)
        return
    
    # Извлекаем индекс и поле: edit_field_0_price -> (0, "price")
    try:
        data_parts = call.data.replace(EDIT_FIELD_PREFIX, "").split("_", 1)
        if len(data_parts) != 2:
            await call.answer("Ошибка обработки запроса", show_alert=True)
            return
        
        item_index = int(data_parts[0])
        field = data_parts[1]
        
        if item_index < 0 or item_index >= len(items):
            await call.answer("Неверный номер позиции", show_alert=True)
            return
        
        if field not in ["price", "quantity", "product_name", "category1", "description"]:
            await call.answer("Неверное поле для редактирования", show_alert=True)
            return
    except (ValueError, IndexError):
        await call.answer("Ошибка обработки запроса", show_alert=True)
        return
    
    item = items[item_index]
    
    # Сохраняем состояние редактирования в pending_cheque
    pending["edit_state"] = {
        "item_index": item_index,
        "field": field
    }
    context_manager.set_pending_cheque(user_id, pending)
    
    # Формируем сообщение с подсказкой
    field_names = {
        "price": "цену",
        "quantity": "количество",
        "product_name": "название",
        "category1": "категорию",
        "description": "описание"
    }
    
    current_value = item.get(field, "")
    if field == "price":
        current_value = f"{float(item.get('price', 0) or 0):.2f} ₽"
    elif field == "quantity":
        current_value = f"{float(item.get('quantity', 1) or 1)} шт."
    
    prompt_text = (
        f"Введите новое значение для {field_names.get(field, field)}:\n"
        f"Текущее: {current_value if current_value else '—'}"
    )
    
    try:
        await call.message.edit_text(prompt_text)
    except Exception:
        await call.message.answer(prompt_text)
    
    await call.answer()


@dp.callback_query(F.data.startswith(DELETE_ITEM_PREFIX))
async def callback_delete_item(call: CallbackQuery):
    """Удаляет позицию из черновика чека."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    items = pending["items"]
    if not items:
        await call.answer("Нет позиций для удаления", show_alert=True)
        return
    
    # Извлекаем индекс позиции
    try:
        item_index = int(call.data.replace(DELETE_ITEM_PREFIX, ""))
        if item_index < 0 or item_index >= len(items):
            await call.answer("Неверный номер позиции", show_alert=True)
            return
    except ValueError:
        await call.answer("Ошибка обработки запроса", show_alert=True)
        return
    
    # Удаляем позицию
    deleted_item = items.pop(item_index)
    item_name = deleted_item.get("product_name", "N/A")
    
    # Очищаем состояние редактирования если оно было для этой позиции
    if "edit_state" in pending:
        edit_state = pending["edit_state"]
        if edit_state.get("item_index") == item_index:
            pending.pop("edit_state", None)
        elif edit_state.get("item_index") > item_index:
            # Корректируем индекс если удалили позицию выше
            edit_state["item_index"] -= 1
    
    # Обновляем pending_cheque
    if items:
        context_manager.set_pending_cheque(user_id, pending)
        await refresh_cheque_display(user_id, call.message)
        await call.answer(f"Позиция '{item_name[:30]}' удалена", show_alert=False)
    else:
        # Если позиций не осталось, удаляем весь черновик
        discard_pending_cheque(user_id, remove_file=True)
        await call.message.edit_text("🗑️ Все позиции удалены. Черновик очищен.")
        await call.answer("Черновик очищен", show_alert=False)


@dp.callback_query(F.data == BACK_TO_CHEQUE)
async def callback_back_to_cheque(call: CallbackQuery):
    """Возвращает к отображению списка позиций чека."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    # Очищаем состояние редактирования
    if "edit_state" in pending:
        pending.pop("edit_state", None)
        context_manager.set_pending_cheque(user_id, pending)
    
    await refresh_cheque_display(user_id, call.message)
    await call.answer()


@dp.callback_query(F.data.startswith(ADD_ITEM_FIELD_PREFIX))
async def callback_add_item_field(call: CallbackQuery):
    """Начинает ввод поля для добавления позиции."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    # Извлекаем поле: add_item_field_product_name -> "product_name"
    try:
        field = call.data.replace(ADD_ITEM_FIELD_PREFIX, "")
        if field not in ["product_name", "price"]:
            await call.answer("Неверное поле для ввода", show_alert=True)
            return
    except Exception:
        await call.answer("Ошибка обработки запроса", show_alert=True)
        return
    
    # Инициализируем add_state если его нет
    if "add_state" not in pending:
        pending["add_state"] = {}
    
    # Сохраняем состояние ожидания ввода
    pending["add_state"]["field"] = field
    context_manager.set_pending_cheque(user_id, pending)
    
    # Формируем сообщение с подсказкой
    field_names = {
        "product_name": "наименование товара",
        "price": "цену товара"
    }
    
    current_value = pending["add_state"].get(field, "")
    if field == "price" and current_value:
        current_value = f"{float(current_value):.2f} ₽"
    
    prompt_text = (
        f"Введите {field_names.get(field, field)}:\n"
        f"Текущее: {current_value if current_value else '—'}"
    )
    
    try:
        await call.message.edit_text(prompt_text)
    except Exception:
        await call.message.answer(prompt_text)
    
    await call.answer()


@dp.callback_query(F.data == CANCEL_ADD_ITEM)
async def callback_cancel_add_item(call: CallbackQuery):
    """Отменяет добавление позиции."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    # Очищаем состояние добавления
    if "add_state" in pending:
        pending.pop("add_state", None)
        context_manager.set_pending_cheque(user_id, pending)
    
    await call.message.edit_text("❌ Добавление позиции отменено")
    await call.answer("Добавление отменено", show_alert=False)


@dp.callback_query(F.data.startswith(NEW_CHEQUE_ORG_PREFIX))
async def callback_new_cheque_org(call: CallbackQuery):
    """Обработка ввода организации для нового чека."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending or "new_cheque_state" not in pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    new_cheque_state = pending.get("new_cheque_state", {})
    new_cheque_state["waiting_for"] = "organization"
    context_manager.set_pending_cheque(user_id, pending)
    
    await call.message.edit_text("✏️ Введите название организации:")
    await call.answer()


@dp.callback_query(F.data == NEW_CHEQUE_DATE_TODAY)
async def callback_new_cheque_date_today(call: CallbackQuery):
    """Устанавливает дату чека на сегодня."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending or "new_cheque_state" not in pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    today = datetime.now().strftime("%d.%m.%Y")
    new_cheque_state = pending.get("new_cheque_state", {})
    new_cheque_state["date"] = today
    context_manager.set_pending_cheque(user_id, pending)
    
    keyboard = build_new_cheque_setup_keyboard(new_cheque_state)
    await call.message.edit_text(
        f"✅ Дата установлена: {today}\n\nЗаполните организацию:",
        reply_markup=keyboard
    )
    await call.answer()


@dp.callback_query(F.data == NEW_CHEQUE_DATE_YESTERDAY)
async def callback_new_cheque_date_yesterday(call: CallbackQuery):
    """Устанавливает дату чека на вчера."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending or "new_cheque_state" not in pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")
    new_cheque_state = pending.get("new_cheque_state", {})
    new_cheque_state["date"] = yesterday
    context_manager.set_pending_cheque(user_id, pending)
    
    keyboard = build_new_cheque_setup_keyboard(new_cheque_state)
    await call.message.edit_text(
        f"✅ Дата установлена: {yesterday}\n\nЗаполните организацию:",
        reply_markup=keyboard
    )
    await call.answer()


@dp.callback_query(F.data == NEW_CHEQUE_DATE_CUSTOM)
async def callback_new_cheque_date_custom(call: CallbackQuery):
    """Запрашивает ввод даты вручную."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending or "new_cheque_state" not in pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    new_cheque_state = pending.get("new_cheque_state", {})
    new_cheque_state["waiting_for"] = "date_custom"
    context_manager.set_pending_cheque(user_id, pending)
    
    await call.message.edit_text("✏️ Введите дату в формате ДД.ММ.ГГГГ (например: 15.11.2025):")
    await call.answer()


@dp.callback_query(F.data.startswith(NEW_CHEQUE_DATE_PREFIX))
async def callback_new_cheque_date_select(call: CallbackQuery):
    """Показывает меню выбора даты для нового чека."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending or "new_cheque_state" not in pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    keyboard = build_new_cheque_date_keyboard()
    await call.message.edit_text("📅 Выберите дату чека:", reply_markup=keyboard)
    await call.answer()


@dp.callback_query(F.data == "new_cheque_start_add")
async def callback_new_cheque_start_add(call: CallbackQuery):
    """Начинает добавление позиций в новый чек."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending or "new_cheque_state" not in pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    new_cheque_state = pending.get("new_cheque_state", {})
    organization = new_cheque_state.get("organization", "")
    date = new_cheque_state.get("date", "")
    
    if not organization or not date:
        await call.answer("⚠️ Сначала заполните организацию и дату", show_alert=True)
        return
    
    # Очищаем состояние настройки, оставляем только данные
    add_state = pending.get("add_state", {})
    keyboard = build_add_item_keyboard(add_state)
    await call.message.edit_text(
        "➕ Добавление новой позиции\n\nВыберите поле для заполнения:",
        reply_markup=keyboard
    )
    await call.answer()


@dp.callback_query(F.data == "new_cheque_add_item")
async def callback_new_cheque_add_item(call: CallbackQuery):
    """Открывает форму добавления позиции для нового чека."""
    user_id = call.from_user.id
    pending = context_manager.get_pending_cheque(user_id)
    if not pending:
        await call.answer("Черновик отсутствует", show_alert=True)
        return
    
    add_state = pending.get("add_state", {})
    keyboard = build_add_item_keyboard(add_state)
    await call.message.edit_text(
        "➕ Добавление новой позиции\n\nВыберите поле для заполнения:",
        reply_markup=keyboard
    )
    await call.answer()


@dp.message(F.text)
async def handle_text(message: Message):
    user_id = message.from_user.id
    username_raw = message.from_user.username
    username = username_raw if username_raw else f"user_{user_id}"
    user_message = message.text
    
    logger.info(f"========== NEW MESSAGE ==========")
    logger.info(f"User {user_id} (@{username_raw or 'no_username'}) -> username for DB: '{username}'")
    logger.info(f"Message: {user_message}")
    
    pending = context_manager.get_pending_cheque(user_id)
    user_lower = user_message.lower()
    
    # Обработка команды обновления последнего запроса
    refresh_commands = [
        "обнови последний запрос",
        "обновить последний запрос",
        "пересчитай последний запрос",
        "пересчитать последний запрос",
        "обнови запрос",
        "обновить запрос"
    ]
    if any(cmd in user_lower for cmd in refresh_commands):
        response = refresh_last_query(user_id, username, context_manager)
        context_manager.add_message(user_id, "assistant", response)
        await message.answer(response, parse_mode=None)
        return
    
    # Обработка команды объединения групп категорий
    merge_match = re.search(
        r"объедини(?:ть)?(?:\s+группы)?\s+(.+?)\s+и(?:\+)?\s+(.+)",
        user_lower,
        flags=re.IGNORECASE | re.DOTALL
    )
    if merge_match:
        # Извлекаем значения из оригинального сообщения (с сохранением регистра)
        value1_match = re.search(
            r"объедини(?:ть)?(?:\s+группы)?\s+(.+?)\s+и(?:\+)?\s+(.+)",
            user_message,
            flags=re.IGNORECASE | re.DOTALL
        )
        if value1_match:
            value1_raw = value1_match.group(1).strip().strip(' "\'«»')
            value2_raw = value1_match.group(2).strip().strip(' "\'«»')
            
            if value1_raw and value2_raw:
                # Ищем точные значения категорий в базе (с учетом регистра)
                value1 = ai_db.find_exact_category1(value1_raw, username)
                value2 = ai_db.find_exact_category1(value2_raw, username)
                
                if not value2:
                    response = f"❌ Категория '{value2_raw}' не найдена в базе данных."
                elif not value1:
                    response = f"❌ Категория '{value1_raw}' не найдена в базе данных."
                else:
                    rows_updated, found = ai_db.merge_category1_groups(value2, value1, username)
                    if not found:
                        response = f"❌ Категория '{value2}' не найдена в базе данных."
                    else:
                        # Очищаем кеш после успешного объединения, чтобы новые данные были доступны
                        context_manager.clear_last_query(user_id)
                        response = f"✅ Объединение выполнено: категория '{value2}' объединена с '{value1}'. Обновлено записей: {rows_updated}"
                context_manager.add_message(user_id, "assistant", response)
                await message.answer(response, parse_mode=None)
                return
    
    # Проверяем текстовые команды добавления позиции
    add_commands = ["добавь позицию", "добавить позицию", "добавить товар", "новая позиция", "добавить позицию в чек"]
    if pending and any(cmd in user_lower for cmd in add_commands):
        # Открываем форму добавления
        add_state = pending.get("add_state", {})
        keyboard = build_add_item_keyboard(add_state)
        await message.answer(
            "➕ Добавление новой позиции\n\nВыберите поле для заполнения:",
            reply_markup=keyboard
        )
        return
    
    # Проверяем текстовые команды редактирования позиции
    edit_commands = ["изменить позицию", "корректировать позицию", "редактировать позицию", "исправить позицию", "поправить позицию", "отредактировать позицию", "изменить товар", "корректировать товар", "исправить товар", "поправить товар", "отредактировать товар"]
    if pending and any(cmd in user_lower for cmd in edit_commands):
        # Парсим номер позиции из команды
        # Ищем числа в команде: "изменить позицию 1", "корректировать 2-ю", "редактировать позицию №3"
        numbers = re.findall(r'\d+', user_message)
        if numbers:
            try:
                item_index = int(numbers[0]) - 1  # Пользователь указывает с 1, мы используем с 0
                items = pending.get("items", [])
                if 0 <= item_index < len(items):
                    item = items[item_index]
                    item_name = item.get("product_name", "N/A")
                    edit_text = f"✏️ Редактирование позиции #{item_index + 1}\n\n{item_name}"
                    keyboard = build_edit_item_keyboard(item_index, item)
                    await message.answer(edit_text, reply_markup=keyboard)
                    return
                else:
                    await message.answer(f"❌ Позиция #{item_index + 1} не найдена. В чеке {len(items)} позиций.")
                    return
            except ValueError:
                pass
        
        # Если номер не указан - показываем список позиций
        items = pending.get("items", [])
        if not items:
            await message.answer("❌ В чеке нет позиций для редактирования")
            return
        
        text = "✏️ Выберите позицию для редактирования:\n\n"
        for idx, item in enumerate(items, 1):
            name = item.get("product_name", "N/A")[:40]
            price = float(item.get("price", 0) or 0)
            text += f"{idx}. {name} | {price:.2f} ₽\n"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"✏️ Позиция {idx}",
                callback_data=f"{EDIT_ITEM_PREFIX}{idx - 1}"
            )] for idx in range(1, len(items) + 1)
        ])
        await message.answer(text, reply_markup=keyboard)
        return
    
    # Проверяем команды создания нового чека
    new_cheque_commands = ["сделать новый чек", "добавить новый чек", "создать чек", "новый чек", "создать новый чек", "добавить чек"]
    if any(cmd in user_lower for cmd in new_cheque_commands):
        # Если уже есть pending_cheque, спрашиваем подтверждение
        if pending:
            await message.answer("⚠️ У вас уже есть черновик чека. Сначала сохраните или удалите его.")
            return
        
        # Создаем новый pending_cheque
        create_new_cheque_pending(user_id, username)
        pending = context_manager.get_pending_cheque(user_id)
        new_cheque_state = pending.get("new_cheque_state", {})
        keyboard = build_new_cheque_setup_keyboard(new_cheque_state)
        await message.answer(
            "📝 Создание нового чека\n\nЗаполните организацию и дату чека:",
            reply_markup=keyboard
        )
        return
    
    # Проверяем состояние настройки нового чека
    if pending and "new_cheque_state" in pending:
        new_cheque_state = pending.get("new_cheque_state", {})
        waiting_for = new_cheque_state.get("waiting_for")
        
        if waiting_for == "organization":
            # Сохраняем организацию
            organization = user_message.strip()
            if not organization:
                await message.answer("❌ Название организации не может быть пустым")
                return
            new_cheque_state["organization"] = organization
            new_cheque_state.pop("waiting_for", None)
            context_manager.set_pending_cheque(user_id, pending)
            
            keyboard = build_new_cheque_setup_keyboard(new_cheque_state)
            await message.answer(
                f"✅ Организация сохранена: {organization}\n\nЗаполните дату чека:",
                reply_markup=keyboard
            )
            return
        
        elif waiting_for == "date_custom":
            # Парсим дату вручную
            date_str = user_message.strip()
            # Пробуем распарсить дату
            parsed_date = _normalize_date_token(date_str)
            if not parsed_date:
                await message.answer("❌ Неверный формат даты. Используйте формат ДД.ММ.ГГГГ (например: 15.11.2025)")
                return
            new_cheque_state["date"] = parsed_date
            new_cheque_state.pop("waiting_for", None)
            context_manager.set_pending_cheque(user_id, pending)
            
            keyboard = build_new_cheque_setup_keyboard(new_cheque_state)
            await message.answer(
                f"✅ Дата сохранена: {parsed_date}\n\nТеперь можно начать добавление позиций:",
                reply_markup=keyboard
            )
            return
    
    # Проверяем состояние добавления позиции
    if pending and "add_state" in pending:
        add_state = pending["add_state"]
        field = add_state.get("field")
        
        if field:
            # Валидация и преобразование значения
            new_value = user_message.strip()
            
            if field == "price":
                try:
                    # Убираем возможные символы валюты и пробелы
                    clean_value = new_value.replace("₽", "").replace(",", ".").strip()
                    price_value = float(clean_value)
                    if price_value < 0:
                        await message.answer("❌ Цена не может быть отрицательной")
                        return
                    add_state["price"] = price_value
                    add_state.pop("field", None)  # Убираем ожидание ввода
                except ValueError:
                    await message.answer("❌ Неверный формат цены. Введите число (например: 123.45)")
                    return
            else:
                # Для product_name
                if not new_value:
                    await message.answer("❌ Название товара не может быть пустым")
                    return
                add_state["product_name"] = new_value
                add_state.pop("field", None)  # Убираем ожидание ввода
            
            # Обновляем состояние
            context_manager.set_pending_cheque(user_id, pending)
            
            # Проверяем, заполнены ли оба поля
            if add_state.get("product_name") and add_state.get("price") is not None:
                # Оба поля заполнены - добавляем позицию
                product_name = add_state["product_name"]
                price = add_state["price"]
                
                # Добавляем позицию в pending_cheque
                await add_item_to_pending_cheque(user_id, product_name, price)
                
                # Получаем обновленный pending после добавления позиции
                pending = context_manager.get_pending_cheque(user_id)
                
                # Обновляем отображение чека (если есть позиции)
                items = pending.get("items", [])
                if items:
                    await refresh_cheque_display(user_id, message)
                
                # Показываем кнопки управления только для нового чека
                if pending and "new_cheque_state" in pending:
                    actions_keyboard = build_new_cheque_actions_keyboard()
                    await message.answer(
                        f"✅ Позиция добавлена: {product_name[:40]} | {price:.2f} ₽",
                        reply_markup=actions_keyboard
                    )
                else:
                    await message.answer(f"✅ Позиция добавлена: {product_name[:40]} | {price:.2f} ₽")
            else:
                # Обновляем клавиатуру с текущими значениями
                keyboard = build_add_item_keyboard(add_state)
                await message.answer(
                    "➕ Добавление новой позиции\n\nВыберите поле для заполнения:",
                    reply_markup=keyboard
                )
            return
    
    # Проверяем состояние редактирования позиции
    if pending and "edit_state" in pending:
        edit_state = pending["edit_state"]
        item_index = edit_state.get("item_index")
        field = edit_state.get("field")
        
        if item_index is not None and field:
            items = pending.get("items", [])
            if 0 <= item_index < len(items):
                item = items[item_index]
                
                # Валидация и преобразование значения
                new_value = user_message.strip()
                
                if field == "price":
                    try:
                        # Убираем возможные символы валюты и пробелы
                        clean_value = new_value.replace("₽", "").replace(",", ".").strip()
                        price_value = float(clean_value)
                        if price_value < 0:
                            await message.answer("❌ Цена не может быть отрицательной")
                            return
                        item["price"] = price_value
                        new_value = str(price_value)
                    except ValueError:
                        await message.answer("❌ Неверный формат цены. Введите число (например: 123.45)")
                        return
                
                elif field == "quantity":
                    try:
                        # Убираем возможные единицы измерения
                        clean_value = new_value.replace("шт", "").replace("кг", "").replace("л", "").replace(",", ".").strip()
                        quantity_value = float(clean_value)
                        if quantity_value <= 0:
                            await message.answer("❌ Количество должно быть больше нуля")
                            return
                        item["quantity"] = quantity_value
                        new_value = str(quantity_value)
                    except ValueError:
                        await message.answer("❌ Неверный формат количества. Введите число (например: 2 или 0.5)")
                        return
                
                else:
                    # Для текстовых полей (product_name, category1, description)
                    item[field] = new_value
                
                # Очищаем состояние редактирования
                pending.pop("edit_state", None)
                context_manager.set_pending_cheque(user_id, pending)
                
                # Обновляем отображение чека
                await refresh_cheque_display(user_id, message)
                await message.answer(f"✅ Поле обновлено: {field} = {new_value[:50]}")
                return
    
    context_manager.add_message(user_id, "user", user_message)
    user_lower = user_message.lower()
    
    # Проверяем ключевые слова для Excel и графика
    excel_keywords = ["эксель", "excel", "таблица", "таблицу"]
    need_excel = any(keyword in user_lower for keyword in excel_keywords)
    need_chart = "график" in user_lower
    
    single_day_match = re.search(r"покажи\s+все\s+чеки\s+за\s+(\d{2}\.\d{2}\.\d{4})", user_message, flags=re.IGNORECASE)
    if single_day_match:
        date_str = single_day_match.group(1)
        if not _parse_ddmmyyyy(date_str):
            error_response = (
                f"❌ Не смог распознать дату '{date_str}'. "
                "Используй формат ДД.ММ.ГГГГ."
            )
            context_manager.add_message(user_id, "assistant", error_response)
            await message.answer(error_response, parse_mode=None)
            return
        
        result = ai_db.fetch_by_period(date_str, date_str, username)
        context_manager.set_last_query(
            user_id,
            "fetch_by_period",
            {"start_date": date_str, "end_date": date_str},
            result,
            username,
        )
        if result:
            purchases_text = report_builder.format_purchases_list(result, limit=len(result))
        else:
            purchases_text = "Записей не найдено"
        final_response = f"📅 Чеки за {date_str}:\n\n{purchases_text}"
        context_manager.add_message(user_id, "assistant", final_response)
        await message.answer(final_response, parse_mode=None)
        return
    
    grouped_category_match = re.search(
        r"покажи.*категор(?:ия|и)2.*категор(?:ия|и)1\s+(.+)",
        user_message,
        flags=re.IGNORECASE | re.DOTALL,
    )
    category1_value = None
    if grouped_category_match:
        category1_value = grouped_category_match.group(1).strip()
        category1_value = category1_value.splitlines()[0].strip()
        category1_value = category1_value.strip(' "\'«»')
    elif (
        ("категор" in user_lower or "category" in user_lower)
        and ("категория1" in user_lower or "category1" in user_lower)
    ):
        idx = user_lower.rfind("категория1")
        key_len = len("категория1")
        if idx == -1:
            idx = user_lower.rfind("category1")
            key_len = len("category1")
        if idx != -1:
            value_part = user_message[idx + key_len :]
            value_part = value_part.replace("=", " ").replace(":", " ")
            category1_candidate = value_part.strip()
            if category1_candidate:
                category1_candidate = category1_candidate.splitlines()[0].strip()
                category1_candidate = category1_candidate.strip(' "\'«»')
            if category1_candidate:
                category1_value = category1_candidate
    if category1_value:
        start_date, end_date = resolve_period_for_message(user_id, user_message)
        dataset = []
        last_query = context_manager.get_last_query(user_id)
        if (
            last_query
            and last_query.get("type") == "fetch_by_period"
            and last_query.get("params", {}).get("start_date") == start_date
            and last_query.get("params", {}).get("end_date") == end_date
        ):
            dataset = last_query.get("result", []) or []
        if not dataset:
            dataset = ai_db.fetch_by_period(start_date, end_date, username)
        result = aggregate_category2_by_category1(dataset, category1_value)
        context_manager.set_last_query(
            user_id,
            "get_grouped_stats_filtered",
            {"start_date": start_date, "end_date": end_date, "field": "category2", "filters": {"category1": category1_value}},
            result,
            username,
        )
        if result:
            final_response = (
                f"📊 Группировка category2 при category1 = '{category1_value}' "
                f"за период {start_date} - {end_date}:\n\n"
                f"{report_builder.format_grouped_stats(result, 'category2')}"
            )
        else:
            final_response = (
                f"Нет данных для category2 при category1 = '{category1_value}' "
                f"за период {start_date} - {end_date}"
            )
        context_manager.add_message(user_id, "assistant", final_response)
        await message.answer(final_response, parse_mode=None)
        return
    
    # Обработка запросов на рекомендации по экономии
    if should_handle_economy_request(user_message):
        advice_text = await process_economy_request(
            message=user_message,
            user_id=user_id,
            username=username,
            context_manager=context_manager,
            ai_client=ai_client,
        )
        if advice_text:
            context_manager.add_message(user_id, "assistant", advice_text)
            await message.answer(advice_text, parse_mode=None)
        return
    
    messages = [{"role": "system", "content": context_manager.get_system_prompt()}]
    messages.extend(context_manager.get_messages(user_id))
    
    if any(keyword in user_lower for keyword in ("вчера", "вчераш", "last day", "yesterday", "прошлый день")):
        messages.append({"role": "system", "content": "Для запроса пользователя используй функцию get_yesterday()."})
    elif ("прошл" in user_lower and ("месяц" in user_lower or "month" in user_lower)) or "last month" in user_lower:
        messages.append({"role": "system", "content": "Для запроса пользователя используй функцию get_previous_month()."})
    elif ("прошл" in user_lower and ("год" in user_lower or "year" in user_lower)) or "last year" in user_lower:
        messages.append({"role": "system", "content": "Для запроса пользователя используй функцию get_previous_year()."})
    
    tools = ai_client.get_tools_definition()
    
    try:
        response = await asyncio.wait_for(
            asyncio.to_thread(ai_client.get_response, messages, tools),
            timeout=60.0
        )
    except asyncio.TimeoutError:
        logger.error("AI response timeout (60s)")
        error_message = "Запрос занял слишком много времени. Попробуйте упростить запрос или повторить позже"
        context_manager.add_message(user_id, "assistant", error_message)
        await message.answer(error_message, parse_mode=None)
        return
    except Exception as e:
        logger.error(f"Error calling AI client: {e}")
        import traceback
        logger.error(traceback.format_exc())
        error_message = "Произошла ошибка при обработке запроса. Попробуйте позже."
        context_manager.add_message(user_id, "assistant", error_message)
        await message.answer(error_message, parse_mode=None)
        return
    
    if not response:
        logger.error("AI client returned None response")
        error_message = "Не удалось получить ответ от AI. Попробуйте позже."
        context_manager.add_message(user_id, "assistant", error_message)
        await message.answer(error_message, parse_mode=None)
        return
    
    content_preview = (response.get('content') or 'None')[:100]
    logger.info(f"AI response: content={content_preview}, has_tool_calls={bool(response.get('tool_calls'))}, error={response.get('error')}")
    
    if response.get("error"):
        error_message = response.get("content", "Не удалось обработать запрос. Попробуйте переформулировать или повторить позже")
        context_manager.add_message(user_id, "assistant", error_message)
        await message.answer(error_message, parse_mode=None)
        return
    
    all_photos = []
    all_excel_paths = []
    all_chart_data = []
    if response.get("tool_calls"):
        tool_results = []
        for tool_call in response["tool_calls"]:
            function_name = tool_call.function.name
            function_args = json.loads(tool_call.function.arguments)
            
            result, photos, extra_outputs = execute_tool_call(function_name, function_args, username, user_id, user_message, need_excel, need_chart)
            if result:
                tool_results.append(result)
            all_photos.extend(photos)
            if extra_outputs.get("excel_path"):
                all_excel_paths.append(extra_outputs["excel_path"])
            if extra_outputs.get("chart_data") and extra_outputs.get("chart_field"):
                all_chart_data.append((extra_outputs["chart_data"], extra_outputs["chart_field"]))
        
        final_response = "\n\n".join(tool_results)
        if not final_response:
            last_query = context_manager.get_last_query(user_id)
            if last_query and last_query.get("result"):
                params = last_query.get("params", {})
                field = params.get("field", "category1")
                final_response = report_builder.format_grouped_stats(last_query.get("result", []), field)
    else:
        final_response = response.get("content")
        if not final_response:
            logger.warning("AI response has no content, using default message")
            final_response = "Не удалось обработать запрос. Попробуйте переформулировать."
    
    # Если запрошен график, сначала проверяем кеш, если нет - вызываем функцию по умолчанию
    try:
        if need_chart and not all_chart_data:
            should_refresh = _should_refresh_cache(user_message)
            last_query = context_manager.get_last_query(user_id)
            if not should_refresh and last_query and last_query.get("type", "").startswith("get_grouped_by"):
                # Используем данные из кеша
                result = last_query.get("result", [])
                if result:
                    field_map = {
                        "get_grouped_by_category1": "category1",
                        "get_grouped_by_category2": "category2",
                        "get_grouped_by_category3": "category3",
                        "get_grouped_by_organization": "organization",
                        "get_grouped_by_description": "description"
                    }
                    query_type = last_query.get("type", "")
                    chart_field = field_map.get(query_type, last_query.get("params", {}).get("field"))
                    if chart_field:
                        all_chart_data.append((result, chart_field))
            else:
                if last_query and last_query.get("params"):
                    params = last_query.get("params", {})
                    start_date = params.get("start_date")
                    end_date = params.get("end_date")
                    if start_date and end_date:
                        grouped = ai_db.get_grouped_stats("category1", start_date, end_date, username)
                        if grouped:
                            context_manager.set_last_query(
                                user_id,
                                "get_grouped_by_category1",
                                {"start_date": start_date, "end_date": end_date, "field": "category1"},
                                grouped,
                                username,
                            )
                            all_chart_data.append((grouped, "category1"))
                if not all_chart_data:
                    # Кеша нет - вызываем функцию группировки по умолчанию (category1 за текущий месяц)
                    start_date, end_date = get_current_month()
                    result = ai_db.get_grouped_stats("category1", start_date, end_date, username)
                    if result:
                        context_manager.set_last_query(user_id, "get_grouped_by_category1", 
                                                      {"start_date": start_date, "end_date": end_date, "field": "category1"}, 
                                                      result, username)
                        all_chart_data.append((result, "category1"))
    except Exception as chart_err:
        logger.error(f"Error in chart processing: {chart_err}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Если запрошен Excel/график, не выводим текстовый ответ (только вложение)
    # Обнуляем только если действительно есть Excel файлы или графики для отправки
    if (need_excel and all_excel_paths) or (need_chart and all_chart_data):
        final_response = ""
    else:
        # Если Excel/график не запрошены или не готовы, но final_response пустой - восстанавливаем из кеша
        if not final_response:
            last_query = context_manager.get_last_query(user_id)
            if last_query and last_query.get("result"):
                params = last_query.get("params", {})
                field = params.get("field", "category1")
                final_response = report_builder.format_grouped_stats(last_query.get("result", []), field)

    if final_response:
        try:
            context_manager.add_message(user_id, "assistant", final_response)
            await message.answer(final_response, parse_mode=None)
        except Exception as send_err:
            logger.error(f"Failed to send text response: {send_err}")
            import traceback
            logger.error(traceback.format_exc())
    
    # Отправляем графики для сгруппированных данных
    for chart_data, chart_field in all_chart_data:
        try:
            chart_buf = create_pie_chart(chart_data, chart_field)
            chart_path = os.path.join(DB_DIR, f"chart_{user_id}.png")
            with open(chart_path, "wb") as f:
                f.write(chart_buf.read())
            chart_file = FSInputFile(chart_path)
            await message.answer_photo(chart_file)
            try:
                os.remove(chart_path)
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Failed to create/send chart: {e}")
            import traceback
            logger.error(traceback.format_exc())
            await message.answer(f"⚠️ Не удалось построить график: {str(e)}")
    
    # Отправляем Excel файлы
    for excel_path in all_excel_paths:
        if os.path.exists(excel_path):
            try:
                excel_file = FSInputFile(excel_path)
                await message.answer_document(excel_file, caption="📊 Excel файл")
            except Exception as e:
                logger.error(f"Failed to send Excel {excel_path}: {e}")
                await message.answer(f"⚠️ Не удалось отправить Excel файл")
    
    # Отправляем фото чеков
    for photo_path in all_photos:
        if os.path.exists(photo_path):
            try:
                photo_file = FSInputFile(photo_path)
                await message.answer_photo(photo_file, caption="📸 Фото чека")
            except Exception as e:
                logger.error(f"Failed to send photo {photo_path}: {e}")
                await message.answer(f"⚠️ Не удалось отправить фото чека")


async def main():
    ensure_dirs()
    init_db()
    try:
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        logger.info("Polling cancelled")
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as exc:
        logger.exception(f"Unhandled exception: {exc}")

