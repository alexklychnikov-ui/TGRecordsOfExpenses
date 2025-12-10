from __future__ import annotations

import asyncio
import logging
from typing import Optional, Tuple

from aiAssistant.core.date_helpers import (
    parse_period_string,
    get_current_month,
    normalize_to_current_month_if_same_month_wrong_year,
)
from aiAssistant.db import db_manager as ai_db

from .analyzer import generate_economy_advice

logger = logging.getLogger(__name__)


ECONOMY_KEYWORDS = [
    "эконом",
    "рекоменд",
    "совет",
    "сократ",
]


def should_handle_economy_request(message: str) -> bool:
    text = (message or "").lower()
    return any(keyword in text for keyword in ECONOMY_KEYWORDS)


def _detect_period(message: str, context_manager, user_id: int, username: str) -> Tuple[str, str]:
    text = (message or "").lower()

    # 1) явный период в сообщении
    period = parse_period_string(text)
    if not period:
        # 2) кешированный период последнего запроса пользователя
        last_query = context_manager.get_last_query(user_id) if context_manager else None
        if last_query and last_query.get("username") == username:
            params = last_query.get("params") or {}
            cached_start = params.get("start_date")
            cached_end = params.get("end_date")
            if cached_start and cached_end:
                period = (cached_start, cached_end)
        # 3) дефолт — текущий месяц
        if not period:
            period = get_current_month()
    start_date, end_date = period
    start_date, end_date = normalize_to_current_month_if_same_month_wrong_year(start_date, end_date)
    return start_date, end_date


def _normalize_grouped(data):
    """Приводит разные форматы группировок к единому виду для анализа."""
    normalized = []
    for item in data or []:
        group_name = item.get("group_name") or item.get("category") or item.get("category1") or item.get("category2") or item.get("category3") or item.get("organization") or item.get("description")
        normalized.append(
            {
                "group_name": group_name,
                "total": float(item.get("total") or 0.0),
                "count": int(item.get("count") or 0),
                "cheque_count": int(item.get("cheque_count") or 0),
            }
        )
    return normalized


async def process_economy_request(
    message: str,
    user_id: int,
    username: str,
    context_manager,
    ai_client,
) -> Optional[str]:
    start_date, end_date = _detect_period(message, context_manager, user_id, username)

    grouped_data = []
    last_query = context_manager.get_last_query(user_id) if context_manager else None
    allowed_group_types = {
        "get_grouped_by_category1",
        "get_grouped_by_category2",
        "get_grouped_by_category3",
        "get_grouped_by_organization",
        "get_grouped_by_description",
        "get_grouped_stats_filtered",
    }

    if last_query:
        params = last_query.get("params") or {}
        same_period = params.get("start_date") == start_date and params.get("end_date") == end_date
        same_user = last_query.get("username") == username
        if same_period and same_user and last_query.get("type") in allowed_group_types:
            grouped_data = last_query.get("result") or []

    if not grouped_data:
        grouped_data = ai_db.get_grouped_stats("category1", start_date, end_date, username)
        context_manager.set_last_query(
            user_id,
            "get_grouped_by_category1",
            {"start_date": start_date, "end_date": end_date, "field": "category1"},
            grouped_data,
            username,
        )

    grouped_data = _normalize_grouped(grouped_data)

    if not grouped_data:
        return (
            f"❌ Нет данных по расходам за период {start_date} - {end_date}."
            " Сначала добавьте данные или уточните период."
        )

    try:
        advice_text = await asyncio.wait_for(
            asyncio.to_thread(generate_economy_advice, ai_client, grouped_data, start_date, end_date),
            timeout=60.0
        )
    except asyncio.TimeoutError:
        logger.error("Economy advice generation timeout (60s)")
        return "Запрос занял слишком много времени. Попробуйте упростить запрос или повторить позже"
    except Exception as e:
        logger.error(f"Error generating economy advice: {e}")
        return "Не удалось обработать запрос. Попробуйте переформулировать или повторить позже"

    if not advice_text or advice_text.startswith("Ошибка AI:"):
        error_msg = advice_text if advice_text and advice_text.startswith("Ошибка AI:") else None
        if error_msg:
            logger.error(f"AI error in economy advice: {error_msg}")
        return "Не удалось получить рекомендации по экономии. Попробуйте позже."

    return advice_text


