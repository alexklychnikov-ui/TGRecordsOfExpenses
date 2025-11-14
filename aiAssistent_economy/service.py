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


def _detect_period(message: str) -> Tuple[str, str]:
    text = (message or "").lower()
    period = parse_period_string(text)
    if not period:
        period = get_current_month()
    start_date, end_date = period
    start_date, end_date = normalize_to_current_month_if_same_month_wrong_year(start_date, end_date)
    return start_date, end_date


async def process_economy_request(
    message: str,
    user_id: int,
    username: str,
    context_manager,
    ai_client,
) -> Optional[str]:
    start_date, end_date = _detect_period(message)

    grouped_data = ai_db.get_grouped_stats("category1", start_date, end_date, username)

    # Сохраняем данные в кеш независимо от наличия результата
    context_manager.set_last_query(
        user_id,
        "get_grouped_by_category1",
        {"start_date": start_date, "end_date": end_date, "field": "category1"},
        grouped_data,
        username,
    )

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


