import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from aiAssistant.core.date_helpers import get_current_month
from aiAssistant.telegram.bot import (
    aggregate_category2_by_category1,
    extract_period_from_message,
    resolve_period_for_message,
    context_manager,
)


def test_aggregate_category2_filters_strictly_by_category1():
    records = [
        {"category1": "Медикаменты", "category2": "Антибиотики", "price": 100, "chequeid": 1},
        {"category1": "Медикаменты", "category2": "Витамины", "price": 200, "chequeid": 2},
        {"category1": "Медикаменты", "category2": "Витамины", "price": 50, "chequeid": 3},
        {"category1": "Продукты", "category2": "Фрукты", "price": 999, "chequeid": 4},
    ]

    result = aggregate_category2_by_category1(records, "Медикаменты")

    assert len(result) == 2
    names = {row["group_name"] for row in result}
    assert names == {"Антибиотики", "Витамины"}
    vitamins = next(row for row in result if row["group_name"] == "Витамины")
    assert vitamins["count"] == 2
    assert vitamins["cheque_count"] == 2
    assert pytest.approx(vitamins["total"], 0.01) == 250


def test_extract_period_from_message_detects_range():
    start, end = extract_period_from_message(
        "группируй по категории1 с 01.11.2025 по 30.11.2025"
    )
    assert start == "01.11.2025"
    assert end == "30.11.2025"


def test_resolve_period_defaults_to_current_month():
    user_id = 12345
    context_manager.clear_context(user_id)
    expected = get_current_month()
    resolved = resolve_period_for_message(user_id, "группируй по категории1")
    assert resolved == expected


def test_resolve_period_uses_last_query_if_present():
    user_id = 54321
    context_manager.clear_context(user_id)
    context_manager.set_last_query(
        user_id,
        "fetch_by_period",
        {"start_date": "05.11.2025", "end_date": "07.11.2025"},
        [],
        "test_user",
    )
    resolved = resolve_period_for_message(user_id, "группируй по категории1")
    assert resolved == ("05.11.2025", "07.11.2025")
    context_manager.clear_context(user_id)

