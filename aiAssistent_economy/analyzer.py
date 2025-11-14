from __future__ import annotations

import os
from typing import List, Dict


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROMPT_PATH = os.path.join(BASE_DIR, "prompt.txt")
LAST_REQUEST_PATH = os.path.join(BASE_DIR, "last_request.txt")


def _load_system_prompt() -> str:
    try:
        with open(PROMPT_PATH, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return (
            "Ты финансовый аналитик. Проанализируй траты пользователя по категориям и дай конкретные рекомендации по экономии."
        )


def _format_grouped_data(grouped_data: List[Dict]) -> str:
    if not grouped_data:
        return "Нет данных"

    total_sum = sum(float(item.get("total", 0) or 0.0) for item in grouped_data)
    lines = []
    if total_sum == 0:
        total_sum = 1.0  # избегаем деления на ноль

    # сортируем по сумме убыванию
    sorted_items = sorted(grouped_data, key=lambda x: float(x.get("total", 0) or 0.0), reverse=True)

    for idx, item in enumerate(sorted_items, start=1):
        name = item.get("group_name") or "Без названия"
        total = float(item.get("total", 0) or 0.0)
        count = int(item.get("count", 0) or 0)
        cheques = int(item.get("cheque_count", 0) or 0)
        share = (total / total_sum) * 100 if total_sum else 0.0
        lines.append(
            f"{idx}. {name}: сумма {total:.2f} ₽ (доля {share:.1f}%), позиций {count}, чеков {cheques}"
        )

    return "\n".join(lines)


def build_request_text(grouped_data: List[Dict], start_date: str, end_date: str) -> str:
    total_sum = sum(float(item.get("total", 0) or 0.0) for item in grouped_data)
    categories_block = _format_grouped_data(grouped_data)

    request_text = (
        f"Период анализа: {start_date} - {end_date}\n"
        f"Общий объём расходов: {total_sum:.2f} ₽\n\n"
        f"Категории:\n{categories_block}\n"
    )

    return request_text.strip()


def save_request_text(text: str) -> None:
    try:
        with open(LAST_REQUEST_PATH, "w", encoding="utf-8") as f:
            f.write(text)
    except Exception:
        # не критично, если не удалось сохранить
        pass


def generate_economy_advice(ai_client, grouped_data: List[Dict], start_date: str, end_date: str) -> str:
    if not grouped_data:
        return ""

    system_prompt = _load_system_prompt()
    request_text = build_request_text(grouped_data, start_date, end_date)
    save_request_text(request_text)

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": request_text},
    ]

    response = ai_client.get_response(messages)
    
    if response.get("error"):
        error_message = response.get("content", "Не удалось обработать запрос")
        return error_message
    
    content = (response.get("content") or "").strip()
    return content






