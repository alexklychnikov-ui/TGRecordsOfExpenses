"""Helper functions for date calculations."""
from datetime import datetime, timedelta
import calendar


def get_last_n_days(n: int) -> tuple[str, str]:
    """Возвращает период последних N дней (включая сегодня)."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=n-1)
    return start_date.strftime("%d.%m.%Y"), end_date.strftime("%d.%m.%Y")


def get_current_week() -> tuple[str, str]:
    """Возвращает период текущей недели (с понедельника по сегодня)."""
    today = datetime.now()
    start_of_week = today - timedelta(days=today.weekday())
    return start_of_week.strftime("%d.%m.%Y"), today.strftime("%d.%m.%Y")


def get_current_month() -> tuple[str, str]:
    """Возвращает период текущего месяца (с 1 числа по сегодня)."""
    today = datetime.now()
    start_of_month = today.replace(day=1)
    return start_of_month.strftime("%d.%m.%Y"), today.strftime("%d.%m.%Y")


def get_yesterday() -> tuple[str, str]:
    """Возвращает дату вчера."""
    now = datetime.now()
    target = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    formatted = target.strftime("%d.%m.%Y")
    return formatted, formatted


def get_previous_month() -> tuple[str, str]:
    """Возвращает полный предыдущий месяц."""
    today = datetime.now()
    first_of_current = today.replace(day=1)
    last_day_prev = first_of_current - timedelta(days=1)
    start_prev = last_day_prev.replace(day=1)
    start = start_prev.strftime("%d.%m.%Y")
    end = last_day_prev.strftime("%d.%m.%Y")
    return start, end


def get_previous_year() -> tuple[str, str]:
    """Возвращает полный предыдущий год."""
    today = datetime.now()
    prev_year = today.year - 1
    start = datetime(prev_year, 1, 1).strftime("%d.%m.%Y")
    end = datetime(prev_year, 12, 31).strftime("%d.%m.%Y")
    return start, end


def get_last_n_months(n: int) -> tuple[str, str]:
    """Возвращает период последних N месяцев."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30*n)
    return start_date.strftime("%d.%m.%Y"), end_date.strftime("%d.%m.%Y")


def get_full_current_month() -> tuple[str, str]:
    """Возвращает полный текущий месяц (с 1 по последний день месяца)."""
    today = datetime.now()
    start = today.replace(day=1)
    last_day = calendar.monthrange(today.year, today.month)[1]
    end = today.replace(day=last_day)
    return start.strftime("%d.%m.%Y"), end.strftime("%d.%m.%Y")


def _parse_ddmmyyyy(s: str) -> datetime | None:
    try:
        return datetime.strptime(s, "%d.%m.%Y")
    except Exception:
        return None


def normalize_to_current_month_if_same_month_wrong_year(start_date: str, end_date: str) -> tuple[str, str]:
    """If the provided period has a wrong year (e.g., 2023 instead of current year), 
    correct it to current year. Always use current year for date calculations."""
    now = datetime.now()
    current_year = now.year
    ds = _parse_ddmmyyyy(start_date)
    de = _parse_ddmmyyyy(end_date)
    if not ds or not de:
        return start_date, end_date
    
    # Если год не совпадает с текущим - исправляем на текущий год
    if ds.year != current_year or de.year != current_year:
        # Исправляем год на текущий
        start = f"{ds.day:02d}.{ds.month:02d}.{current_year}"
        # Для конечной даты: если это последний день месяца в старом году, используем последний день текущего месяца
        # Иначе просто исправляем год
        if de.day == 31 or (de.month == 2 and de.day == 28) or (de.month in [4,6,9,11] and de.day == 30):
            # Похоже на последний день месяца - используем последний день текущего месяца
            import calendar
            last_day = calendar.monthrange(current_year, de.month)[1]
            end = f"{last_day:02d}.{de.month:02d}.{current_year}"
        else:
            end = f"{de.day:02d}.{de.month:02d}.{current_year}"
        return start, end
    
    return start_date, end_date


def parse_period_string(period: str) -> tuple[str, str] | None:
    """Парсит строки типа 'за неделю', 'за 7 дней', 'за октябрь'."""
    period_lower = period.lower().strip()
    
    if "вчера" in period_lower or "вчераш" in period_lower or ("прошл" in period_lower and "день" in period_lower) or "last day" in period_lower or "yesterday" in period_lower:
        return get_yesterday()
    
    if "недел" in period_lower or "week" in period_lower:
        return get_current_week()
    
    if "прошл" in period_lower and ("месяц" in period_lower or "month" in period_lower):
        return get_previous_month()
    
    if "прошл" in period_lower and ("год" in period_lower or "year" in period_lower):
        return get_previous_year()
    
    if "месяц" in period_lower or "month" in period_lower:
        return get_current_month()
    
    import re
    days_match = re.search(r'(\d+)\s*дн', period_lower)
    if days_match:
        n = int(days_match.group(1))
        return get_last_n_days(n)
    
    months = {
        "январ": "01", "феврал": "02", "март": "03", "апрел": "04",
        "ма": "05", "июн": "06", "июл": "07", "август": "08",
        "сентябр": "09", "октябр": "10", "ноябр": "11", "декабр": "12"
    }
    
    for month_name, month_num in months.items():
        if month_name in period_lower:
            year_match = re.search(r'20\d{2}', period_lower)
            now = datetime.now()
            year = year_match.group(0) if year_match else now.strftime("%Y")
            start = f"01.{month_num}.{year}"
            # Для текущего месяца конец = сегодня, иначе последний день месяца
            if int(year) == now.year and int(month_num) == now.month:
                end = now.strftime("%d.%m.%Y")
            else:
                import calendar
                last_day = calendar.monthrange(int(year), int(month_num))[1]
                end = f"{last_day}.{month_num}.{year}"
            return start, end
    
    return None






