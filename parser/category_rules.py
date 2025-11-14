import json
import os
from typing import Tuple, List, Iterable
from config import CATEGORY_RULES_PATH


# Built-in fallback rules
_FALLBACK_RULES: list[tuple[tuple[str, ...], tuple[str, str, str]]] = [
    (("молоко", "йогурт", "кефир", "сыр", "творог"), ("Продукты питания", "Молочные продукты", "Прочее")),
    (("хлеб", "батон", "булка", "лаваш"), ("Продукты питания", "Хлебобулочные изделия", "Хлеб")),
    (("яблок", "банан", "фрукт", "овощ", "огурец", "помидор"), ("Продукты питания", "Фрукты и овощи", "Прочее")),
    (("вода", "сок", "лимонад", "квас", "напиток"), ("Продукты питания", "Напитки", "Безалкогольные")),
    (("пиво", "вино", "водка", "коньяк"), ("Продукты питания", "Напитки", "Алкогольные")),
    (("сахар", "соль", "специи"), ("Продукты питания", "Бакалея", "Прочее")),
    (("масло", "подсолнеч", "оливков", "рафинированное"), ("Продукты питания", "Масла", "Растительные")),
    (("куриц", "свинин", "говядин", "фарш", "колбас"), ("Продукты питания", "Мясо", "Прочее")),
    (("шампунь", "мыло", "зубн", "паста", "щетка"), ("Быт", "Гигиена", "Прочее")),
    (("салфет", "бумага", "туалетн"), ("Быт", "Хозтовары", "Бумажные")),
]


def _load_rules_from_json() -> list[tuple[tuple[str, ...], tuple[str, str, str]]]:
    if not os.path.isfile(CATEGORY_RULES_PATH):
        return _FALLBACK_RULES
    try:
        with open(CATEGORY_RULES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        rules: list[tuple[tuple[str, ...], tuple[str, str, str]]] = []
        for entry in data:
            kws = tuple(str(k).lower() for k in entry.get("keywords", []))
            cat = entry.get("category", [])
            if len(kws) == 0 or len(cat) != 3:
                continue
            rules.append((kws, (str(cat[0]), str(cat[1]), str(cat[2]))))
        return rules or _FALLBACK_RULES
    except Exception:
        return _FALLBACK_RULES


_RULES = _load_rules_from_json()


def categorize_product(product_name: str) -> Tuple[str, str, str]:
    name = (product_name or "").lower()
    for keywords, cats in _RULES:
        if any(k in name for k in keywords):
            return cats
    return ("Прочее", "Прочее", "Прочее")


def get_active_rules() -> List[Tuple[Tuple[str, ...], Tuple[str, str, str]]]:
    return list(_RULES)


def validate_rules_file(path: str) -> tuple[bool, List[str]]:
    errors: List[str] = []
    if not os.path.isfile(path):
        return False, [f"Rules file not found: {path}"]
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return False, ["JSON root must be a list"]
        for i, entry in enumerate(data):
            if not isinstance(entry, dict):
                errors.append(f"[{i}] entry must be an object")
                continue
            kws = entry.get("keywords")
            cat = entry.get("category")
            if not isinstance(kws, list) or not kws:
                errors.append(f"[{i}] keywords must be non-empty list")
            if not isinstance(cat, list) or len(cat) != 3:
                errors.append(f"[{i}] category must be list of 3 strings")
        return (len(errors) == 0), errors
    except Exception as e:
        return False, [f"Exception: {e}"]


_FOOD_LEVEL2 = {
    "Напитки",
    "Хлебобулочные изделия",
    "Молочные продукты",
    "Фрукты",
    "Овощи",
    "Фрукты и овощи",
    "Бакалея",
    "Мясо",
    "Масла",
}

_HOUSEHOLD_LEVEL2 = {"Гигиена", "Хозтовары"}


def normalize_categories(product_name: str, c1: str | None, c2: str | None, c3: str | None) -> Tuple[str, str, str]:
    n1 = (c1 or "").strip()
    n2 = (c2 or "").strip()
    n3 = (c3 or "").strip()

    # If model returned level2 in level1 for food categories, shift under "Продукты питания"
    if n1 in _FOOD_LEVEL2:
        return ("Продукты питания", n1, n2 or n3 or "Прочее")

    # Special case: alcohol often comes as level1
    if n1 == "Алкоголь":
        return ("Продукты питания", "Напитки", "Алкогольные")

    # Household categories sometimes returned as level1
    if n1 in _HOUSEHOLD_LEVEL2:
        return ("Быт", n1, n2 or n3 or "Прочее")

    # If category1 empty but category2 present, promote appropriately
    if not n1 and n2:
        if n2 in _FOOD_LEVEL2:
            return ("Продукты питания", n2, n3 or "Прочее")
        if n2 in _HOUSEHOLD_LEVEL2:
            return ("Быт", n2, n3 or "Прочее")

    # Default: ensure non-empty strings
    return (n1 or "Прочее", n2 or "Прочее", n3 or "Прочее")


