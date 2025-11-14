import base64
import json
import os
import re
from typing import List, Dict, Optional

# ensure project root on sys.path when running this file directly
import sys
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config import OPENAI_API_KEY
from .category_rules import categorize_product, normalize_categories

key = OPENAI_API_KEY


def _read_file_as_base64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def _load_parsing_prompt() -> str:
    prompt_path = os.path.join(os.path.dirname(__file__), "prompt.txt")
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read().strip()


def parse_cheque_with_gpt(
    image_path: str,
    hint_text: Optional[str] = None,
    enrich_categories: bool = False,
    preparsed_text: Optional[str] = None,
) -> List[Dict]:
    try:
        from openai import OpenAI  # lazy import to keep deps minimal for non-parse flows
    except Exception as exc:
        raise RuntimeError("openai package is required for parsing") from exc

    if not key or key == "YOUR_OPENAI_KEY" or key.strip() == "":
        raise RuntimeError(
            "OPENAI_API_KEY не установлен!\n"
            "Установите ключ одним из способов:\n"
            "1. В файле .env: OPENAI_API_KEY=sk-...\n"
            "2. В config.py: раскомментируйте строку 32\n"
            "3. Глобальная переменная: set OPENAI_API_KEY=sk-..."
        )
    
    if not key.startswith("sk-"):
        raise RuntimeError(
            f"OPENAI_API_KEY имеет неверный формат!\n"
            f"Ключ должен начинаться с 'sk-'\n"
            f"Текущее значение: {key[:10]}..."
        )

    try:
        client = OpenAI(api_key=key, timeout=60.0)
    except Exception as e:
        raise RuntimeError(
            f"Ошибка инициализации OpenAI клиента!\n"
            f"Проверьте корректность ключа (начинается с 'sk-proj-' или 'sk-')\n"
            f"Ошибка: {str(e)}"
        )

    ext = os.path.splitext(image_path)[1].lower()
    # Handle text receipts as well (e.g., .txt exported data) or externally prepared text
    if preparsed_text is not None or ext in (".txt", ".json"):
        try:
            if preparsed_text is not None:
                text_receipt = preparsed_text
            else:
                with open(image_path, "r", encoding="utf-8") as f:
                    text_receipt = f.read()
        except UnicodeDecodeError:
            with open(image_path, "r", encoding="cp1251") as f:
                text_receipt = f.read()

        system_prompt = _load_parsing_prompt()
        
        user_content = (hint_text + "\n\n" if hint_text else "") + text_receipt

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            temperature=0.1,
        )
    else:
        image_b64 = _read_file_as_base64(image_path)
        mime = "image/jpeg" if ext in (".jpg", ".jpeg") else "image/png"

        system_prompt = _load_parsing_prompt()
        
        user_text = "Извлеки позиции покупок из этого чека."
        if hint_text:
            user_text += "\n\nПодсказка: " + hint_text

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_text},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:{mime};base64,{image_b64}"},
                        },
                    ],
                },
            ],
        )

    content = response.choices[0].message.content
    # Try to extract JSON if model wrapped it in code fences
    text = content.strip()
    if text.startswith("```"):
        # strip outer triple backticks and optional language tag
        # ```json\n...\n```
        m = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
        if m:
            text = m.group(1).strip()
    # Fallback: remove a leading 'json' line if present
    if text.lower().startswith("json\n"):
        text = text.split("\n", 1)[1]

    parsed = json.loads(text)
    if not isinstance(parsed, list):
        raise ValueError("Expected a JSON array from the model")
    # post-process: enrich categories via separate classification request per item
    def classify_categories_via_gpt(name: str) -> Optional[Dict[str, str]]:
        try:
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
                        "content": f"Наименование: {name}",
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
        except Exception:
            pass
        return None

    for item in parsed:
        name = item.get("product_name") or ""
        # optional enrichment disabled by default; keep structure simple to avoid syntax issues
        if not item.get("category1") or not item.get("category2"):
            c1, c2, c3 = categorize_product(name)
            item.setdefault("category1", c1)
            item.setdefault("category2", c2)
            item.setdefault("category3", c3)
        c1n, c2n, c3n = normalize_categories(
            name,
            item.get("category1"),
            item.get("category2"),
            item.get("category3"),
        )
        item["category1"], item["category2"], item["category3"] = c1n, c2n, c3n
    return parsed

