import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_ROOT = Path(__file__).resolve().parent
if str(TEST_ROOT) not in sys.path:
    sys.path.insert(0, str(TEST_ROOT))

from cheque_parser_with_raw import parse_cheque_with_gpt_raw  # noqa: E402

CHEQUE_TEXT_DIR = PROJECT_ROOT / ".chequeData" / "111"


def pick_latest_cheque_text() -> Path:
    candidates = [p for p in CHEQUE_TEXT_DIR.glob("*.txt") if p.is_file()]
    if not candidates:
        raise FileNotFoundError(f"Не нашёл ни одного .txt файла в {CHEQUE_TEXT_DIR}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def main(path: str | None = None) -> None:
    if path:
        image_path = Path(path).expanduser().resolve()
    else:
        print(f"[1/4] Ищу текстовый чек в {CHEQUE_TEXT_DIR} ...")
        image_path = pick_latest_cheque_text()
    if not image_path.exists():
        raise FileNotFoundError(image_path)
    print(f"[2/4] Использую файл: {image_path}")
    mod_time = datetime.fromtimestamp(image_path.stat().st_mtime)
    print(f"      Последняя модификация: {mod_time}")

    print("[3/5] Отправляю на парсинг через копию parser.cheque_parser ...")
    items, raw_text = parse_cheque_with_gpt_raw(str(image_path))

    print("[4/5] Сырый ответ модели (без форматирования):")
    print(raw_text)

    print("[5/5] Результат после json.loads:")
    print(json.dumps(items, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    user_path = sys.argv[1] if len(sys.argv) > 1 else None
    main(user_path)

