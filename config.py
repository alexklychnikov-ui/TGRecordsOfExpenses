import os

# Paths
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Minimal .env loader (no external deps)
# .env has priority over existing environment variables
_ENV_PATH = os.path.join(PROJECT_ROOT, ".env")
if os.path.isfile(_ENV_PATH):
    try:
        with open(_ENV_PATH, "r", encoding="utf-8-sig") as _f:
            for _line in _f:
                _line = _line.strip()
                if _line.startswith("\ufeff"):
                    _line = _line.lstrip("\ufeff")
                if not _line or _line.startswith("#"):
                    continue
                if "=" in _line:
                    _k, _v = _line.split("=", 1)
                    if _k and _v is not None:
                        _k = _k.strip().lstrip("\ufeff")
                        _val = _v.strip().lstrip("\ufeff")
                        if (len(_val) >= 2 and ((_val[0] == '"' and _val[-1] == '"') or (_val[0] == "'" and _val[-1] == "'"))):
                            _val = _val[1:-1]
                        os.environ[_k] = _val
    except Exception:
        pass

# Prefer env vars; fallback to hardcoded values for local development
# You can set these here if you don't want to use .env file
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "YOUR_OPENAI_KEY")

# Alternative: hardcode your API keys here as global variables if you prefer
# TELEGRAM_BOT_TOKEN = "your-actual-telegram-bot-token"
# OPENAI_API_KEY = "your-actual-openai-api-key"

# Other paths
CHEQUE_DIR = os.path.join(PROJECT_ROOT, ".chequeData")
DB_DIR = os.path.join(PROJECT_ROOT, ".dbData")
DB_PATH = os.path.join(DB_DIR, "receipts.db")
CATEGORY_RULES_PATH = os.path.join(PROJECT_ROOT, "parser", "category_rules.json")

