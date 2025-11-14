# Records Of Expenses V3

Унифицированный Telegram-бот для ведения личных расходов: распознаёт чеки с помощью OpenAI, хранит данные в SQLite, строит отчёты и отвечает на вопросы через AI-ассистента.

---

## Возможности
- Парсинг чеков из фото и текстовых файлов (GPT-4o mini vision)
- Сохранение покупок в SQLite с фильтрацией по Telegram-username
- Поиск дубликатов и хранение медиа в `.chequeData/<username>`
- Контекстные диалоги (до 20 сообщений на пользователя)
- Просмотр чеков и отправка связанных фотографий
- Отчёты, агрегаты, Excel-выгрузки и диаграммы
- Экономические рекомендации на основе исторических трат
- Обновление позиций чека (цены, категории, описания) по голосовым командам

---

## Архитектура
```
RecordsOfExpensesV3/
├── bot_unified.py          # единая точка входа
├── config.py               # загрузка .env, пути к данным
├── requirements.txt
├── .chequeData/            # папки с оригинальными файлами чеков
├── .dbData/                # база SQLite и отчёты
├── aiAssistant/            # AI-логика и Telegram-бот (aiogram 3.x)
│   ├── telegram/bot.py     # хендлеры, диалоги, вызовы инструментов
│   ├── core/               # контекст и OpenAI клиент
│   ├── db/                 # расширенная аналитика БД
│   ├── reports/            # форматирование текстов и диаграмм
│   └── charts/             # построение круговых диаграмм
├── aiAssistent_economy/    # генератор советов по экономии
├── parser/                 # распознавание чеков и категоризация
├── db/                     # базовый менеджер SQLite
└── Export2Excel/           # выгрузки Excel (детальные и агрегированные)
```

---

## Требования
- Python 3.11+
- Telegram Bot API token (`BotFather`)
- OpenAI API key (совместимый с `gpt-4o-mini`)
- Рабочая директория с доступом на запись для `.chequeData` и `.dbData`

`requirements.txt`:
```
openai>=1.43.0
requests>=2.31.0
pytest>=7.4.0
aiogram>=3.4.0
openpyxl>=3.1.2
matplotlib>=3.7.0
```

---

## Установка
```bash
git clone <repo-url> RecordsOfExpensesV3
cd RecordsOfExpensesV3
python -m venv .venv
.venv\Scripts\activate          # Windows
source .venv/bin/activate       # macOS/Linux
pip install -r requirements.txt
```

---

## Настройка окружения
1. Создайте `.env` в корне:
   ```env
   TELEGRAM_BOT_TOKEN=000000:XXXXXXXXXXXXXXX
   OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxx
   OPENAI_MODEL=gpt-4o-mini
   SQLITE_PATH=./.dbData/receipts.db
   RECEIPTS_MEDIA_DIR=./.chequeData
   DEFAULT_LOCALE=ru
   ```
2. Либо задайте переменные окружения, либо отредактируйте `config.py`.
3. При первом запуске директории `.chequeData` и `.dbData` будут созданы автоматически, база проинициализируется функцией `init_db`.

---

## Запуск
```bash
python bot_unified.py
```

При старте бот выводит краткую справку в консоль и дальше работает через aiogram polling. Остановка — `Ctrl+C`.

---

## Как это работает
- **Приём чеков**: пользователь отправляет фото → `aiAssistant.telegram.bot` сохраняет файл в `.chequeData/<username>` и вызывает `parser.cheque_parser.parse_cheque_with_gpt`.
- **Парсинг**: модуль `parser` запрашивает GPT-4o mini, затем нормализует категории через `category_rules`.
- **Запись в БД**: `db.db_manager.bulk_insert_purchases` сохраняет строки в SQLite (`.dbData/receipts.db`), индексы `idx_username`, `idx_date_username_org` ускоряют выборки.
- **Диалоги**: сообщения проходят через `ContextManager`, AI-инструменты описаны в `AIClient.get_tools_definition`. Ответы могут запускать SQL-аналитику, экспорт в Excel или генерацию диаграмм.
- **Экономия расходов**: если запрос содержит ключевые слова (экономия, сократить и т.п.), активируется `aiAssistent_economy.service.process_economy_request` — строится отчёт по категориям и генерируется текстовый совет.

---

## Команды и примеры
- `/start` — приветствие и инструкция
- `/clear` — сбросить контекст диалога

Примерные запросы:
```
покажи последний чек
→ текст + фото, chequeid запоминается для последующих команд

общая сумма за последние 7 дней
→ агрегированная статистика

покажи траты в пятёрочке за октябрь
→ список записей + возможность выгрузки в Excel

измени цену у записи 128 на 199.90
→ обновление строки в базе

дай совет как сократить расходы за месяц
→ отчёт по категориям + рекомендации
```

---

## Инструменты AI (основные функции)
- `get_last_n_days`, `get_current_week`, `get_current_month`, `fetch_by_period`
- `get_summary_last_n_days`, `get_summary_week`, `get_summary_month`, `get_summary`
- `get_last_cheque`, `get_cheque_by_id`
- `fetch_by_category`, `fetch_by_organization`, `fetch_by_product_name`, `fetch_by_description`
- `update_description_by_cheque`, `update_description_by_organization`
- `update_record`, `update_field_by_cheque`
- `get_grouped_stats*` с фильтрами и построением диаграмм
- `export_to_excel`, `export_grouped_to_excel`, `_export_filtered_to_excel`

Все функции автоматически подставляют `username` пользователя и ограничивают выборки его данными.

---

## Экспорт и отчёты
- **Excel**: сохраняется в `.dbData/Report_<user_id>.xlsx`
- **Диаграммы**: создаются через `aiAssistant.charts.chart_builder.create_pie_chart`
- **Текстовые отчёты**: форматируются в `aiAssistant.reports.report_builder`

---

## Обновление и сопровождение
- Индексация и фильтрация по дате реализованы в `aiAssistant.db.db_manager`
- Контекст хранится в памяти процесса; после рестарта контекст очищается
- Категоризация управляется файлами `parser/category_rules.json` и `aiAssistent_economy/prompt.txt`
- Логи выводятся через стандартный `logging` (уровень INFO)

---

## Решение проблем
- **TELEGRAM_BOT_TOKEN not set** — заполните `.env` или `config.py`
- **OPENAI_API_KEY не установлен** — проверьте ключ, формат должен начинаться с `sk-`
- **openai package is required** — убедитесь, что зависимости установлены
- **Бот молчит** — бот должен быть в списке разрешённых у пользователя и не быть перезапущенным слишком часто (ограничения Telegram)
- **Ошибки БД** — удалите блокировки, проверьте права записи в `.dbData`

---

## Версия
- Unified Bot v2.1.0 (08.11.2025)
- Единая документация сведена в этот файл; все прочие `.md` удалены как дубликаты.

