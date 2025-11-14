"""OpenAI API client for AI assistant."""
import os
import json
import logging
from typing import List, Dict, Any, Optional
from openai import OpenAI
from openai import APIError, APITimeoutError, APIConnectionError, RateLimitError

logger = logging.getLogger(__name__)


class AIClient:
    def __init__(self, api_key: Optional[str] = None):
        from config import OPENAI_API_KEY
        self.api_key = api_key or OPENAI_API_KEY
        
        if not self.api_key or self.api_key == "YOUR_OPENAI_KEY" or self.api_key.strip() == "":
            raise RuntimeError(
                "OPENAI_API_KEY не установлен!\n"
                "Установите ключ одним из способов:\n"
                "1. В файле .env: OPENAI_API_KEY=sk-...\n"
                "2. В config.py: раскомментируйте строку 32\n"
                "3. Глобальная переменная: set OPENAI_API_KEY=sk-..."
            )
        
        if not self.api_key.startswith("sk-"):
            raise RuntimeError(
                f"OPENAI_API_KEY имеет неверный формат!\n"
                f"Ключ должен начинаться с 'sk-'\n"
                f"Текущее значение: {self.api_key[:10]}..."
            )
        
        try:
            self.client = OpenAI(api_key=self.api_key, timeout=90.0)
        except Exception as e:
            raise RuntimeError(
                f"Ошибка инициализации OpenAI клиента!\n"
                f"Проверьте корректность ключа и баланс аккаунта\n"
                f"Ошибка: {str(e)}"
            )
        
        self.model = "gpt-4o-mini"
    
    def _get_user_friendly_error_message(self, error: Exception) -> str:
        """Преобразует техническую ошибку в понятное сообщение для пользователя."""
        error_str = str(error).lower()
        error_type = type(error).__name__
        
        if isinstance(error, APITimeoutError) or "timeout" in error_str or "timed out" in error_str:
            return "Запрос занял слишком много времени. Попробуйте упростить запрос или повторить позже"
        
        if isinstance(error, RateLimitError) or "rate limit" in error_str or "429" in error_str:
            return "Слишком много запросов. Подождите немного и попробуйте снова"
        
        if isinstance(error, APIError):
            status_code = getattr(error, 'status_code', None)
            if status_code == 403:
                return "Сейчас не могу обработать запрос. Попробуйте позже или уточните запрос"
            if status_code == 401:
                return "Проблема с доступом. Попробуйте позже"
            if status_code == 429:
                return "Слишком много запросов. Подождите немного и попробуйте снова"
        
        if "403" in error_str or "forbidden" in error_str or "unsupported_country" in error_str or "region" in error_str:
            return "Сейчас не могу обработать запрос. Попробуйте позже или уточните запрос"
        
        if isinstance(error, APIConnectionError) or "connection" in error_str or "network" in error_str:
            return "Проблема с подключением. Проверьте интернет и попробуйте снова"
        
        logger.error(f"Unhandled API error type: {error_type}, message: {str(error)}")
        return "Не удалось обработать запрос. Попробуйте переформулировать или повторить позже"
    
    def get_response(self, messages: List[Dict[str, str]], tools: Optional[List[Dict]] = None) -> Dict[str, Any]:
        try:
            if tools:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    tools=tools,
                    tool_choice="auto",
                    temperature=0.3,
                    timeout=90.0
                )
            else:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=0.3,
                    timeout=90.0
                )
            
            return {
                "content": response.choices[0].message.content,
                "tool_calls": response.choices[0].message.tool_calls if hasattr(response.choices[0].message, 'tool_calls') else None,
                "error": None
            }
        except APITimeoutError as e:
            logger.error(f"API timeout error: {str(e)}")
            user_message = self._get_user_friendly_error_message(e)
            return {"content": user_message, "tool_calls": None, "error": "timeout"}
        except RateLimitError as e:
            logger.error(f"Rate limit error: {str(e)}")
            user_message = self._get_user_friendly_error_message(e)
            return {"content": user_message, "tool_calls": None, "error": "rate_limit"}
        except APIError as e:
            logger.error(f"API error (code: {getattr(e, 'status_code', 'unknown')}): {str(e)}")
            user_message = self._get_user_friendly_error_message(e)
            return {"content": user_message, "tool_calls": None, "error": "api_error"}
        except APIConnectionError as e:
            logger.error(f"API connection error: {str(e)}")
            user_message = self._get_user_friendly_error_message(e)
            return {"content": user_message, "tool_calls": None, "error": "connection"}
        except Exception as e:
            logger.error(f"Unexpected error in get_response: {type(e).__name__}: {str(e)}")
            user_message = self._get_user_friendly_error_message(e)
            return {"content": user_message, "tool_calls": None, "error": "unknown"}
    
    def get_tools_definition(self) -> List[Dict]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "get_last_n_days",
                    "description": "Получить записи за последние N дней (включая сегодня). Например: 'за последние 7 дней' - используй эту функцию с n=7",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "n": {"type": "integer", "description": "Количество дней"}
                        },
                        "required": ["n"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "export_all_to_excel",
                    "description": "Выгрузить все записи текущего пользователя в Excel-файл (.dbData/Report.xlsx)",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "export_to_excel_by_period",
                    "description": "Выгрузить записи за период в Excel-файл (.dbData/Report.xlsx). Используй совместно с датами из хелперов периодов.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "start_date": {"type": "string", "description": "Дата начала DD.MM.YYYY"},
                            "end_date": {"type": "string", "description": "Дата конца DD.MM.YYYY"}
                        },
                        "required": ["start_date", "end_date"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "export_group_items_to_excel",
                    "description": "Выгрузить в Excel все детальные записи (карточки товаров) по указанному сгруппированному значению из предыдущего запроса. Использует период (даты) из последнего запроса группировки. Например: 'выгрузи в эксель все карточки по категории Продукты питания' - выгрузит все записи где category1='Продукты питания' за тот же период, что был в предыдущем запросе.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "group_value": {"type": "string", "description": "Значение группы для фильтрации (например: 'Продукты питания', 'Электроника', название организации и т.д.). Должно точно совпадать с одним из значений в результатах предыдущего запроса группировки."}
                        },
                        "required": ["group_value"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_current_week",
                    "description": "Получить записи за текущую неделю (с понедельника по сегодня). Используй для запроса 'за неделю'",
                    "parameters": {
                        "type": "object",
                        "properties": {}
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_current_month",
                    "description": "Получить записи за текущий месяц (с 1 числа по сегодня). Используй для запроса 'за месяц'",
                    "parameters": {
                        "type": "object",
                        "properties": {}
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_yesterday",
                    "description": "Получить записи за вчерашний день. Используй для запросов вида 'вчера', 'вчерашний день', 'last day'",
                    "parameters": {
                        "type": "object",
                        "properties": {}
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_previous_month",
                    "description": "Получить записи за прошлый календарный месяц (полностью). Используй для запросов 'прошлый месяц', 'last month'",
                    "parameters": {
                        "type": "object",
                        "properties": {}
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_previous_year",
                    "description": "Получить записи за предыдущий календарный год. Используй для запросов 'прошлый год', 'last year'",
                    "parameters": {
                        "type": "object",
                        "properties": {}
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "fetch_by_period",
                    "description": "Получить записи покупок за конкретный период. Используй когда пользователь указывает точные даты",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "start_date": {"type": "string", "description": "Дата начала в формате DD.MM.YYYY"},
                            "end_date": {"type": "string", "description": "Дата конца в формате DD.MM.YYYY"}
                        },
                        "required": ["start_date", "end_date"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_summary_last_n_days",
                    "description": "Получить общую сумму за последние N дней. Используй для 'общая сумма за 7 дней', 'сколько потратил за 30 дней' и т.п.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "n": {"type": "integer", "description": "Количество дней"}
                        },
                        "required": ["n"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_summary_week",
                    "description": "Получить общую сумму за текущую неделю (с понедельника). Используй для 'сумма за неделю'",
                    "parameters": {
                        "type": "object",
                        "properties": {}
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_summary_month",
                    "description": "Получить общую сумму за текущий месяц. Используй для 'сумма за месяц'",
                    "parameters": {
                        "type": "object",
                        "properties": {}
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_summary",
                    "description": "Получить общую сумму за конкретный период. Используй когда пользователь указывает точные даты",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "start_date": {"type": "string", "description": "Дата начала в формате DD.MM.YYYY"},
                            "end_date": {"type": "string", "description": "Дата конца в формате DD.MM.YYYY"}
                        },
                        "required": ["start_date", "end_date"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_cheque_by_id",
                    "description": "Получить чек текущего пользователя по номеру",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "chequeid": {"type": "integer", "description": "Номер чека"}
                        },
                        "required": ["chequeid"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_last_cheque",
                    "description": "Получить последний чек текущего пользователя",
                    "parameters": {
                        "type": "object",
                        "properties": {}
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "delete_cheque",
                    "description": "Удалить чек текущего пользователя по номеру. Если номер не указан, используется последний просмотренный чек",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "chequeid": {"type": "integer", "description": "Номер чека (необязательно, если не указан - используется последний просмотренный)"}
                        },
                        "required": []
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "add_item_to_cheque",
                    "description": "Добавить новую товарную позицию в существующий чек. Автоматически заполняются дата, организация из чека, категории через AI-классификацию. Если номер чека не указан, используется последний просмотренный чек",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "chequeid": {"type": "integer", "description": "Номер чека (необязательно, если не указан - используется последний просмотренный)"},
                            "product_name": {"type": "string", "description": "Название товара"},
                            "price": {"type": "number", "description": "Цена товара"},
                            "quantity": {"type": "number", "description": "Количество (по умолчанию 1, допускает дробные значения)"},
                            "discount": {"type": "number", "description": "Скидка (по умолчанию 0)"}
                        },
                        "required": ["product_name", "price"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "fetch_by_category",
                    "description": "Получить покупки текущего пользователя по категории",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "level": {"type": "integer", "description": "Уровень категории (1, 2 или 3)"},
                            "name": {"type": "string", "description": "Название категории"}
                        },
                        "required": ["level", "name"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "fetch_by_organization",
                    "description": "Получить покупки текущего пользователя по организации. Ищет по вхождению текста (например: 'лента' найдет 'Лента', 'ЛЕНТА', 'Магазин Лента' и т.д.)",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "organization": {"type": "string", "description": "Название или часть названия организации"}
                        },
                        "required": ["organization"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "fetch_by_product_name",
                    "description": "Получить покупки по названию товара. Ищет по вхождению текста (например: 'молоко' найдет 'Молоко', 'Молоко 2.5%', 'Молоко домик' и т.д.)",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "product_name": {"type": "string", "description": "Название или часть названия товара"}
                        },
                        "required": ["product_name"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "fetch_by_description",
                    "description": "Получить покупки по комментарию/описанию. Ищет по вхождению текста в поле description",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "description": {"type": "string", "description": "Текст для поиска в комментариях"}
                        },
                        "required": ["description"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "update_description_by_cheque",
                    "description": "Добавить или изменить комментарий к чеку текущего пользователя. Если номер чека не указан, используется последний просмотренный чек. ВСЕГДА передавай параметр 'description' с текстом комментария",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "chequeid": {"type": "integer", "description": "Номер чека (необязательно, если не указан - используется последний просмотренный)"},
                            "description": {"type": "string", "description": "Текст комментария (обязательно, например: 'авоська', 'рабочие расходы' и т.д.)"}
                        },
                        "required": ["description"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "update_description_by_organization",
                    "description": "Добавить комментарий ко всем чекам текущего пользователя в указанной организации",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "organization": {"type": "string", "description": "Название организации"},
                            "description": {"type": "string", "description": "Текст комментария"}
                        },
                        "required": ["organization", "description"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "update_record",
                    "description": "Обновить конкретное поле записи по ID. ID = номер позиции (строки) в чеке, который показывается рядом с товаром. Используй для команд вида 'Позиция 102 измени цену 123.45' или 'Измени наименование товара в позиции 102 …'",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "record_id": {"type": "integer", "description": "ID записи"},
                            "field": {"type": "string", "description": "Название поля (price, discount, product_name, description, quantity, category1, category2, category3, organization, date)"},
                            "value": {"type": "string", "description": "Новое значение"}
                        },
                        "required": ["record_id", "field", "value"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "update_field_by_cheque",
                    "description": "Обновить поле у всех записей конкретного чека текущего пользователя (например, дату у чека). Если номер чека не указан, используется последний просмотренный чек",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "chequeid": {"type": "integer", "description": "Номер чека (необязательно, если не указан - используется последний просмотренный)"},
                            "field": {"type": "string", "description": "Поле (например, date, description, organization)"},
                            "value": {"type": "string", "description": "Новое значение"}
                        },
                        "required": ["field", "value"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_grouped_by_category1",
                    "description": "Группировка по категории 1 уровня за период. Показывает сумму по каждой категории. Если пользователь просит график или диаграмму, используй эту функцию - она автоматически построит круговую диаграмму. ВАЖНО: всегда используй текущий год (2025) в датах, никогда не используй старые годы типа 2023!",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "start_date": {"type": "string", "description": "Дата начала DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"},
                            "end_date": {"type": "string", "description": "Дата конца DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"}
                        },
                        "required": ["start_date", "end_date"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_grouped_by_category2",
                    "description": "Группировка по категории 2 уровня за период. Показывает сумму по каждой категории. Если пользователь просит график или диаграмму, используй эту функцию - она автоматически построит круговую диаграмму. ВАЖНО: всегда используй текущий год (2025) в датах, никогда не используй старые годы типа 2023!",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "start_date": {"type": "string", "description": "Дата начала DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"},
                            "end_date": {"type": "string", "description": "Дата конца DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"}
                        },
                        "required": ["start_date", "end_date"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_grouped_stats_filtered",
                    "description": "Группировка по полю (category1/2/3, organization, description) за период с дополнительными фильтрами (например: category1=Напитки). Если пользователь просит график или диаграмму, используй эту функцию - она автоматически построит круговую диаграмму. ВАЖНО: всегда используй текущий год (2025) в датах, никогда не используй старые годы типа 2023!",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "field": {"type": "string", "description": "Поле группировки: category1|category2|category3|organization|description"},
                            "start_date": {"type": "string", "description": "Дата начала DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"},
                            "end_date": {"type": "string", "description": "Дата конца DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"},
                            "filters": {
                                "type": "object",
                                "description": "Доп. фильтры: словарь {field: value}",
                                "additionalProperties": {"type": "string"}
                            }
                        },
                        "required": ["field", "start_date", "end_date"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_grouped_by_category3",
                    "description": "Группировка по категории 3 уровня за период. Показывает сумму по каждой категории. Если пользователь просит график или диаграмму, используй эту функцию - она автоматически построит круговую диаграмму. ВАЖНО: всегда используй текущий год (2025) в датах, никогда не используй старые годы типа 2023!",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "start_date": {"type": "string", "description": "Дата начала DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"},
                            "end_date": {"type": "string", "description": "Дата конца DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"}
                        },
                        "required": ["start_date", "end_date"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_grouped_by_organization",
                    "description": "Группировка по организациям за период. Показывает сумму по каждой организации. Если пользователь просит график или диаграмму, используй эту функцию - она автоматически построит круговую диаграмму. ВАЖНО: всегда используй текущий год (2025) в датах, никогда не используй старые годы типа 2023!",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "start_date": {"type": "string", "description": "Дата начала DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"},
                            "end_date": {"type": "string", "description": "Дата конца DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"}
                        },
                        "required": ["start_date", "end_date"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_grouped_by_description",
                    "description": "Группировка по комментариям/тегам за период. Показывает сумму по каждому комментарию. Если пользователь просит график или диаграмму, используй эту функцию - она автоматически построит круговую диаграмму. ВАЖНО: всегда используй текущий год (2025) в датах, никогда не используй старые годы типа 2023!",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "start_date": {"type": "string", "description": "Дата начала DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"},
                            "end_date": {"type": "string", "description": "Дата конца DD.MM.YYYY. ВСЕГДА используй текущий год (2025), никогда не используй старые годы!"}
                        },
                        "required": ["start_date", "end_date"]
                    }
                }
            }
        ]

