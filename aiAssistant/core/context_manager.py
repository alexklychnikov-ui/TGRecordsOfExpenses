"""Context manager for storing user conversation history."""
from typing import Dict, List
import os


class ContextManager:
    def __init__(self, max_messages: int = 20):
        self.max_messages = max_messages
        self._contexts: Dict[int, List[Dict[str, str]]] = {}
        self._last_cheque: Dict[int, int] = {}  # user_id -> chequeid
        self._last_query: Dict[int, Dict] = {}  # user_id -> {type, params, result, username}
        self._pending_cheques: Dict[int, Dict] = {}  # user_id -> pending data
    
    def add_message(self, user_id: int, role: str, content: str) -> None:
        if user_id not in self._contexts:
            self._contexts[user_id] = []
        
        self._contexts[user_id].append({"role": role, "content": content})
        
        if len(self._contexts[user_id]) > self.max_messages:
            self._contexts[user_id] = self._contexts[user_id][-self.max_messages:]
    
    def get_messages(self, user_id: int) -> List[Dict[str, str]]:
        return self._contexts.get(user_id, [])
    
    def clear_context(self, user_id: int) -> None:
        if user_id in self._contexts:
            del self._contexts[user_id]
        if user_id in self._last_cheque:
            del self._last_cheque[user_id]
        if user_id in self._last_query:
            del self._last_query[user_id]
        if user_id in self._pending_cheques:
            del self._pending_cheques[user_id]
    
    def set_last_cheque(self, user_id: int, chequeid: int) -> None:
        """Сохранить последний просмотренный чек для пользователя."""
        self._last_cheque[user_id] = chequeid
    
    def get_last_cheque(self, user_id: int) -> int | None:
        """Получить последний просмотренный чек для пользователя."""
        return self._last_cheque.get(user_id)
    
    def set_last_query(self, user_id: int, query_type: str, params: Dict, result: List[Dict], username: str) -> None:
        """
        Сохранить последний запрос к БД для пользователя.
        
        Args:
            user_id: Уникальный ID пользователя Telegram (message.from_user.id)
            query_type: Тип запроса (например, "get_grouped_by_category1")
            params: Параметры запроса (start_date, end_date, field и т.д.)
            result: Результат запроса (список словарей)
            username: Username пользователя для БД
        """
        self._last_query[user_id] = {
            "type": query_type,
            "params": params,
            "result": result,
            "username": username
        }
    
    def get_last_query(self, user_id: int) -> Dict | None:
        """
        Получить последний запрос к БД для пользователя.
        
        Args:
            user_id: Уникальный ID пользователя Telegram (message.from_user.id)
        
        Returns:
            Словарь с данными последнего запроса или None, если запросов не было
        """
        return self._last_query.get(user_id)
    
    def clear_last_query(self, user_id: int) -> None:
        """
        Очистить кеш последнего запроса для пользователя.
        
        Args:
            user_id: Уникальный ID пользователя Telegram (message.from_user.id)
        """
        self._last_query.pop(user_id, None)

    def set_pending_cheque(self, user_id: int, data: Dict) -> None:
        self._pending_cheques[user_id] = data

    def get_pending_cheque(self, user_id: int) -> Dict | None:
        return self._pending_cheques.get(user_id)

    def clear_pending_cheque(self, user_id: int) -> None:
        self._pending_cheques.pop(user_id, None)
    
    def get_system_prompt(self) -> str:
        try:
            base_dir = os.path.dirname(os.path.dirname(__file__))
            prompt_path = os.path.join(base_dir, "assistant_prompt_ru.txt")
            with open(prompt_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    return content
        except Exception:
            pass
        return (
            "Ты — AI-ассистент по финансам. Помогаешь пользователю анализировать расходы из базы данных чеков.\n\n"
            "Доступные функции для работы с БД:\n"
            "- fetch_by_period(start_date, end_date) - записи за период (формат дат: DD.MM.YYYY)\n"
            "- get_last_n_days(n) - записи за последние N дней\n"
            "- get_current_week() - записи за текущую неделю (с понедельника)\n"
            "- get_current_month() - записи за месяц\n"
            "- get_yesterday() - записи за вчерашний день\n"
            "- get_previous_month() - записи за прошлый месяц\n"
            "- get_previous_year() - записи за прошлый год\n"
            "- fetch_by_category(level, name) - записи по категории\n"
            "- fetch_by_organization(organization) - поиск по контексту в названии организации (LIKE)\n"
            "- fetch_by_product_name(product_name) - поиск по контексту в названии товара (LIKE)\n"
            "- fetch_by_description(description) - поиск по контексту в комментариях (LIKE)\n"
            "- get_cheque_by_id(chequeid) - чек по номеру\n"
            "- get_last_cheque() - последний чек (по дате, ближайший к текущей)\n"
            "- get_summary(start_date, end_date) - сумма за период\n"
            "- get_summary_last_n_days(n) - сумма за N дней\n"
            "- get_summary_week() - сумма за неделю\n"
            "- get_summary_month() - сумма за месяц\n"
            "- get_grouped_by_category1(start_date, end_date) - группировка по категории 1 уровня\n"
            "- get_grouped_by_category2(start_date, end_date) - группировка по категории 2 уровня\n"
            "- get_grouped_by_category3(start_date, end_date) - группировка по категории 3 уровня\n"
            "- get_grouped_by_organization(start_date, end_date) - группировка по организациям\n"
            "- get_grouped_by_description(start_date, end_date) - группировка по комментариям\n"
            "- update_record(record_id, field, value) - обновить запись (record_id = номер позиции/строки в чеке; поля: price, discount, product_name, description, quantity, category1-3, organization, date)\n"
            "- update_description_by_cheque(chequeid, description) - добавить комментарий к чеку\n"
            "- update_description_by_organization(organization, description) - добавить комментарий ко всем чекам организации\n\n"
            "ВАЖНО: \n"
            "- Username определяется автоматически из Telegram. НЕ спрашивай и НЕ передавай username.\n"
            "- \"За неделю\" = с понедельника текущей недели по сегодня\n"
            "- \"За последние 7 дней\" = текущая дата минус 7 дней по сегодня\n"
            "- \"За месяц\" = с 1 числа текущего месяца по сегодня\n"
            "- \"Вчера\" / \"прошлый день\" = вызови get_yesterday()\n"
            "- \"Прошлый месяц\" = вызови get_previous_month()\n"
            "- \"Прошлый год\" = вызови get_previous_year()\n"
            "- Последний чек = чек с датой ближайшей к текущей дате\n\n"
            "Отвечай кратко, по делу. Используй эмодзи для наглядности. Суммы округляй до 2 знаков."
        )


