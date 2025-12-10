"""Report builder for generating user-friendly output."""
from typing import List, Dict, Any


class ReportBuilder:
    @staticmethod
    def format_cheque(items: List[Dict]) -> str:
        if not items:
            return ""
        
        chequeid = items[0].get("chequeid", "N/A")
        date = items[0].get("date", "N/A")
        organization = items[0].get("organization", "N/A")
        positions_count = len(items)
        
        # Header
        result = f"🧾 Чек № {chequeid} | 📅 {date}\n"
        result += f"🏪 {organization}\n\n"
        
        # Body - компактный формат
        total = 0.0
        for idx, item in enumerate(items, 1):
            name = item.get("product_name", "N/A")
            price = float(item.get("price", 0) or 0)
            quantity = float(item.get("quantity", 1) or 1)
            total += price
            
            # Обрезаем длинные названия
            name_display = name[:40] + "..." if len(name) > 40 else name
            
            # Показываем количество только если не равно 1
            if quantity != 1:
                result += f"{idx}. {name_display} | {price:.2f} ₽ × {quantity} шт.\n"
            else:
                result += f"{idx}. {name_display} | {price:.2f} ₽\n"
        
        # Footer
        result += f"\n💳 Итого: {total:.2f} ₽"
        
        return result
    
    @staticmethod
    def format_purchases_list(purchases: List[Dict], limit: int = 10) -> str:
        if not purchases:
            return "Записей не найдено"
        
        total_count = len(purchases)
        display_items = purchases[:limit]
        
        result = f"📊 **Найдено записей: {total_count}**\n\n"
        
        total_sum = 0
        for item in display_items:
            name = item.get("product_name", "N/A")[:40]
            price = float(item.get("price", 0))
            total_sum += price
            date = item.get("date", "N/A")
            org = item.get("organization", "N/A")[:30]
            cid = item.get("chequeid", "N/A")
            
            result += f"• #{cid} {name}\n"
            result += f"  💰 {price:.2f} ₽ | 📅 {date} | 🏪 {org}\n\n"
        
        if total_count > limit:
            result += f"... и ещё {total_count - limit} записей\n\n"
        
        result += f"💳 **Сумма (первые {len(display_items)}): {total_sum:.2f} ₽**"
        
        return result

    @staticmethod
    def format_cheque_totals(purchases: List[Dict], limit: int = 20) -> str:
        """
        Группировка позиций по номерам чеков: дата чека, номер чека, сумма чека.
        """
        if not purchases:
            return "Записей не найдено"

        groups = {}
        order = []
        for p in purchases:
            cid = p.get("chequeid")
            if cid not in groups:
                groups[cid] = {
                    "sum": 0.0,
                    "date": p.get("date", "N/A"),
                    "chequeid": cid or "N/A",
                }
                order.append(cid)
            price = float(p.get("price", 0) or 0)
            groups[cid]["sum"] += price

        total_cheques = len(groups)
        display_ids = order[:limit]

        lines = []
        for cid in display_ids:
            g = groups[cid]
            lines.append(f"• 📅 {g['date']} | 🧾 {g['chequeid']} | 💳 {g['sum']:.2f} ₽")

        result = f"📊 **Найдено чеков: {total_cheques}**\n\n"
        result += "\n".join(lines)

        if total_cheques > limit:
            result += f"\n\n... и ещё {total_cheques - limit} чеков"

        return result
    
    @staticmethod
    def format_summary(summary: Dict) -> str:
        count = summary.get("count", 0)
        total = summary.get("total", 0.0)
        cheque_count = summary.get("cheque_count", 0)
        
        result = "📊 **Статистика:**\n\n"
        result += f"🧾 Чеков: {cheque_count}\n"
        result += f"📦 Позиций: {count}\n"
        result += f"💰 **Общая сумма: {total:.2f} ₽**"
        
        return result
    
    @staticmethod
    def format_category_stats(stats: List[Dict]) -> str:
        if not stats:
            return "Нет данных по категориям"
        
        result = "📊 **Статистика по категориям:**\n\n"
        
        for item in stats:
            category = item.get("category", "N/A")
            count = item.get("count", 0)
            total = item.get("total", 0.0)
            
            result += f"🏷️ **{category}**\n"
            result += f"   📦 Позиций: {count}\n"
            result += f"   💰 Сумма: {total:.2f} ₽\n\n"
        
        total_sum = sum(item.get("total", 0) for item in stats)
        result += f"💳 **Итого: {total_sum:.2f} ₽**"
        
        return result
    
    @staticmethod
    def format_grouped_stats(stats: List[Dict], field_name: str) -> str:
        if not stats:
            return f"Нет данных для группировки по {field_name}"
        
        field_emoji = {
            "category1": "🏷️",
            "category2": "🏷️",
            "category3": "🏷️",
            "organization": "🏪",
            "description": "📝"
        }
        
        emoji = field_emoji.get(field_name, "📊")
        
        result = f"📊 **Группировка по {field_name}:**\n\n"
        
        for idx, item in enumerate(stats, 1):
            group_name = item.get("group_name", "N/A")
            count = item.get("count", 0)
            total = item.get("total", 0.0)
            cheque_count = item.get("cheque_count", 0)
            
            result += f"{idx}. {emoji} **{group_name}**\n"
            result += f"   🧾 Чеков: {cheque_count}\n"
            result += f"   📦 Позиций: {count}\n"
            result += f"   💰 Сумма: {total:.2f} ₽\n\n"
        
        total_sum = sum(item.get("total", 0) for item in stats)
        total_items = sum(item.get("count", 0) for item in stats)
        total_cheques = sum(item.get("cheque_count", 0) for item in stats)
        
        result += f"📊 **Итого:**\n"
        result += f"   🧾 Чеков: {total_cheques}\n"
        result += f"   📦 Позиций: {total_items}\n"
        result += f"   💳 **Сумма: {total_sum:.2f} ₽**"
        
        return result
    
    @staticmethod
    def format_update_result(success: bool, rows_affected: int = 0) -> str:
        if success:
            return f"✅ Обновлено записей: {rows_affected}"
        else:
            return "❌ Не удалось обновить запись"

