"""
Unified Telegram Bot - объединяет функциональность старого бота и AI-ассистента

Этот бот включает:
- Парсинг чеков из фото и документов (GPT-4o vision)
- Проверку дубликатов чеков
- Сохранение чеков в папки пользователей
- AI-ассистент для аналитики и ответов на вопросы
- Контекстные диалоги с памятью
- Отображение фото чеков по запросу

Запуск: python bot_unified.py
"""
import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Импортируем main из AI-ассистента, который уже включает всю функциональность
from aiAssistant.telegram.bot import main

if __name__ == "__main__":
    print("=" * 60)
    print("Unified Telegram Bot - Zapusk")
    print("=" * 60)
    print()
    print("Funkcionalnost:")
    print("  [*] Parsing chekov iz foto/dokumentov")
    print("  [*] Proverka dublikatov")
    print("  [*] AI-assistant dlya analitiki")
    print("  [*] Otchety i statistika")
    print("  [*] Otobrazhenie foto chekov")
    print()
    print("Komandy bota:")
    print("  /start - nachat dialog")
    print("  /clear - ochistit kontekst")
    print("  /help - pomoshch")
    print()
    print("Primery zaprosov:")
    print("  - Pokazhi posledniy chek")
    print("  - Obshchaya summa za nedelyu")
    print("  - Statistika po kategoriyam")
    print()
    print("Nazhmite Ctrl+C dlya ostanovki")
    print("=" * 60)
    print()
    
    import asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[STOP] Bot ostanovlen")
    except Exception as e:
        print(f"\n\n[ERROR] Oshibka: {e}")
        import traceback
        traceback.print_exc()

