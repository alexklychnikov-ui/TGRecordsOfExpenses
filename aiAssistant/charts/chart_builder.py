"""Chart builder for generating pie charts from grouped data."""
import io
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
from typing import List, Dict


def create_pie_chart(grouped_data: List[Dict], group_field_name: str) -> io.BytesIO:
    """
    Создает круговую диаграмму из сгруппированных данных.
    
    Args:
        grouped_data: Список словарей с полями ["group_name", "total"]
        group_field_name: Название поля группировки (для заголовка)
    
    Returns:
        BytesIO объект с изображением PNG
    """
    if not grouped_data:
        raise ValueError("Empty data for chart")
    
    # Извлекаем данные
    labels = []
    values = []
    for item in grouped_data:
        group_name = item.get("group_name") or "Без названия"
        total = float(item.get("total", 0))
        if total > 0:  # Пропускаем нулевые значения
            labels.append(str(group_name))
            values.append(total)
    
    if not values:
        raise ValueError("No positive values in data")
    
    # Вычисляем общую сумму для расчета процентов
    total_sum = sum(values)
    
    # Динамический расчет размера фигуры на основе количества элементов
    num_items = len(labels)
    base_height = 6
    height = base_height + max(0, (num_items - 5) * 0.3)
    fig, ax = plt.subplots(figsize=(6, height))
    
    # Создаем круговую диаграмму
    colors = plt.cm.Set3(range(len(values)))  # Контрастные цвета
    wedges, texts, autotexts = ax.pie(
        values,
        labels=None,  # Без подписей на секторах
        autopct='%1.1f%%',  # Процент на секторах
        startangle=75,
        colors=colors,
        textprops={'fontsize': 10, 'weight': 'bold'}
    )
    
    # Заголовок
    field_names = {
        "category1": "категориям 1 уровня",
        "category2": "категориям 2 уровня",
        "category3": "категориям 3 уровня",
        "organization": "организациям",
        "description": "комментариям"
    }
    title_text = field_names.get(group_field_name, group_field_name)
    ax.set_title(f"Распределение сумм по {title_text}", fontsize=14, fontweight='bold', pad=20)
    
    # Легенда снизу с названиями, суммами и процентами
    legend_labels = []
    for label, value in zip(labels, values):
        percent = (value / total_sum) * 100 if total_sum > 0 else 0
        legend_labels.append(f"{label}: {value:.2f} ₽ ({percent:.1f}%)")
    
    # Оптимизация легенды: используем несколько колонок при большом количестве элементов
    ncol = 2 if num_items > 15 else 1
    
    # Динамический расчет отступа легенды от графика
    legend_y_offset = -0.3 - max(0, (num_items - 10) * 0.02)
    
    ax.legend(
        wedges,
        legend_labels,
        loc="center",
        bbox_to_anchor=(0.5, legend_y_offset),
        ncol=ncol,
        fontsize=9
    )
    
    # Динамический расчет отступа снизу
    bottom_margin = 0.2 + min(0.5, num_items * 0.02)
    plt.subplots_adjust(bottom=bottom_margin)
    plt.tight_layout()
    
    # Сохраняем в BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)
    
    return buf






