# indicators.py
# -*- coding: utf-8 -*-
"""
Правила сигналов в точности как в таблице пользователя:

Инфляционные и проэкономические (где перегрев = риск ужесточения):
- CPI, PPI, NFP, Retail Sales:
    Факт > Прогноз → SHORT
    Факт < Прогноз → LONG

Негативные (больше = хуже для экономики → мягче политика):
- Уровень безработицы, Первичные заявки на пособие, Запасы сырой нефти:
    Факт > Прогноз → LONG
    Факт < Прогноз → SHORT

FOMC (ставка, сравнение Факт vs Предыдущее):
    Ставка ↑ → SHORT
    Ставка ↓ → LONG
"""
from typing import Dict, Any
from storage import list_custom_indicators

Indicator = Dict[str, Any]

# === Базовые правила ===
def _rule_long_if_actual_lt_forecast(row):
    a, f = row.get("actual_val"), row.get("forecast_val")
    if a is None or f is None:
        return "NEUTRAL"
    return "LONG" if a < f else "SHORT"

def _rule_long_if_actual_gt_forecast(row):
    a, f = row.get("actual_val"), row.get("forecast_val")
    if a is None or f is None:
        return "NEUTRAL"
    return "LONG" if a > f else "SHORT"

def _rule_short_if_actual_gt_forecast(row):
    """Инфляционные и проэкон. показатели: Факт > Прогноз → SHORT"""
    a, f = row.get("actual_val"), row.get("forecast_val")
    if a is None or f is None:
        return "NEUTRAL"
    return "SHORT" if a > f else "LONG"

def _rule_fomc_rate(row):
    """
    FOMC: сравнение Факт (текущая ставка) с Прогнозом (ожиданием).
    Факт < Прогноз → LONG; Факт ≥ Прогноз → SHORT.
    """
    a, f = row.get("actual_val"), row.get("forecast_val")
    if a is None or f is None:
        return "NEUTRAL"
    return "LONG" if a < f else "SHORT"

# === Сопоставление кодов правил для кастомов ===
_RULES = {
    "LT": _rule_long_if_actual_lt_forecast,
    "GT": _rule_long_if_actual_gt_forecast,
    "FOMC": _rule_fomc_rate,
}

# ===== ПРЕСЕТЫ =====
PRESET_INDICATORS: Dict[str, Indicator] = {
    # Инфляционные
    "CPI": {
        "title": "Индекс потребительских цен (CPI / Core CPI)",
        "url": "https://www.investing.com/economic-calendar/core-cpi-56",
        "rule": _rule_short_if_actual_gt_forecast,
    },
    "PPI": {
        "title": "Индекс цен производителей (PPI)",
        "url": "https://ru.investing.com/economic-calendar/ppi-238",
        "rule": _rule_short_if_actual_gt_forecast,
    },
    "CORE_PCE": {
        "title": "Базовый ценовой индекс расходов на личное потребление (Core PCE), м/м",
        "url": "https://ru.investing.com/economic-calendar/core-pce-price-index-61",
        "rule": _rule_short_if_actual_gt_forecast,
    },
    # Проэкономические
    "NFP": {
        "title": "Несельскохозяйственная занятость (NFP)",
        "url": "https://www.investing.com/economic-calendar/nonfarm-payrolls-227",
        "rule": _rule_short_if_actual_gt_forecast,
    },
    "RETAIL_SALES": {
        "title": "Розничные продажи",
        "url": "https://www.investing.com/economic-calendar/retail-sales-256",
        "rule": _rule_short_if_actual_gt_forecast,
    },
    # Негативные
    "UNEMPLOYMENT": {
        "title": "Уровень безработицы",
        "url": "https://www.investing.com/economic-calendar/unemployment-rate-300",
        "rule": _rule_long_if_actual_gt_forecast,
    },
    "JOBLESS_CLAIMS": {
        "title": "Первичные заявки на пособие по безработице",
        "url": "https://www.investing.com/economic-calendar/initial-jobless-claims-294",
        "rule": _rule_long_if_actual_gt_forecast,
    },
    "CRUDE_OIL_INV": {
        "title": "Запасы сырой нефти (EIA)",
        "url": "https://www.investing.com/economic-calendar/eia-crude-oil-inventories-75",
        "rule": _rule_long_if_actual_gt_forecast,
    },
    "ISM_MANUFACTURING_PMI": {
        "title": "Индекс деловой активности в производственном секторе (ISM Manufacturing PMI)",
        "url": "https://ru.investing.com/economic-calendar/ism-manufacturing-pmi-173",
        "rule": _rule_short_if_actual_gt_forecast,
    },
    # Ставка ФРС
    "FOMC_RATE": {
        "title": "Решение по процентной ставке ФРС США",
        "url": "https://ru.investing.com/economic-calendar/interest-rate-decision-168",
        "rule": _rule_fomc_rate,
    },
}

# === Слияние пресетов и кастомов ===
async def get_indicators(chat_id: int) -> Dict[str, Indicator]:
    result: Dict[str, Indicator] = dict(PRESET_INDICATORS)
    customs = await list_custom_indicators(chat_id)
    for ci in customs:
        rule_func = _RULES.get((ci.get("rule") or "").upper(), _rule_long_if_actual_lt_forecast)
        result[ci["key"]] = {
            "title": ci["title"],
            "url": ci["url"],
            "rule": rule_func,
        }
    return result

def rules_hints() -> str:
    return (
        "📘 <b>Подсказка по правилам:</b>\n"
        "• <b>LT</b> — сигнал LONG, если Факт меньше Прогноза\n"
        "• <b>GT</b> — сигнал LONG, если Факт больше Прогноза\n"
        "• <b>FOMC</b> — решение ФРС:\n"
        "   ↳ Факт < Прогноза → LONG\n"
        "   ↳ Факт ≥ Прогноза → SHORT"
    )