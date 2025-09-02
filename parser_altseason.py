# parser_altseason.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import datetime as dt
from io import BytesIO
from typing import Dict, Optional, Tuple, List

import requests
from bs4 import BeautifulSoup, Tag
from PIL import Image, ImageDraw, ImageFont

# --------- источники (пробуем по очереди) ---------
ALTSEASON_URLS = [
    "https://www.blockchaincenter.net/en/altcoin-season-index/",
    "https://www.blockchaincenter.net/ru/altcoin-season-index/",
    "https://www.blockchaincenter.net/altcoin-season-index/",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}
TIMEOUT = 12

# ======================== базовые утилиты ========================
def _fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def _find_numbers_0_100(text: str) -> List[Tuple[int, int]]:
    """Вернёт все числа 0..100 и их позиции в тексте."""
    nums: List[Tuple[int, int]] = []
    for m in re.finditer(r"(?<!\d)(\d{1,3})(?!\d)", text):
        try:
            v = int(m.group(1))
        except ValueError:
            continue
        if 0 <= v <= 100:
            nums.append((v, m.start()))
    return nums

def _extract_index_heuristic(html: str) -> int:
    """
    Робастное извлечение индекса:
      1) прямые паттерны 'Altcoin Season Index: 55' / 'Индекс сезона альткоинов ... 55'
      2) число ближе всего к якорям 'current/Сейчас'
      3) фолбэк — разумные числа 30..90 (не 25/75)
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    m = re.search(r"(Altcoin\s+Season\s+Index|Индекс\s+сезона\s+альткоинов)[^\d]{0,40}(\d{1,3})", text, re.I)
    if m:
        v = int(m.group(2))
        if 0 <= v <= 100:
            return v

    anchors = []
    for kw in ["Сейчас", "текущ", "current", "Now", "Altcoin Season Index", "Индекс сезона альткоинов"]:
        for a in re.finditer(kw, text, flags=re.I):
            anchors.append(a.start())

    nums = _find_numbers_0_100(text)
    if not nums:
        raise ValueError("Не нашли чисел 0–100 на странице")

    if anchors:
        def dist(npos: int) -> int:
            return min(abs(npos - a) for a in anchors)

        # отфильтруем очевидные линии-пороги
        filtered = [(v, pos) for (v, pos) in nums if v not in (0, 25, 75, 100)]
        pick = min(filtered or nums, key=lambda t: dist(t[1]))
        return pick[0]

    for v, _ in nums:
        if 30 <= v <= 90 and v not in (25, 75):
            return v
    return nums[0][0]

# ======================== публичные функции (индекс) ========================
def fetch_altseason_index() -> Tuple[int, str]:
    """
    Возвращает (index, used_url).
    Бросает ValueError с понятным сообщением при неудаче.
    """
    last_error: Optional[str] = None
    for url in ALTSEASON_URLS:
        try:
            html = _fetch_html(url)
            value = _extract_index_heuristic(html)
            return value, url
        except Exception as e:
            last_error = str(e)
            continue
    raise ValueError(f"Не удалось распознать индекс на странице: {last_error or 'неизвестная ошибка'}")

def classify_altseason(value: int) -> Tuple[str, str]:
    """
    Классификация и подсказка:
      ≤25  → «Сезон биткоина»
      26–68 → «Нейтрально»
      69–74 → «Близко к альтсезону»
      ≥75  → «Альтсезон»
    """
    if value <= 25:
        return "🔵 Сезон биткоина", "Преимущество за BTC-парами."
    if value >= 75:
        return "🟢 Альтсезон", "Альты часто обгоняют BTC. Риски выше."
    if value >= 69:
        return "🟡 Близко к альтсезону", "Следим: >69 — разморозка альтов, >75 — горячая фаза."
    return "⚪️ Нейтральная зона", "Явного преимущества нет."

def format_altseason_status(value: int) -> str:
    label, tip = classify_altseason(value)
    return (
        f"<b>Индекс альтсезона</b>: <b>{value}</b>/100\n"
        f"Статус: {label}\n"
        f"Пороги: 25 (BTC-season) · 69 (близко) · 75 (альтсезон)\n"
        f"{tip}"
    )

def format_altseason_text(value: int, src_url: str) -> str:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"{format_altseason_status(value)}\n\n<i>Источник</i>: {src_url}\n<i>Обновлено</i>: {ts} МСК"

# ======================== сводка из правой таблицы ========================
def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def _parse_int(s: str) -> Optional[int]:
    if s is None:
        return None
    s = s.strip()
    if not s or s.lower() in {"none", "n/a", "-", "—"}:
        return None
    try:
        return int(re.sub(r"[^\d-]", "", s))
    except Exception:
        return None

# ключевые слова (и RU, и EN) для «фуззи»-сопоставления
_KEYWORDS = {
    "days_since_last": [["days", "since", "last"], ["дней", "прошлого"]],
    "avg_between": [["average", "between"], ["средн", "между"]],
    "longest_without": [["longest", "without"], ["самая", "длин", "без"]],
    "avg_length": [["average", "length"], ["средн", "длитель"]],
    "longest_length": [["longest", "season"], ["самый", "длин", "сезон"]],
    "total_days": [["total", "number", "days"], ["общее", "колич", "дней"]],
}

# — варианты меток (точные строки на RU/EN)
_LABEL_VARIANTS = {
    "days_since_last": [
        "days since last season",
        "дней с прошлого сезона",
    ],
    "avg_between": [
        "average days between seasons",
        "среднее количество дней между сезонами",
    ],
    "longest_without": [
        "longest period without a season",
        "самая длинная серия без сезона",
    ],
    "avg_length": [
        "average season length (days)",
        "average length of season (days)",   # иногда на сайте встречается такая форма
        "средняя продолжительность сезона (дней)",
        "средняя длительность сезона (дней)",
    ],
    "longest_length": [
        "longest season (days)",
        "самый длинный сезон (дни)",
    ],
    "total_days": [
        "total number of days in season",
        "total days of season",
        "общее количество дней сезона",
    ],
}

def _match_key(label_norm: str) -> Optional[str]:
    # 1) точные варианты
    for key, variants in _LABEL_VARIANTS.items():
        for v in variants:
            if _normalize(v) in label_norm:
                return key
    # 2) фуззи: набор ключевых слов (все должны встретиться)
    for key, bundles in _KEYWORDS.items():
        for kws in bundles:
            if all(kw in label_norm for kw in kws):
                return key
    return None

def fetch_altseason_stats(timeout: int = 12) -> Dict[str, Dict[str, Optional[int]]]:
    """
    Возвращает метрики ТОЛЬКО из блока 'Altcoin Season':
      {
        "days_since_last": {"alt": 259, "btc": 47},
        "avg_between": {"alt": 66, "btc": 17},
        "longest_without": {"alt": 486, "btc": 191},
        "avg_length": {"alt": 18, "btc": 10},
        "longest_length": {"alt": 117, "btc": 126},
        "total_days": {"alt": 404, "btc": 953},
      }
    """
    last_err = None
    html = None
    for url in ALTSEASON_URLS:
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            html = r.text
            break
        except Exception as e:
            last_err = e
    if html is None:
        raise RuntimeError(f"Не удалось загрузить страницу: {last_err}")

    soup = BeautifulSoup(html, "html.parser")

    # ---------- 1) найдём заголовок "Altcoin Season Index" и ближайшую таблицу Altcoin/Bitcoin ----------
    hdr = soup.find(string=re.compile(r"(Altcoin\s+Season\s+Index|Индекс\s+сезона\s+альткоинов)", re.I))
    target_table: Optional[Tag] = None
    if hdr:
        node = hdr.parent
        for _ in range(200):
            node = node.find_next()
            if node is None:
                break
            if node.name == "table":
                first = node.find("tr")
                if first:
                    cols = [c.get_text(" ", strip=True) for c in first.find_all(["th", "td"])]
                    if len(cols) == 3:
                        h2, h3 = _normalize(cols[1]), _normalize(cols[2])
                        if ("altcoin" in h2 and "bitcoin" in h3) or ("альт" in h2 and "биткоин" in h3):
                            target_table = node
                            break

    # ---------- 2) фолбэк: просто ищем первую таблицу формата [label | Altcoin | Bitcoin] ----------
    if target_table is None:
        for tbl in soup.find_all("table"):
            first = tbl.find("tr")
            if not first:
                continue
            cols = [c.get_text(" ", strip=True) for c in first.find_all(["th", "td"])]
            if len(cols) != 3:
                continue
            h2, h3 = _normalize(cols[1]), _normalize(cols[2])
            if ("altcoin" in h2 and "bitcoin" in h3) or ("альт" in h2 and "биткоин" in h3):
                target_table = tbl
                break

    if target_table is None:
        raise RuntimeError("Таблица Altcoin/Bitcoin для секции 'Altcoin Season' не найдена")

    # ---------- 3) парсим строки ----------
    stats: Dict[str, Dict[str, Optional[int]]] = {}
    for tr in target_table.find_all("tr")[1:]:
        tds = tr.find_all(["td", "th"])
        if len(tds) != 3:
            continue
        label = _normalize(tds[0].get_text(" ", strip=True))
        alt_v = _parse_int(tds[1].get_text(" ", strip=True))
        btc_v = _parse_int(tds[2].get_text(" ", strip=True))

        key = _match_key(label)
        if key:
            stats[key] = {"alt": alt_v, "btc": btc_v}

    # проверим, что всё основное распознали
    required = {"days_since_last", "avg_between", "longest_without", "avg_length", "longest_length", "total_days"}
    missing = [k for k in sorted(required) if k not in stats]
    if missing:
        raise RuntimeError("Не удалось распознать метрики таблицы: " + ", ".join(missing))

    return stats

def format_altseason_stats(stats: Dict[str, Dict[str, Optional[int]]]) -> str:
    """Формат сводки для Telegram."""
    def g(k):
        v = stats.get(k, {})
        return v.get("alt"), v.get("btc")

    d1a, d1b = g("days_since_last")
    d2a, d2b = g("avg_between")
    d3a, d3b = g("longest_without")
    d4a, d4b = g("avg_length")
    d5a, d5b = g("longest_length")
    d6a, d6b = g("total_days")

    lines = [
        "<b>📊 Сводка по сезонам</b>",
        f"• Дней с прошлого сезона: <b>{d1a}</b> (альты) | <b>{d1b}</b> (BTC)",
        f"• Среднее кол-во дней между сезонами: <b>{d2a}</b> | <b>{d2b}</b>",
        f"• Самая длинная серия без сезона: <b>{d3a}</b> | <b>{d3b}</b>",
        f"• Средняя длительность сезона (дни): <b>{d4a}</b> | <b>{d4b}</b>",
        f"• Самый длинный сезон (дни): <b>{d5a}</b> | <b>{d5b}</b>",
        f"• Всего дней сезона: <b>{d6a}</b> | <b>{d6b}</b>",
        "",
        "ℹ️ Порог альтсезона: <b>69+</b>. Биткоин-сезон: <b>≤25</b>.",
    ]
    return "\n".join(lines)

# ======================== отрисовка PNG-карточки ========================
def _try_font(size: int):
    """Пробуем системные шрифты; если нет — встроенный."""
    for name in ["arial.ttf", "DejaVuSans.ttf"]:
        try:
            return ImageFont.truetype(name, size)
        except Exception:
            pass
    return ImageFont.load_default()

def _text_size(drw: ImageDraw.ImageDraw, text: str, font) -> tuple[int, int]:
    """Безопасно получаем ширину/высоту текста для разных версий Pillow."""
    try:
        l, t, r, b = drw.textbbox((0, 0), text, font=font)
        return (r - l), (b - t)
    except Exception:
        return drw.textsize(text, font=font)

def render_altseason_card(value: int, width: int = 900, height: int = 220) -> Tuple[bytes, str]:
    """
    Рисует горизонтальную шкалу 0..100 с отметками 25/69/75 и текущим значением.
    Возвращает (png_bytes, filename).
    """
    pad = 20
    bar_h = 36
    img = Image.new("RGB", (width, height), (18, 18, 22))
    drw = ImageDraw.Draw(img)

    f_title = _try_font(28)
    f_val = _try_font(46)
    f_small = _try_font(18)

    # Заголовок
    drw.text((pad, pad), "Индекс альтсезона", fill=(230, 230, 240), font=f_title)

    # Шкала
    bar_left = pad
    bar_right = width - pad
    bar_top = pad + 52
    bar_bottom = bar_top + bar_h

    # Градиент по зонам
    def lerp(a, b, t): return int(a + (b - a) * t)

    for x in range(bar_left, bar_right):
        t = (x - bar_left) / (bar_right - bar_left)
        if t <= 0.25:  # оранж
            col = (lerp(255, 255, t / .25), lerp(140, 200, t / .25), 0)
        elif t <= 0.69:  # нейтральная
            tt = (t - .25) / .44
            col = (lerp(220, 140, tt), lerp(220, 230, tt), lerp(220, 240, tt))
        else:  # зелёная
            tt = (t - .69) / .31
            col = (lerp(140, 0, tt), lerp(230, 200, tt), lerp(140, 60, tt))
        drw.line([(x, bar_top), (x, bar_bottom)], fill=col)

    # Отметки 25 / 69 / 75
    def mark(xpos: int, text: str):
        drw.line([(xpos, bar_top - 6), (xpos, bar_bottom + 6)], fill=(240, 240, 240), width=2)
        tw, th = _text_size(drw, text, f_small)
        drw.text((xpos - tw // 2, bar_bottom + 10), text, fill=(210, 210, 220), font=f_small)

    for p, t in [(25, "25"), (69, "69"), (75, "75")]:
        x = int(bar_left + (bar_right - bar_left) * (p / 100.0))
        mark(x, t)

    # Текущее значение
    v = max(0, min(100, int(value)))
    vx = int(bar_left + (bar_right - bar_left) * (v / 100.0))
    drw.rectangle([(vx - 2, bar_top - 10), (vx + 2, bar_bottom + 10)], fill=(255, 255, 255))
    label, tip = classify_altseason(v)

    # Подписи и значение
    drw.text((pad, bar_bottom + 54), f"Статус: {label}", fill=(230, 230, 240), font=f_small)
    drw.text((pad, bar_top - 48), "0", fill=(180, 180, 190), font=f_small)
    drw.text((bar_right - 14, bar_top - 48), "100", fill=(180, 180, 190), font=f_small)

    val_text = f"{v}"
    vt_w, vt_h = _text_size(drw, val_text, f_val)
    drw.text((bar_right - vt_w, pad + 2), val_text, fill=(255, 255, 255), font=f_val)
    drw.text((pad, bar_bottom + 82), tip, fill=(190, 190, 200), font=f_small)

    bio = BytesIO()
    img.save(bio, format="PNG", optimize=True)
    return bio.getvalue(), f"altseason_{v}.png"

__all__ = [
    "fetch_altseason_index",
    "classify_altseason",
    "format_altseason_status",
    "format_altseason_text",
    "fetch_altseason_stats",
    "format_altseason_stats",
    "render_altseason_card",
]
