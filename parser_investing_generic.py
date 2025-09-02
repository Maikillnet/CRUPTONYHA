# parser_investing_generic.py
# -*- coding: utf-8 -*-
"""
Универсальный парсер таблиц Investing (2025-стайл):

- Стабильный HTTP с бэкоффом: cloudscraper -> requests (3 попытки)
- Умный выбор таблицы: ищет шапку Actual/Forecast/Previous и их синонимы (вкл. русские)
- Парсит числа и единицы:
    %, K/Thousand/Ths/тыс., M/Mln/Million/млн., B/Bln/Billion/млрд., T/Trillion,
    bn, mio/mio. (EU), скобки как отрицательные: (1.2M) -> -1.2
- Терпит любые пробелы/NBSP, запятую/точку, юникодный минус
- Доп. поля: release_dt_iso (если удаётся собрать из 1-2 столбцов), revised_from_*
- Богатые понятные ошибки с тегами этапов: [NET]/[HTML]/[TABLE]/[HEAD]/[IDX]/[ROW]/[PARSE]

Публичные функции (совместимы):
    fetch_table_rows(url, limit=12) -> (rows, err)
    format_table_for_tg(rows, src_url, max_rows=6) -> str
    render_table_png(rows, title, max_rows=8) -> (png_bytes, filename)
"""

import io
import os
import re
import time
from typing import List, Tuple, Dict, Any, Optional
from bs4 import BeautifulSoup

from PIL import Image, ImageDraw, ImageFont, ImageFilter

# ===== TZ для release_dt_iso (опционально) =====
_TZ_NAME = os.getenv("TZ", "Europe/Moscow")
try:
    import pytz
    _tz = pytz.timezone(_TZ_NAME)
except Exception:
    _tz = None

# ================= HTTP =================
def _get(url: str):
    """
    Надёжный GET: 3 попытки с backoff, cloudscraper -> requests.
    Возврат: (status_code, text|None, err|None, debug_note)
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9,ru-RU,ru;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    last_err = None
    notes = []
    for attempt in range(3):
        try:
            try:
                import cloudscraper  # type: ignore
                s = cloudscraper.create_scraper()
                r = s.get(url, headers=headers, timeout=25)
                notes.append(f"try{attempt+1}: cloudscraper -> HTTP {r.status_code}")
                if r.status_code == 200 and r.text:
                    return r.status_code, r.text, None, "; ".join(notes)
                last_err = f"[NET] cloudscraper HTTP {r.status_code}"
            except Exception as e:
                last_err = f"[NET] cloudscraper fail: {e}"
                notes.append(str(last_err))

            import requests
            r = requests.get(url, headers=headers, timeout=25)
            notes.append(f"try{attempt+1}: requests -> HTTP {r.status_code}")
            if r.status_code == 200 and r.text:
                return r.status_code, r.text, None, "; ".join(notes)
            last_err = f"[NET] requests HTTP {r.status_code}"
        except Exception as e:
            last_err = f"[NET] requests fail: {e}"
            notes.append(str(last_err))

        time.sleep(0.6 * (2 ** attempt))  # 0.6, 1.2

    return 0, None, last_err or "[NET] network error", "; ".join(notes)

# ============== Units & numbers ==========
# Нормируем к: percent / thousand / million / billion / trillion / None
_UNIT_TOKENS = {
    # percent
    "%": ("percent", 1.0),
    "percent": ("percent", 1.0),
    "проц": ("percent", 1.0),

    # thousand
    "k": ("thousand", 1_000.0),
    "thousand": ("thousand", 1_000.0),
    "ths": ("thousand", 1_000.0),
    "тыс": ("thousand", 1_000.0),
    "тыс.": ("thousand", 1_000.0),

    # million
    "m": ("million", 1_000_000.0),
    "mln": ("million", 1_000_000.0),
    "million": ("million", 1_000_000.0),
    "млн": ("million", 1_000_000.0),
    "млн.": ("million", 1_000_000.0),
    "mio": ("million", 1_000_000.0),
    "mio.": ("million", 1_000_000.0),

    # billion
    "b": ("billion", 1_000_000_000.0),
    "bn": ("billion", 1_000_000_000.0),
    "bln": ("billion", 1_000_000_000.0),
    "billion": ("billion", 1_000_000_000.0),
    "млрд": ("billion", 1_000_000_000.0),
    "млрд.": ("billion", 1_000_000_000.0),

    # trillion
    "t": ("trillion", 1_000_000_000_000.0),
    "trn": ("trillion", 1_000_000_000_000.0),
    "trillion": ("trillion", 1_000_000_000_000.0),
}

# «Шум» рядом с числами
_NOISE_TOKENS = {
    "bbl", "barrel", "barrels", "jobs", "claims", "inventories", "units",
    "mom", "qoq", "yoy", "mtm", "m/m", "y/y", "q/q"
}

# Заголовки
_HEAD_ACTUAL_KEYS   = ["actual", "факт", "фактич"]
_HEAD_FORECAST_KEYS = ["forecast", "прогноз", "estimate", "consensus", "est.", "exp.", "expectation"]
_HEAD_PREV_KEYS     = ["previous", "prior", "пред.", "пред", "предыдущ"]

# Числа и «revised from …»
RE_NUM_CORE = r"[+\-−]?\s*\d{1,3}(?:[ \u00A0\u202F]?\d{3})*(?:[.,]\d+)?"
RE_NUM = re.compile(rf"\(?\s*({RE_NUM_CORE})\s*\)?", re.I)
RE_REVISED = re.compile(r"(?:revised\s+from|пересмотрено\s+с)\s+([^\s].*)", re.I)

def _normalize_spaces(s: str) -> str:
    return (s or "").replace("\u00A0", " ").replace("\u202F", " ").strip()

def _to_float(num_str: str, paren_neg: bool) -> Optional[float]:
    if not num_str:
        return None
    t = num_str.strip().replace("−", "-").replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(",", "")
    elif "," in t and "." not in t:
        t = t.replace(",", ".")
    try:
        val = float(t)
        if paren_neg:
            val = -abs(val)
        return val
    except Exception:
        return None

def _extract_unit_token(text_lower: str) -> Optional[str]:
    if "%" in text_lower:
        return "%"
    tokens = re.findall(r"[a-zа-я.%]+", text_lower)
    for tok in tokens:
        t = tok.strip(".").lower()
        if t in _UNIT_TOKENS:
            return t
        if t in _NOISE_TOKENS:
            continue
    # прилипшие суффиксы после числа: "3.2M"
    m = RE_NUM.search(text_lower)
    if m:
        end = m.end()
        tail = text_lower[end:].strip().strip("()[]{}:;")
        suf = re.match(r"^[a-zа-я.%]{1,7}", tail)
        if suf:
            t = suf.group(0).strip(".").lower()
            if t in _UNIT_TOKENS:
                return t
    return None

def _to_scalar(txt: str):
    """
    Возвращает (value: float | None, unit: str | None)
    unit ∈ {"percent","thousand","million","billion","trillion", None}
    """
    if not txt:
        return None, None
    raw = _normalize_spaces(str(txt))
    if raw.lower() in {"—", "-", "•", "", "n/a", "na", "—/—", "waiting", "pending"}:
        return None, None

    m = RE_NUM.search(raw)
    if not m:
        return None, None

    paren_neg = raw.strip().startswith("(") and raw.strip().endswith(")")
    num = _to_float(m.group(1), paren_neg)
    if num is None:
        return None, None

    unit_token = _extract_unit_token(raw.lower())
    if unit_token and unit_token in _UNIT_TOKENS:
        norm_name, mul = _UNIT_TOKENS[unit_token]
        return num * mul, norm_name

    return num, None

def _parse_revised(text: str):
    """
    Возвращает (val, unit) если встречено 'revised from ...', иначе (None, None)
    """
    if not text:
        return None, None
    m = RE_REVISED.search(text)
    if not m:
        return None, None
    tail = m.group(1).strip()
    return _to_scalar(tail)

def _clean(s: str) -> str:
    return _normalize_spaces(s).replace("  ", " ")

def _idx_for(heads: List[str], keys: List[str], default: int) -> int:
    for i, h in enumerate(heads):
        if any(k in h for k in keys):
            return i
    return default

# ============== Table detection & diagnostics ==========
def _score_heads(heads: List[str]) -> int:
    line = " ".join(heads)
    score = 0
    score += 5 if any(k in line for k in _HEAD_ACTUAL_KEYS) else 0
    score += 5 if any(k in line for k in _HEAD_FORECAST_KEYS) else 0
    score += 5 if any(k in line for k in _HEAD_PREV_KEYS) else 0
    return score

def _pick_target_table(soup: BeautifulSoup):
    tables = soup.select("table")
    if not tables:
        return None, None, {"tables_found": 0, "note": "[TABLE] no <table> found"}

    best = (None, [], -1)
    diag = {"tables_found": len(tables), "candidates": []}

    for tb in tables:
        heads = [th.get_text(" ", strip=True).lower() for th in tb.select("thead th")]
        sc = _score_heads(heads) if heads else 0
        diag["candidates"].append({"heads": heads, "score": sc})
        if sc > best[2]:
            best = (tb, heads, sc)

    if best[0] is not None and best[2] > 0:
        diag["picked"] = {"heads": best[1], "score": best[2]}
        return best[0], best[1], diag

    # если <thead> пуст, пробуем по строкам
    for tb in tables:
        trs = tb.select("tbody tr")
        if not trs:
            continue
        for tr in trs[:3]:
            tds = [td.get_text(" ", strip=True).lower() for td in tr.select("td")]
            if len(tds) >= 5 and any("%" in x or re.search(r"\d", x) for x in tds):
                heads = [th.get_text(" ", strip=True).lower() for th in tb.select("thead th")]
                diag["picked"] = {"heads": heads, "score": 1, "fallback": True}
                return tb, heads if heads else [], diag

    tb = tables[-1]
    heads = [th.get_text(" ", strip=True).lower() for th in tb.select("thead th")]
    diag["picked"] = {"heads": heads, "score": 0, "fallback": "last_table"}
    return tb, heads, diag

# ============== DateTime helper ==========
_MONTHS = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
    "янв":1,"фев":2,"мар":3,"апр":4,"май":5,"июн":6,"июл":7,"авг":8,"сен":9,"ноя":11,"дек":12,
}

def _to_iso(date_text: str, time_text: str) -> Optional[str]:
    if not date_text or not time_text:
        return None
    d = _clean(date_text)
    t = _clean(time_text)[:5]
    day = month = year = None

    m = re.match(r"^(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})$", d)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100: year += 2000
    else:
        m = re.match(r"^(\d{1,2})\s+([A-Za-zА-Яа-я\.]{3,})\s+(\d{2,4})$", d)
        if m:
            day = int(m.group(1))
            mon_txt = m.group(2).strip(".").lower()
            month = _MONTHS.get(mon_txt)
            year = int(m.group(3)); 
            if year < 100: year += 2000

    if not (day and month and year):
        return None

    hm = re.match(r"^(\d{1,2}):(\d{2})$", t)
    hh, mm = (int(hm.group(1)), int(hm.group(2))) if hm else (0, 0)

    try:
        if _tz:
            import datetime as _dt
            dt = _tz.localize(_dt.datetime(year, month, day, hh, mm, 0))
            return dt.isoformat()
    except Exception:
        pass
    return None

# ============== Public API ==============
def fetch_table_rows(url: str, limit: int = 12) -> Tuple[list, Optional[str]]:
    """
    Возвращает (rows, err).
    rows: [{date,time,actual,forecast,previous, actual_val,actual_unit,..., release_dt_iso?, revised_from_*?}]
    """
    code, html, net_err, net_note = _get(url)
    if code != 200 or not html:
        return [], f"{net_err or '[NET] HTTP error'} | note: {net_note}"

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as e:
        return [], f"[HTML] soup fail: {e}"

    target, heads_for_log, diag = _pick_target_table(soup)
    if target is None:
        return [], f"[TABLE] not found | diag: {diag}"

    rows = []
    bad_rows = 0

    # индексы колонок
    heads = heads_for_log or []
    idx_actual   = _idx_for(heads, _HEAD_ACTUAL_KEYS,   -3)
    idx_forecast = _idx_for(heads, _HEAD_FORECAST_KEYS, -2)
    idx_previous = _idx_for(heads, _HEAD_PREV_KEYS,     -1)

    # если индексы «минусовые», зафиксируем в диагностике
    idx_diag = {"idx_actual": idx_actual, "idx_forecast": idx_forecast, "idx_previous": idx_previous}
    diag["col_indexing"] = idx_diag

    for tr in target.select("tbody tr"):
        tds = tr.select("td")
        if len(tds) < 5:
            bad_rows += 1
            continue

        vals = [td.get_text(" ", strip=True) for td in tds]

        def cell(i):
            try:
                return vals[i]
            except Exception:
                return ""

        date_text     = cell(0)
        time_text     = cell(1)
        actual_text   = cell(idx_actual)
        forecast_text = cell(idx_forecast)
        previous_text = cell(idx_previous)

        act_v, act_u = _to_scalar(actual_text)
        fc_v,  fc_u  = _to_scalar(forecast_text)
        pr_v,  pr_u  = _to_scalar(previous_text)

        # revised from …
        rev_v, rev_u = _parse_revised(actual_text)

        row: Dict[str, Any] = {
            "date": _clean(date_text),
            "time": _clean(time_text),
            "actual": _clean(actual_text),
            "forecast": _clean(forecast_text),
            "previous": _clean(previous_text),
            "actual_val": act_v, "actual_unit": act_u,
            "forecast_val": fc_v, "forecast_unit": fc_u,
            "previous_val": pr_v, "previous_unit": pr_u,
        }

        if rev_v is not None:
            row["revised_from_val"] = rev_v
            row["revised_from_unit"] = rev_u

        iso = _to_iso(row["date"], row["time"])
        if iso:
            row["release_dt_iso"] = iso

        rows.append(row)

    if not rows:
        return [], f"[ROW] no rows parsed | diag: {diag} | bad_rows: {bad_rows}"

    return rows[:limit], None

# ============== Formatting (text) ==============
def _fmt_val(v, u):
    if v is None:
        return "ожид"
    if u == "percent":
        s = f"{v:.2f}".rstrip("0").rstrip(".")
        return s + "%"
    absv = abs(v)
    if absv >= 1_000_000_000_000:
        s = f"{v/1_000_000_000_000:.2f}".rstrip("0").rstrip(".")
        return s + "T"
    if absv >= 1_000_000_000:
        s = f"{v/1_000_000_000:.2f}".rstrip("0").rstrip(".")
        return s + "B"
    if absv >= 1_000_000:
        s = f"{v/1_000_000:.2f}".rstrip("0").rstrip(".")
        return s + "M"
    if absv >= 1_000:
        s = f"{v/1_000:.2f}".rstrip("0").rstrip(".")
        return s + "K"
    return f"{v:.2f}".rstrip("0").rstrip(".")

def format_table_for_tg(rows, src_url: str, max_rows: int = 6) -> str:
    headers = ["Release Date", "Time", "Actual", "Forecast", "Previous"]
    data = []
    for r in (rows or [])[:max_rows]:
        date = r.get("date", "")[:12]
        tm   = (r.get("time", "") or "")[:5]
        actual   = _fmt_val(r.get("actual_val"),   r.get("actual_unit"))
        forecast = _fmt_val(r.get("forecast_val"), r.get("forecast_unit"))
        previous = _fmt_val(r.get("previous_val"), r.get("previous_unit"))
        data.append([date, tm, actual, forecast, previous])
    if not data:
        data = [["—","—","—","—","—"]]

    W_DATE, W_TIME, W_ACT, W_FC, W_PR = 12, 5, 12, 9, 9
    widths = [W_DATE, W_TIME, W_ACT, W_FC, W_PR]

    def cut_pad(s: str, w: int) -> str:
        s = (s or "").strip()
        if len(s) > w:
            s = s[:w-1] + "…"
        return s.ljust(w)

    V, H = "│", "─"
    TL, TR, BL, BR = "┌", "┐", "└", "┘"
    T, B, X, LJ, RJ = "┬", "┴", "┼", "├", "┤"

    def line(left, mid, right):
        parts = [H * w for w in widths]
        return left + mid.join(parts) + right

    top = line(TL, T, TR)
    mid = line(LJ, X, RJ)
    bot = line(BL, B, BR)

    head = (
        V + cut_pad(headers[0], W_DATE)
        + V + cut_pad(headers[1], W_TIME)
        + V + cut_pad(headers[2], W_ACT)
        + V + cut_pad(headers[3], W_FC)
        + V + cut_pad(headers[4], W_PR) + V
    )

    body_lines = []
    for row in data:
        line = (
            V + cut_pad(row[0], W_DATE)
            + V + cut_pad(row[1], W_TIME)
            + V + cut_pad(row[2], W_ACT)
            + V + cut_pad(row[3], W_FC)
            + V + cut_pad(row[4], W_PR) + V
        )
        body_lines.append(line)

    table = "\n".join([top, head, mid, *body_lines, bot])
    footer = f'\n<b>Источник:</b> <a href="{src_url}">Investing.com</a>'
    return f"<pre>{table}</pre>{footer}"

# ============== PNG (modern 2025 look) ==============
def _load_font_candidates(size, bold=False, mono=False):
    # Моно для чисел, Sans для остального
    mono_paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "C:/Windows/Fonts/consola.ttf",
        "C:/Windows/Fonts/lucon.ttf",
    ]
    sans_paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
    ]
    paths = mono_paths if mono else sans_paths
    for p in paths:
        try:
            return ImageFont.truetype(p, size=size)
        except Exception:
            continue
    return ImageFont.load_default()

def _text_size(draw, text, font):
    x0, y0, x1, y1 = draw.textbbox((0, 0), str(text), font=font)
    return (x1 - x0), (y1 - y0)

def _auto_col_widths(rows, headers, draw, f_head, f_cell):
    # Базовые рамки ширин (px)
    min_w = [170, 96, 120, 120, 120]
    max_w = [260, 140, 220, 220, 220]
    widths = min_w[:]

    # Заголовки
    for i, head in enumerate(headers):
        w, _ = _text_size(draw, head, f_head)
        widths[i] = max(widths[i], w + 28)

    # Данные (до 12 строк)
    sample = rows[:12] if rows else []
    for r in sample:
        cells = [
            (r.get("date", "") or "—").replace(", ", " "),
            (r.get("time", "") or "—")[:5],
            _fmt_val(r.get("actual_val"),   r.get("actual_unit")),
            _fmt_val(r.get("forecast_val"), r.get("forecast_unit")),
            _fmt_val(r.get("previous_val"), r.get("previous_unit")),
        ]
        for i, txt in enumerate(cells):
            w, _ = _text_size(draw, txt, f_cell if i >= 2 else f_head)
            widths[i] = max(widths[i], w + 28)

    widths = [min(widths[i], max_w[i]) for i in range(len(widths))]
    for i in (2, 3, 4):  # чуть больше места числам
        widths[i] += 10
    return widths

def render_table_png(rows, title: str, max_rows: int = 8):
    headers = ["RELEASE DATE", "TIME", "ACTUAL", "FORECAST", "PREVIOUS"]  # капс для чистоты сетки

    # Собираем видимые ряды
    tbl = []
    for r in (rows or [])[:max_rows]:
        date = (r.get("date", "") or "—").replace(", ", " ")
        if len(date) > 18:   # аккуратная обрезка без многоточий
            date = date[:18]
        tm   = (r.get("time", "") or "—")[:5]
        actual   = _fmt_val(r.get("actual_val"),   r.get("actual_unit"))
        forecast = _fmt_val(r.get("forecast_val"), r.get("forecast_unit"))
        previous = _fmt_val(r.get("previous_val"), r.get("previous_unit"))
        tbl.append([date, tm, actual, forecast, previous])
    if not tbl:
        tbl = [["—","—","—","—","—"]]

    # Цвета (светлая премиум-палитра)
    BG         = (246, 248, 252)     # общий фон
    CARD       = (255, 255, 255, 255)# карточка
    SHADOW     = (0, 0, 0, 46)       # мягкая тень
    GRID       = (225, 230, 238, 255)# линии сетки
    BORDER     = (214, 221, 232, 255)# рамка карточки
    HEAD_L     = (244, 248, 255, 255)# градиент шапки слева
    HEAD_R     = (235, 241, 255, 255)# градиент шапки справа
    CHIP_BG    = (227, 235, 250, 255)# «чипы» под заголовки
    ROW_A      = (252, 253, 255, 255)# зебра 1
    ROW_B      = (246, 248, 252, 255)# зебра 2
    TEXT       = (28, 35, 49, 255)   # основной текст
    SUB        = (109, 120, 137, 255)# подпись
    ACCENT     = (33, 102, 245, 255) # синий акцент

    # Геометрия
    PAD = 28
    GAP = 12
    TITLE_H = 58
    SUB_H   = 22
    HEAD_H  = 52
    ROW_H   = 48
    FOOT_H  = 24
    RADIUS  = 18

    # Шрифты (моно — только для чисел)
    f_title = _load_font_candidates(28, bold=True)
    f_sub   = _load_font_candidates(14)
    f_head  = _load_font_candidates(15, bold=True)
    f_cellL = _load_font_candidates(16)             # левые колонки
    f_cellR = _load_font_candidates(16, mono=True)  # числа — моно для ровной колонки

    # Канва для измерений
    tmp = Image.new("RGBA", (10, 10), (0,0,0,0))
    dtmp = ImageDraw.Draw(tmp)

    # Авто-ширины
    # NB: передадим оригинальные rows для корректной оценки (если есть)
    col_w = _auto_col_widths(rows or [], headers, dtmp, f_head, f_cellR)
    table_w = sum(col_w)

    # Итоговые размеры
    width  = PAD*2 + table_w
    height = PAD*2 + TITLE_H + SUB_H + GAP + HEAD_H + len(tbl)*ROW_H + GAP + FOOT_H

    # Фон
    base = Image.new("RGBA", (width, height), BG)

    # Тень под карточкой (мягкая, без смаза линий)
    shadow = Image.new("RGBA", (width, height), (0,0,0,0))
    sd = ImageDraw.Draw(shadow)
    card_rect = [PAD-2, PAD-2 + TITLE_H + SUB_H, width-PAD+2, height-PAD-8]
    sd.rounded_rectangle(card_rect, radius=RADIUS+6, fill=SHADOW)
    shadow = shadow.filter(ImageFilter.GaussianBlur(radius=12))
    base.alpha_composite(shadow)

    # Карточка-основание
    card = Image.new("RGBA", (width, height), (0,0,0,0))
    cd = ImageDraw.Draw(card)
    cd.rounded_rectangle(card_rect, radius=RADIUS, fill=CARD, outline=BORDER, width=1)

    # Заголовок
    d = ImageDraw.Draw(base)
    d.text((PAD, PAD-2), title, fill=TEXT, font=f_title)
    d.text((PAD, PAD - 2 + TITLE_H - 18), "source: investing.com", fill=SUB, font=f_sub)

    # Хелперы точных линий
    def vline(x, y0, y1, color): cd.rectangle([x, y0, x, y1], fill=color)
    def hline(x0, x1, y, color): cd.rectangle([x0, y, x1, y], fill=color)

    # Шапка с градиентом
    x0 = PAD
    y0 = PAD + TITLE_H + SUB_H + GAP
    head_rect = [x0, y0, x0 + table_w, y0 + HEAD_H]

    grad = Image.new("RGBA", (table_w, HEAD_H), (0,0,0,0))
    gd = ImageDraw.Draw(grad)
    for i in range(table_w):
        t = i / max(1, table_w - 1)
        r = int(HEAD_L[0]*(1-t) + HEAD_R[0]*t)
        g = int(HEAD_L[1]*(1-t) + HEAD_R[1]*t)
        b = int(HEAD_L[2]*(1-t) + HEAD_R[2]*t)
        gd.line([(i,0),(i,HEAD_H)], fill=(r,g,b,255))
    card.alpha_composite(grad, dest=(x0, y0))

    # «Чипы» под заголовки — лёгкие плашки для современного вида
    cx = x0
    for i, h in enumerate(headers):
        chip_x = cx + 10
        chip_y = y0 + 9
        chip_w = min(col_w[i] - 20, 200)
        chip_h = HEAD_H - 18
        cd.rounded_rectangle(
            [chip_x, chip_y, chip_x + chip_w, chip_y + chip_h],
            radius=10, fill=CHIP_BG
        )
        # Текст хедера по центру чипа
        tw, th = _text_size(cd, h, f_head)
        tx = chip_x + (chip_w - tw)//2
        ty = chip_y + (chip_h - th)//2
        cd.text((tx, ty), h, fill=TEXT, font=f_head)
        cx += col_w[i]

    # Разделители шапки
    cx = x0
    for i in range(len(col_w) - 1):
        cx += col_w[i]
        vline(cx, y0, y0 + HEAD_H, GRID)
    hline(x0, x0 + table_w, y0 + HEAD_H, GRID)

    # Тело таблицы (ровная зебра, правое выравнивание чисел)
    y = y0 + HEAD_H
    for r_i, row in enumerate(tbl):
        row_rect = [x0, y, x0 + table_w, y + ROW_H]
        cd.rectangle(row_rect, fill=ROW_A if (r_i % 2 == 0) else ROW_B)

        cx = x0
        for i, val in enumerate(row):
            text_val = str(val)
            # слева — выравнивание влево, справа — вправо (моно-шрифт)
            if i <= 1:
                font = f_cellL
                tw, th = _text_size(cd, text_val, font)
                tx = cx + 14
                ty = y + (ROW_H - th)//2
                cd.text((tx, ty), text_val, fill=TEXT, font=font)
            else:
                font = f_cellR
                tw, th = _text_size(cd, text_val, font)
                tx = cx + col_w[i] - 14 - tw
                ty = y + (ROW_H - th)//2
                cd.text((tx, ty), text_val, fill=TEXT, font=font)

            # Вертикальные разделители строго на целых координатах
            if i < len(col_w) - 1:
                vline(cx + col_w[i], y, y + ROW_H, GRID)
            cx += col_w[i]

        hline(x0, x0 + table_w, y + ROW_H, GRID)
        y += ROW_H

        # Подвал (внутри карточки, прижат к нижнему краю с отступом)
        tip = "Tip: compare Fact vs Forecast"
        tw, th = _text_size(cd, tip, f_sub)
        cd.text(
       (card_rect[0] + 14, card_rect[3] - th - 10),
        tip,
        fill=ACCENT,
       font=f_sub
       )

    # Сливаем карточку на фон
    base.alpha_composite(card)

    # В RGB без альфы
    out = Image.new("RGB", (width, height), BG)
    out.paste(base, mask=base.split()[-1])

    buf = io.BytesIO()
    out.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf.getvalue(), "indicator_table.png"
