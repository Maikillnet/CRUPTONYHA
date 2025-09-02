# bot.py
import asyncio
import html
import logging
import os
import random
import re
import socket
from typing import Dict, List, Optional, Tuple

import pytz
from config import BOT_TOKEN, TZ_NAME, POLL_MIN_SEC as MIN_SEC, POLL_MAX_SEC as MAX_SEC
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, KeyboardButton, Message, ReplyKeyboardMarkup

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from parser_altseason import (
    fetch_altseason_index,
    render_altseason_card,
    format_altseason_text,
    format_altseason_status,
    fetch_altseason_stats,
    format_altseason_stats,
)
DEFAULT_IND = "JOBLESS_CLAIMS"
# --- ALTSEASON (синтетический индикатор)
ALTSEASON_KEY = "ALTSEASON"
ALTSEASON_TITLE = "Индекс альтсезона"
ALTSEASON_URL = "https://www.blockchaincenter.net/en/altcoin-season-index/"

if not re.fullmatch(r"\d{7,12}:[A-Za-z0-9_-]{35,}", BOT_TOKEN or ""):
    raise RuntimeError("BOT_TOKEN некорректен или пуст. Проверь config.py.")

tz = pytz.timezone(TZ_NAME)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("jobless-extended")

from storage import (
    init_db, add_sub, list_subs, set_state, get_state,
    add_custom_indicator, delete_custom_indicator_by_title,
    list_custom_indicators,
)
from indicators import get_indicators, PRESET_INDICATORS, rules_hints
from parser_investing_generic import (
    fetch_table_rows as fetch_rows_generic,
    format_table_for_tg as format_tg_generic,
    render_table_png as render_png_generic,
)

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

def h(x) -> str:
    return html.escape(str(x or ""))

# ==== UI labels ====
BTN_INVESTING_MAIN = "Прогноз по investing.com"
BTN_TABLE_PNG = "📋 Таблица (PNG)"
BTN_TABLE_TEXT = "🧾 Таблица (текст)"
BTN_CHECK = "📡 Проверить сейчас"
BTN_BACK = "⬅️ Назад"
BTN_SETTINGS = "⚙️ Настройки"

BTN_SET_TIME = "🕒 Время"
BTN_SET_DAYS = "📅 Дни"
BTN_SET_IND = "📈 Индикатор"
BTN_ENABLE = "✅ Включить"
BTN_DISABLE = "⛔️ Выключить"
BTN_TEST = "🧪 Протестировать"
BTN_SETTINGS_BACK = "⬅️ В меню"

BTN_IND_ADD = "➕ Добавить индикатор"
BTN_IND_DEL = "➖ Удалить индикатор"

# НОВОЕ
BTN_CAL_OVERVIEW = "🗓️ Календарь индикаторов"

# --- ALT season UI ---
BTN_ALTSEASON_MAIN  = "📊 Статус альтсезона"
BTN_ALTSEASON_CHECK = "📡 Узнать статус"

# ==== state keys ====
def _k_curr_ind(chat_id: int) -> str:             return f"sched2:current_ind:{chat_id}"
def _k_enabled(chat_id: int, ind: str) -> str:    return f"sched2:enabled:{chat_id}:{ind}"
def _k_time(chat_id: int, ind: str) -> str:       return f"sched2:time:{chat_id}:{ind}"
def _k_days(chat_id: int, ind: str) -> str:       return f"sched2:days:{chat_id}:{ind}"
def _k_ui_state(chat_id: int) -> str:             return f"ui:state:{chat_id}"
def _k_tmp(chat_id: int, name: str) -> str:       return f"ui:tmp:{chat_id}:{name}"

async def _get_selected_key(chat_id: int) -> str:
    key = await get_state(f"last_indicator:{chat_id}")
    return key or DEFAULT_IND

# ==== parsing days/time ====
_RU2EN = {
    "пн": "mon", "вт": "tue", "ср": "wed", "чт": "thu",
    "пт": "fri", "сб": "sat", "вс": "sun",
    "пон": "mon", "сред": "wed", "чет": "thu", "пят": "fri", "суб": "sat", "воск": "sun",
}
_EN2RU = {"mon":"пн","tue":"вт","wed":"ср","thu":"чт","fri":"пт","sat":"сб","sun":"вс"}
_VALID_DAYS = list(_EN2RU.keys())

def _normalize_days(s: str) -> Optional[str]:
    s = (s or "").strip().lower()
    if not s:
        return None
    if s in ("ежедневно", "каждый день", "daily", "every day"):
        return "mon-sun"
    for ru, en in _RU2EN.items():
        s = re.sub(rf"\b{ru}\b", en, s)
    s = s.replace(" ", "").replace("пон", "mon")
    m = re.fullmatch(r"(mon|tue|wed|thu|fri|sat|sun)-(mon|tue|wed|thu|fri|sat|sun)", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    parts = [p for p in s.split(",") if p]
    if parts and all(p in _VALID_DAYS for p in parts):
        return ",".join(parts)
    if s in _VALID_DAYS:
        return s
    return None

def _days_to_ru(d: Optional[str]) -> Optional[str]:
    if not d:
        return None
    if d == "mon-sun":
        return "каждый день"
    m = re.fullmatch(r"(mon|tue|wed|thu|fri|sat|sun)-(mon|tue|wed|thu|fri|sat|sun)", d)
    if m:
        return f"{_EN2RU[m.group(1)]}–{_EN2RU[m.group(2)]}"
    parts = [p for p in d.split(",") if p]
    if parts:
        return ",".join(_EN2RU.get(p, p) for p in parts)
    return None

_TIME_RE = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")
def _parse_hhmm(s: str) -> Optional[Tuple[int,int]]:
    m = _TIME_RE.fullmatch((s or "").strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))

# ==== keyboards ====

async def get_indicators_with_altseason(chat_id: int) -> Dict[str, dict]:
    """Карта индикаторов + синтетический ALTSEASON для настроек/расписания."""
    IND = await get_indicators(chat_id)
    IND = dict(IND)  # копия
    IND[ALTSEASON_KEY] = {
        "title": ALTSEASON_TITLE,
        "url": ALTSEASON_URL,
        "rule": None,  # для ALTSEASON правило не используется
    }
    return IND

async def indicators_kb(chat_id: int):
    """
    Меню investing.com (без ALTSEASON — у него своё меню).
    """
    IND = await get_indicators(chat_id)
    current_key = await get_state(f"last_indicator:{chat_id}") or DEFAULT_IND

    preset_keys = [k for k in PRESET_INDICATORS.keys() if k in IND]
    custom_items = sorted(
        [(k, v["title"]) for k, v in IND.items() if k not in PRESET_INDICATORS],
        key=lambda kv: kv[1].lower()
    )
    custom_keys = [k for k, _ in custom_items]
    ordered_keys = preset_keys + custom_keys

    buttons, row = [], []
    for key in ordered_keys:
        meta = IND[key]
        title = meta["title"]
        prefix = "✅ " if key == current_key else ""
        sticker = "🧩 " if key not in PRESET_INDICATORS else ""
        text = f"{prefix}{sticker}{title}"
        row.append(KeyboardButton(text=text))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([KeyboardButton(text=BTN_BACK)])
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=buttons)

async def indicators_only_kb(chat_id: int, *, customs_only: bool=False):
    """Клавиатура выбора индикатора в настройках (включая ALTSEASON)."""
    IND = await get_indicators_with_altseason(chat_id)
    buttons, row = [], []
    for key, meta in IND.items():
        if customs_only:
            if key in PRESET_INDICATORS or key == ALTSEASON_KEY:
                continue
        row.append(KeyboardButton(text=meta["title"]))
        if len(row) == 2:
            buttons.append(row); row = []
    if row:
        buttons.append(row)
    buttons.append([KeyboardButton(text=BTN_SETTINGS_BACK)])
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=buttons)

def root_kb():
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text=BTN_INVESTING_MAIN)],
            [KeyboardButton(text=BTN_ALTSEASON_MAIN)],
            [KeyboardButton(text=BTN_SETTINGS)],
        ],
    )

def altseason_kb():
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text=BTN_ALTSEASON_CHECK)],
            [KeyboardButton(text=BTN_BACK)],
        ],
    )

def settings_kb():
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text=BTN_CAL_OVERVIEW)],
            [KeyboardButton(text=BTN_SET_TIME), KeyboardButton(text=BTN_SET_DAYS)],
            [KeyboardButton(text=BTN_SET_IND)],
            [KeyboardButton(text=BTN_ENABLE), KeyboardButton(text=BTN_DISABLE)],
            [KeyboardButton(text=BTN_TEST)],
            [KeyboardButton(text=BTN_IND_ADD), KeyboardButton(text=BTN_IND_DEL)],
            [KeyboardButton(text=BTN_SETTINGS_BACK)],
        ],
    )

def key_by_title_sync(ind_map: Dict[str, dict], title: str) -> Optional[str]:
    for k, v in ind_map.items():
        if v["title"] == title:
            return k
    return None

# ==== investing helpers ====
def _signal_from_rows(rows, ind_key: str, ind_map: Dict[str, dict]) -> str:
    meta = ind_map[ind_key]
    rule = meta.get("rule")
    top = rows[0] if rows else None
    title = h(meta.get("title", ind_key))

    if not top:
        return f"{title}\nНет данных: таблица пуста."

    # Утилита приведения к float
    def _to_float(x):
        try:
            return float(x)
        except Exception:
            return None

    # Готовим числа
    a_raw, f_raw, p_raw = top.get("actual_val"), top.get("forecast_val"), top.get("previous_val")
    a, f, p = _to_float(a_raw), _to_float(f_raw), _to_float(p_raw)

    # Красивые форматированные строки из generic-парсера
    from parser_investing_generic import _fmt_val as fmt
    a_str = fmt(a_raw, top.get("actual_unit"))
    f_str = fmt(f_raw, top.get("forecast_unit"))
    p_str = fmt(p_raw, top.get("previous_unit"))

    # Безопасное применение правила
    sig: str
    try:
        if callable(rule):
            sig = rule(top)
        elif isinstance(rule, str):
            r = rule.upper()
            if r == "FOMC":
                if a is None or p is None:
                    sig = "⏳ Ждём цифры по ставке."
                else:
                    if a > p:
                        sig = "Повышение ставки"
                    elif a < p:
                        sig = "Снижение ставки"
                    else:
                        sig = "Без изменений"
            elif r == "LT":
                if a is None or f is None:
                    sig = "Ждём факт/прогноз"
                else:
                    if a < f:
                        sig = "Лучше прогноза (меньше — лучше)"
                    elif a > f:
                        sig = "Хуже прогноза (меньше — лучше)"
                    else:
                        sig = "На уровне прогноза"
            elif r == "GT":
                if a is None or f is None:
                    sig = "Ждём факт/прогноз"
                else:
                    if a > f:
                        sig = "Лучше прогноза (больше — лучше)"
                    elif a < f:
                        sig = "Хуже прогноза (больше — лучше)"
                    else:
                        sig = "На уровне прогноза"
            else:
                sig = f"Правило {r} не распознано"
        else:
            sig = "Правило не задано"
    except Exception as e:
        # Если что-то пошло не так — не ломаем отправку текста.
        sig = f"Ошибка правила: {e}"

    # Спец-ветка для FOMC (как и раньше)
    if isinstance(rule, str) and rule.upper() == "FOMC":
        if a is None or p is None:
            return f"{title}\n⏳ Ждём цифры по ставке."
        return f"{title}\nФакт: {a_str} • Пред.: {p_str} → {sig}"

    # Общий текст для остальных индикаторов
    if a is None and f is not None:
        return f"{title}\n⏳ Данные ещё не вышли. Прогноз: {f_str}"
    if a is not None and f is None and p is not None:
        return f"{title}\n⚠️ Факт: {a_str}, прогноз не распознан. Пред.: {p_str} → {sig}"
    if a is not None and f is None:
        return f"{title}\n⚠️ Факт: {a_str}, прогноз не распознан → {sig}"
    # Обычный случай: есть и факт, и прогноз
    return f"{title}\nФакт: {a_str} • Прогноз: {f_str} → {sig}"

# ==== sending ====
async def _send_table_text(m: Message, ind_key: str):
    if ind_key == ALTSEASON_KEY:
        try:
            idx, used_url = fetch_altseason_index()
            await m.answer(format_altseason_text(int(idx), used_url), disable_web_page_preview=True)
            try:
                stats = fetch_altseason_stats()
                await m.answer(format_altseason_stats(stats), disable_web_page_preview=True)
            except Exception as e:
                await m.answer(f"ℹ️ Табличную сводку получить не удалось: {h(e)}")
        except Exception as e:
            await m.answer(f"⚠️ Не удалось получить индекс альтсезона: {h(e)}")
        return

    IND = await get_indicators(m.chat.id)
    meta = IND[ind_key]
    rows, err = fetch_rows_generic(meta["url"])
    if err:
        await m.answer(f"⚠️ Не удалось получить таблицу: {h(err)}")
        return
    msg = format_tg_generic(rows, src_url=meta["url"], max_rows=8)
    await m.answer(msg, disable_web_page_preview=True)

async def _send_table_png(m: Message, ind_key: str):
    if ind_key == ALTSEASON_KEY:
        try:
            idx, _ = fetch_altseason_index()
            png, fname = render_altseason_card(int(idx))
            await m.answer_document(BufferedInputFile(png, filename=fname), caption=ALTSEASON_TITLE)
        except Exception as e:
            await m.answer(f"⚠️ Не удалось получить индекс альтсезона: {h(e)}")
        return

    IND = await get_indicators(m.chat.id)
    meta = IND[ind_key]
    rows, err = fetch_rows_generic(meta["url"])
    if err:
        await m.answer(f"⚠️ Не удалось получить таблицу: {h(err)}")
        return
    png_bytes, fname = render_png_generic(rows, title=meta["title"], max_rows=8)
    file = BufferedInputFile(png_bytes, filename=fname)
    await m.answer_document(file, caption=h(meta["title"]))

@dp.message(F.text == BTN_CHECK)
async def cmd_check(m: Message):
    ind_key = await _get_selected_key(m.chat.id)
    if ind_key == ALTSEASON_KEY:
        try:
            idx, used_url = fetch_altseason_index()
            png, fname = render_altseason_card(int(idx))
            await m.answer_document(BufferedInputFile(png, filename=fname), caption=ALTSEASON_TITLE)
            await m.answer(
                format_altseason_status(int(idx)) + f"\n\n<i>Источник</i>: {used_url}",
                disable_web_page_preview=True
            )
            try:
                stats = fetch_altseason_stats()
                await m.answer(format_altseason_stats(stats), disable_web_page_preview=True)
            except Exception as e:
                await m.answer(f"ℹ️ Табличную сводку получить не удалось: {h(e)}")
        except Exception as e:
            await m.answer(f"⚠️ Не удалось получить индекс альтсезона: {h(e)}")
        return

    IND = await get_indicators(m.chat.id)
    meta = IND[ind_key]
    rows, err = fetch_rows_generic(meta["url"])
    if err:
        await m.answer(f"⚠️ Не удалось получить данные: {h(err)}")
        return
    png_bytes, fname = render_png_generic(rows, title=meta["title"], max_rows=8)
    await m.answer_document(BufferedInputFile(png_bytes, filename=fname))
    await m.answer(_signal_from_rows(rows, ind_key, IND), disable_web_page_preview=True)

async def send_indicator_update_for_chat(chat_id: int, ind_key: str):
    if ind_key == ALTSEASON_KEY:
        try:
            idx, used_url = fetch_altseason_index()
            png, fname = render_altseason_card(int(idx))
            await bot.send_document(chat_id, BufferedInputFile(png, filename=fname), caption=ALTSEASON_TITLE)
            await bot.send_message(
                chat_id,
                format_altseason_status(int(idx)) + f"\n\n<i>Источник</i>: {used_url}",
                disable_web_page_preview=True
            )
            try:
                stats = fetch_altseason_stats()
                await bot.send_message(chat_id, format_altseason_stats(stats), disable_web_page_preview=True)
            except Exception as e:
                await bot.send_message(chat_id, f"ℹ️ Табличную сводку получить не удалось: {h(e)}")
        except Exception as e:
            await bot.send_message(chat_id, f"⚠️ Не удалось получить индекс альтсезона: {h(e)}")
        return

    IND = await get_indicators(chat_id)
    meta = IND[ind_key]
    rows, err = fetch_rows_generic(meta["url"])
    if err:
        await bot.send_message(chat_id, f"⚠️ {h(meta['title'])}: не удалось получить данные: {h(err)}")
        return
    try:
        png_bytes, fname = render_png_generic(rows, title=meta["title"], max_rows=8)
        file = BufferedInputFile(png_bytes, filename=fname)
        await bot.send_document(chat_id, file, caption=h(meta["title"]))
    except Exception as e:
        log.warning("send PNG fail %s: %s", ind_key, e)
    await bot.send_message(chat_id, _signal_from_rows(rows, ind_key, IND), disable_web_page_preview=True)

# ==== scheduler ====
scheduler: Optional[AsyncIOScheduler] = None
def _job_id(chat_id: int, ind: str) -> str: return f"user_sched_{chat_id}_{ind}"

async def reschedule_user_job(chat_id: int, ind_key: str):
    """
    Перепланировать задачу пользователя для выбранного индикатора.
    НИЧЕГО не планируем, если:
      - выключено,
      - не задано время,
      - не заданы дни,
      - неверный формат времени/дней.
    """
    global scheduler
    if scheduler is None:
        return

    job_id = _job_id(chat_id, ind_key)

    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass

    enabled = await get_state(_k_enabled(chat_id, ind_key))
    if enabled != "1":
        log.info("User %s %s: schedule disabled -> skip", chat_id, ind_key)
        return

    time_str = (await get_state(_k_time(chat_id, ind_key))) or ""
    days_str = (await get_state(_k_days(chat_id, ind_key))) or ""

    if not time_str or not days_str:
        log.info("User %s %s: time/days not set -> skip (time='%s', days='%s')",
                 chat_id, ind_key, time_str, days_str)
        return

    hhmm = _parse_hhmm(time_str)
    if not hhmm:
        log.warning("User %s %s: bad time format '%s' -> skip", chat_id, ind_key, time_str)
        return
    hh, mm = hhmm

    dow = _normalize_days(days_str)
    if not dow:
        log.warning("User %s %s: bad days format '%s' -> skip", chat_id, ind_key, days_str)
        return

    async def _job(chat_id=chat_id, ind_key=ind_key):
        await send_indicator_update_for_chat(chat_id, ind_key)

    trig_kwargs = {"hour": hh, "minute": mm, "timezone": tz}
    if dow != "mon-sun":
        trig_kwargs["day_of_week"] = dow

    try:
        trig = CronTrigger(**trig_kwargs)
        job = scheduler.add_job(
            _job,
            trig,
            id=job_id,
            replace_existing=True,
            misfire_grace_time=600,
            max_instances=1,
            coalesce=True,
        )
    except Exception as e:
        log.error("User %s %s: failed to schedule (%s)", chat_id, ind_key, e)
        return

    import datetime as dt
    now_msk = dt.datetime.now(tz)
    try:
        next_run = job.trigger.get_next_fire_time(None, now_msk)
    except Exception:
        next_run = None

    log.info("Scheduled user %s for %s: %s at %02d:%02d (%s) | next=%s",
             chat_id, ind_key, dow, hh, mm, TZ_NAME, next_run)

    if next_run:
        try:
            IND = await get_indicators_with_altseason(chat_id)
            title = h(IND.get(ind_key, {}).get("title", ind_key))
            ts = next_run.strftime("%Y-%m-%d %H:%M:%S %Z")
            await bot.send_message(chat_id, f"🗓️ <b>{title}</b>\nСледующий автопрогноз: <b>{h(ts)}</b>")
        except Exception:
            pass

        try:
            if 0 <= (next_run - now_msk).total_seconds() < 30:
                await bot.send_message(chat_id, "⏱️ Сlot уже на носу — отправляю сейчас и дальше по расписанию.")
                await send_indicator_update_for_chat(chat_id, ind_key)
        except Exception as e:
            log.warning("Immediate-fire check failed: %s", e)

# ==== commands / menus ====
@dp.message(Command("start", "menu"))
async def cmd_start(m: Message):
    chat_id = m.chat.id
    await m.answer("🔄 Загружаю меню…")
    await add_sub(chat_id)

    curr = await get_state(_k_curr_ind(chat_id))
    last = await get_state(f"last_indicator:{chat_id}")
    if curr is None:
        await set_state(_k_curr_ind(chat_id), last or DEFAULT_IND)

    await m.answer("Выбери источник прогноза.", reply_markup=root_kb(), parse_mode=None)

@dp.message(F.text == BTN_INVESTING_MAIN)
async def open_investing_menu(m: Message):
    kb = await indicators_kb(m.chat.id)
    await m.answer("<b>Investing.com</b>\nВыберите индикатор или действие:", reply_markup=kb)

@dp.message(F.text == BTN_BACK)
async def go_back(m: Message):
    await m.answer("Возврат в главное меню.", reply_markup=root_kb())

@dp.message(F.text == BTN_SETTINGS)
async def open_settings(m: Message):
    curr = await get_state(_k_curr_ind(m.chat.id))
    IND = await get_indicators_with_altseason(m.chat.id)
    if (not curr) or (curr not in IND):
        last = await get_state(f"last_indicator:{m.chat.id}")
        curr = last if (last in IND) else DEFAULT_IND
        await set_state(_k_curr_ind(m.chat.id), curr)

    t = await get_state(_k_time(m.chat.id, curr))
    d = await get_state(_k_days(m.chat.id, curr))
    enabled = "вкл" if (await get_state(_k_enabled(m.chat.id, curr))) == "1" else "выкл"

    t_show = f"<code>{h(t)}</code>" if t else "<i>не настроено</i>"
    d_show = f"<code>{h(d)}</code>" if d else "<i>не настроено</i>"

    await set_state(_k_ui_state(m.chat.id), "")
    await m.answer(
        f"<b>⚙️ Настройки автопрогноза</b>\n"
        f"Состояние: <b>{h(enabled)}</b>\n"
        f"Индикатор: <b>{h(IND[curr]['title'])}</b>\n"
        f"Дни: {d_show}\n"
        f"Время (МСК): {t_show}\n\n"
        f"Выберите действие:",
        reply_markup=settings_kb()
    )

@dp.message(F.text == BTN_SETTINGS_BACK)
async def back_from_settings(m: Message):
    await set_state(_k_ui_state(m.chat.id), "")
    await m.answer("Готово. Возвращаемся в меню.", reply_markup=root_kb())

@dp.message(F.text == BTN_SET_IND)
async def settings_pick_indicator(m: Message):
    await set_state(_k_ui_state(m.chat.id), "await_pick_ind")
    kb = await indicators_only_kb(m.chat.id, customs_only=False)
    await m.answer("Выберите индикатор ниже — вы будете настраивать именно его.", reply_markup=kb)

@dp.message(F.text == BTN_SET_TIME)
async def settings_set_time(m: Message):
    await set_state(_k_ui_state(m.chat.id), "await_time")
    await m.answer("Отправьте время в формате <code>HH:MM</code> по МСК, например <b>15:30</b>.")

@dp.message(F.text == BTN_SET_DAYS)
async def settings_set_days(m: Message):
    await set_state(_k_ui_state(m.chat.id), "await_days")
    await m.answer(
        "Отправьте дни. Примеры:\n"
        "- <code>mon-sun</code> (ежедневно)\n"
        "- <code>mon-fri</code> (будни)\n"
        "- <code>sat,sun</code>\n"
        "- <code>mon,wed,fri</code>\n"
        "- <code>пн-чт</code>, <code>ср</code> и т.п."
    )

@dp.message(F.text == BTN_ENABLE)
async def settings_enable(m: Message):
    ind = (await get_state(_k_curr_ind(m.chat.id))) or DEFAULT_IND
    await set_state(_k_enabled(m.chat.id, ind), "1")
    IND = await get_indicators_with_altseason(m.chat.id)
    await m.answer(f"Автопрогноз для <b>{h(IND[ind]['title'])}</b> <b>включён</b>. ⏱️ Расписание применено.")
    await reschedule_user_job(m.chat.id, ind)

@dp.message(F.text == BTN_DISABLE)
async def settings_disable(m: Message):
    ind = (await get_state(_k_curr_ind(m.chat.id))) or DEFAULT_IND
    await set_state(_k_enabled(m.chat.id, ind), "0")
    IND = await get_indicators_with_altseason(m.chat.id)
    await m.answer(f"Автопрогноз для <b>{h(IND[ind]['title'])}</b> <b>выключен</b>. ⛔️")
    await reschedule_user_job(m.chat.id, ind)

@dp.message(F.text == BTN_TEST)
async def settings_test(m: Message):
    ind = (await get_state(_k_curr_ind(m.chat.id))) or DEFAULT_IND
    await send_indicator_update_for_chat(m.chat.id, ind)

# ==== НОВОЕ: сводка календаря ====
@dp.message(F.text == BTN_CAL_OVERVIEW)
async def settings_calendar_overview(m: Message):
    IND = await get_indicators_with_altseason(m.chat.id)

    preset_items = [(k, IND[k]) for k in PRESET_INDICATORS.keys() if k in IND]
    if ALTSEASON_KEY in IND:
        preset_items.append((ALTSEASON_KEY, IND[ALTSEASON_KEY]))

    custom_items = sorted(
        [(k, v) for k, v in IND.items() if k not in PRESET_INDICATORS and k != ALTSEASON_KEY],
        key=lambda kv: kv[1]["title"].lower()
    )

    lines = ["<b>🗓️ Календарь индикаторов</b>", ""]

    async def fmt_line(key, meta):
        title = h(meta["title"])
        t = await get_state(_k_time(m.chat.id, key))
        d = await get_state(_k_days(m.chat.id, key))
        en = await get_state(_k_enabled(m.chat.id, key)) == "1"
        status = "✅ Вкл" if en else "❌ Выкл"

        if not t and not d:
            return f"   🔹 <b>{title}</b>\n      └ 🕒 <i>не настроен</i>   |   {status}"

        ru_days = _days_to_ru(_normalize_days(d or "")) or "—"
        time_str = h(t or "—")
        return (
            f"   🔹 <b>{title}</b>\n"
            f"      └ 📅 {ru_days}\n"
            f"      └ 🕒 {time_str}\n"
            f"      └ {status}"
        )

    if preset_items:
        lines.append("<b>📊 Базовые индикаторы</b>")
        for k, meta in preset_items:
            lines.append(await fmt_line(k, meta))
            lines.append("")

    if custom_items:
        lines.append("<b>🧩 Кастомные индикаторы</b>")
        for k, meta in custom_items:
            lines.append(await fmt_line(k, meta))
            lines.append("")

    await m.answer("\n".join(lines), disable_web_page_preview=True)

# ==== ALTSEASON меню ====
@dp.message(F.text == BTN_ALTSEASON_MAIN)
async def open_altseason_menu(m: Message):
    await m.answer("📊 Меню альтсезона:", reply_markup=altseason_kb())

@dp.message(F.text == BTN_ALTSEASON_CHECK)
async def altseason_check(m: Message):
    try:
        idx, used_url = fetch_altseason_index()
    except Exception as e:
        await m.answer(f"⚠️ Не удалось получить индекс альтсезона: {h(e)}")
        return

    png, fname = render_altseason_card(int(idx))
    await m.answer_document(BufferedInputFile(png, filename=fname))
    await m.answer(
        format_altseason_status(int(idx)) + f"\n\n<i>Источник</i>: {used_url}",
        disable_web_page_preview=True
    )

    try:
        stats = fetch_altseason_stats()
        await m.answer(format_altseason_stats(stats), disable_web_page_preview=True)
    except Exception as e:
        await m.answer(f"ℹ️ Табличную сводку получить не удалось: {h(e)}")

# ==== кастомы ====
@dp.message(F.text == BTN_IND_ADD)
async def custom_add_start(m: Message):
    await set_state(_k_ui_state(m.chat.id), "await_add_title")
    await m.answer(
        "➕ Добавление кастом-индикатора.\n"
        "1/3. Отправьте <b>название</b> кнопки (например: <i>My CPI</i>)."
    )

@dp.message(F.text == BTN_IND_DEL)
async def custom_del_start(m: Message):
    await set_state(_k_ui_state(m.chat.id), "await_del_pick")
    kb = await indicators_only_kb(m.chat.id, customs_only=True)
    await m.answer("➖ Выберите КАСТОМНЫЙ индикатор для удаления:", reply_markup=kb)

# ==== таблицы / проверки из основного меню investing ====
@dp.message(F.text == BTN_TABLE_TEXT)
async def cmd_table_text(m: Message):
    ind_key = await _get_selected_key(m.chat.id)
    await _send_table_text(m, ind_key)

@dp.message(F.text == BTN_TABLE_PNG)
async def cmd_table_png(m: Message):
    ind_key = await _get_selected_key(m.chat.id)
    await _send_table_png(m, ind_key)

# ==== текстовый роутер ====
@dp.message(F.text.regexp(r".+"))
async def handle_text(m: Message):
    ui = await get_state(_k_ui_state(m.chat.id)) or ""
    txt = (m.text or "").strip()

    if ui == "await_time":
        parsed = _parse_hhmm(txt)
        if not parsed:
            await m.answer("⛔️ Неверный формат. Введите <code>HH:MM</code> по МСК, например <b>15:30</b>.")
            return

        hh, mm = parsed
        ind = (await get_state(_k_curr_ind(m.chat.id))) or DEFAULT_IND

        await set_state(_k_time(m.chat.id, ind), f"{hh:02d}:{mm:02d}")
        await set_state(_k_ui_state(m.chat.id), "")

        IND = await get_indicators_with_altseason(m.chat.id)
        await m.answer(
            f"✅ Время для <b>{h(IND[ind]['title'])}</b> установлено: <code>{hh:02d}:{mm:02d}</code>.",
            reply_markup=settings_kb()
        )

        await reschedule_user_job(m.chat.id, ind)
        return

    # выбор индикатора из обычного меню (без режима настроек)
    if ui == "":
        raw = txt.lstrip("✅ ").lstrip("🧩 ").strip()
        IND = await get_indicators_with_altseason(m.chat.id)
        key = key_by_title_sync(IND, raw)
        if key:
            await set_state(f"last_indicator:{m.chat.id}", key)
            await set_state(_k_curr_ind(m.chat.id), key)
            await m.answer(
                f"Индикатор выбран: <b>{h(IND[key]['title'])}</b>\nВыберите действие ниже:",
                reply_markup=ReplyKeyboardMarkup(
                    resize_keyboard=True,
                    keyboard=[
                        [KeyboardButton(text=BTN_TABLE_PNG), KeyboardButton(text=BTN_TABLE_TEXT)],
                        [KeyboardButton(text=BTN_CHECK)],
                        [KeyboardButton(text=BTN_BACK)],
                    ],
                ),
            )
            return

    if ui == "await_days":
        nd = _normalize_days(txt)
        if not nd:
            await m.answer(
                "⛔️ Неверный формат дней. Примеры: <code>mon-fri</code>, <code>mon-sun</code>, "
                "<code>sat,sun</code>, <code>пн-чт</code>."
            )
            return
        ind = (await get_state(_k_curr_ind(m.chat.id))) or DEFAULT_IND
        await set_state(_k_days(m.chat.id, ind), nd)
        await set_state(_k_ui_state(m.chat.id), "")

        IND2 = await get_indicators_with_altseason(m.chat.id)
        await m.answer(
            f"✅ Дни для <b>{h(IND2[ind]['title'])}</b> установлены: <code>{h(nd)}</code>.",
            reply_markup=settings_kb()
        )
        await reschedule_user_job(m.chat.id, ind)
        return

    if ui == "await_pick_ind":
        IND = await get_indicators_with_altseason(m.chat.id)
        key = key_by_title_sync(IND, txt)
        if not key:
            kb = await indicators_only_kb(m.chat.id, customs_only=False)
            await m.answer("Не узнал индикатор. Выберите из списка ниже.", reply_markup=kb)
            return
        await set_state(_k_curr_ind(m.chat.id), key)
        await set_state(f"last_indicator:{m.chat.id}", key)
        if await get_state(_k_time(m.chat.id, key)) is None:
            await set_state(_k_time(m.chat.id, key), "15:30")
        if await get_state(_k_days(m.chat.id, key)) is None:
            await set_state(_k_days(m.chat.id, key), "mon-sun")
        await set_state(_k_ui_state(m.chat.id), "")
        kb = settings_kb()
        await m.answer(f"Индикатор для автопрогноза: <b>{h(IND[key]['title'])}</b> ✅", reply_markup=kb)
        if (await get_state(_k_enabled(m.chat.id, key))) == "1":
            await reschedule_user_job(m.chat.id, key)
        return

    if ui == "await_add_title":
        if len(txt) < 2 or len(txt) > 64:
            await m.answer("Название 2–64 символа. Попробуйте ещё раз.")
            return
        await set_state(_k_tmp(m.chat.id, "add_title"), txt)
        await set_state(_k_ui_state(m.chat.id), "await_add_url")
        await m.answer("2/3. Отправьте URL страницы индикатора на Investing.com.")
        return

    if ui == "await_add_url":
        if not (txt.startswith("http://") or txt.startswith("https://")):
            await m.answer("Это не URL. Пришлите полную ссылку, начинающуюся с http(s)://")
            return
        await set_state(_k_tmp(m.chat.id, "add_url"), txt)
        await set_state(_k_ui_state(m.chat.id), "await_add_rule")
        await m.answer("3/3. Отправьте тип правила: <b>LT</b>, <b>GT</b> или <b>FOMC</b>.\n\n" + h(rules_hints()))
        return

    if ui == "await_add_rule":
        rule = txt.upper()
        if rule not in {"LT", "GT", "FOMC"}:
            await m.answer("Неверно. Допустимо: LT / GT / FOMC.\n" + h(rules_hints()))
            return
        title = await get_state(_k_tmp(m.chat.id, "add_title"))
        url = await get_state(_k_tmp(m.chat.id, "add_url"))
        key = await add_custom_indicator(m.chat.id, title, url, rule)
        await set_state(_k_curr_ind(m.chat.id), key)
        await set_state(f"last_indicator:{m.chat.id}", key)
        await set_state(_k_tmp(m.chat.id, "add_title"), "")
        await set_state(_k_tmp(m.chat.id, "add_url"), "")
        await set_state(_k_ui_state(m.chat.id), "")
        kb = await indicators_kb(m.chat.id)
        await m.answer(
            f"✅ Кастом-индикатор <b>{h(title)}</b> добавлен и выбран. Включите ⏱️, если нужно расписание.",
            reply_markup=kb
        )
        return

    if ui == "await_del_pick":
        customs = await list_custom_indicators(m.chat.id)
        custom_map = {c["title"]: c["key"] for c in customs}
        if txt not in custom_map:
            kb = await indicators_only_kb(m.chat.id, customs_only=True)
            await m.answer("Выберите кастом из списка ниже.", reply_markup=kb)
            return

        key_to_delete = custom_map[txt]
        deleted = await delete_custom_indicator_by_title(m.chat.id, txt)

        await set_state(_k_ui_state(m.chat.id), "")
        kb = await indicators_kb(m.chat.id)

        if deleted:
            curr = await get_state(_k_curr_ind(m.chat.id))
            last = await get_state(f"last_indicator:{m.chat.id}")
            if curr == key_to_delete:
                await set_state(_k_curr_ind(m.chat.id), DEFAULT_IND)
            if last == key_to_delete:
                await set_state(f"last_indicator:{m.chat.id}", DEFAULT_IND)
            await m.answer(f"🗑️ Удалён кастом-индикатор <b>{h(txt)}</b>.", reply_markup=kb)
        else:
            await m.answer("Нечего удалять.", reply_markup=kb)
        return

# ==== background / startup ====
async def poll_loop():
    log.info("Start background monitoring: %s–%s sec", MIN_SEC, MAX_SEC)
    last_state = None
    while True:
        try:
            IND = await get_indicators(0)
            meta = IND["JOBLESS_CLAIMS"]
            rows, err = fetch_rows_generic(meta["url"])
            if err:
                log.warning("poll error: %s", err)
            else:
                top = rows[0] if rows else None
                if top:
                    a = top.get("actual_val"); f = top.get("forecast_val")
                    state_now = ("num" if a is not None else "wait", a, f, top.get("date",""))
                    if last_state != state_now:
                        if last_state and last_state[0] == "wait" and state_now[0] == "num":
                            subs = await list_subs()
                            text = _signal_from_rows(rows, "JOBLESS_CLAIMS", IND)
                            for chat_id in subs:
                                try:
                                    await bot.send_message(chat_id, text, disable_web_page_preview=True)
                                except Exception as e:
                                    log.warning("send to %s failed: %s", chat_id, e)
                        last_state = state_now
        except Exception as e:
            log.warning("poll_loop unexpected: %s", e)
        await asyncio.sleep(random.randint(MIN_SEC, MAX_SEC))

async def daily_1530_job():
    keys = ["JOBLESS_CLAIMS", "CPI", "NFP"]
    lines = ["⏰ 15:30 МСК. Обновления:"]
    IND = dict(PRESET_INDICATORS)
    for k in keys:
        meta = IND[k]
        rows, err = fetch_rows_generic(meta["url"])
        if err:
            lines.append(f"{meta['title']}: ошибка получения")
        else:
            lines.append(_signal_from_rows(rows, k, IND))
    text = "\n".join(lines)
    subs = await list_subs()
    for chat_id in subs:
        try:
            await bot.send_message(chat_id, text, disable_web_page_preview=True)
        except Exception as e:
            log.warning("send daily to %s failed: %s", chat_id, e)

async def on_start():
    global scheduler
    await init_db()
    scheduler = AsyncIOScheduler(timezone=tz)
    scheduler.add_job(daily_1530_job, CronTrigger(hour=15, minute=30, timezone=tz))

    subs = await list_subs()
    for chat_id in subs:
        curr = await get_state(_k_curr_ind(chat_id))
        last = await get_state(f"last_indicator:{chat_id}")
        if (not curr) and last:
            await set_state(_k_curr_ind(chat_id), last)
        if await get_state(_k_curr_ind(chat_id)) is None:
            await set_state(_k_curr_ind(chat_id), DEFAULT_IND)

        IND = await get_indicators_with_altseason(chat_id)
        for ind in IND.keys():
            if (await get_state(_k_enabled(chat_id, ind))) == "1":
                await reschedule_user_job(chat_id, ind)

    scheduler.start()
    asyncio.create_task(poll_loop())

def _single_instance_lock(port: int = 54678):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("127.0.0.1", port))
    except OSError:
        raise RuntimeError("⚠️ Похоже, бот уже запущен на этой машине (порт-лок).")
    s.listen(1)
    return s

async def main():
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        log.warning("delete_webhook failed: %s", e)
    try:
        await on_start()
        await dp.start_polling(bot)
    finally:
        try:
            await bot.session.close()
        except Exception:
            pass

if __name__ == "__main__":
    _lock = _single_instance_lock(54678)
    asyncio.run(main())
