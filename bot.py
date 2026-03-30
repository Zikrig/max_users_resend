"""
MAX-бот:
1. Пользовательское меню: /start, bot_started, иной текст в ЛС — каналы и делегаты.
2. Мастер-меню: только /admin — глобальная реклама, кнопки под постами, /stats, мастера из конфига (.env).
3. Копия поста в чат комментариев, кнопки к посту; мут и правка постов в меню канала (трекер до 3 суток).
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import struct
import sys
import time
from datetime import datetime, time as dtime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx
import replies as rep
import uvicorn
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

API_BASE = "https://platform-api.max.ru"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
TRACKED_POST_TTL_SEC = 3 * 24 * 3600
POSTS_PAGE_SIZE = 10


def _menu_prepend(base: str, prepend: Optional[str]) -> str:
    """Строка обратной связи (ошибка/успех) над телом меню после inline-действия."""
    if prepend and prepend.strip():
        return f"{prepend.strip()}\n\n{base}"
    return base


class MoscowFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, MOSCOW_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S")


handler = logging.StreamHandler()
handler.setFormatter(MoscowFormatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.addHandler(handler)
root_logger.setLevel(logging.INFO)
logger = logging.getLogger("MaxBot")

WEBHOOK_SECRET_RE = re.compile(r"^[a-zA-Z0-9_-]{5,256}$")


def normalize_webhook_url(url: str) -> Tuple[str, str]:
    """Возвращает (полный HTTPS URL для MAX, path для Starlette). Если путь не указан — /webhook."""
    raw = url.strip()
    p = urlparse(raw)
    if p.scheme != "https":
        raise ValueError("WEBHOOK_URL должен начинаться с https://")
    path = (p.path or "").strip()
    if not path or path == "/":
        netloc = p.netloc
        if not netloc:
            raise ValueError("WEBHOOK_URL: не указан хост")
        full = f"https://{netloc}/webhook"
        return full, "/webhook"
    if not path.startswith("/"):
        path = "/" + path
    full = f"https://{p.netloc}{path}"
    return full, path


def parse_listen_host_port(raw: str) -> Tuple[str, int]:
    s = raw.strip()
    if ":" not in s:
        return s, 8000
    host, _, port_s = s.rpartition(":")
    if not host:
        host = "0.0.0.0"
    try:
        port = int(port_s)
    except ValueError:
        raise ValueError(f"Некорректный порт в WEBHOOK_LISTEN: {raw!r}") from None
    return host, port


def _mid_from_message_dict(msg: Any) -> Optional[str]:
    if not isinstance(msg, dict):
        return None
    body = msg.get("body")
    if isinstance(body, dict):
        raw = body.get("mid")
        if raw is not None:
            return str(raw)
    return None


def message_mid_from_callback_update(update: Dict[str, Any]) -> Optional[str]:
    """Идентификатор сообщения бота с нажатой inline-кнопкой (редактирование через PUT /messages)."""
    cb = update.get("callback")
    if isinstance(cb, dict):
        for key in ("message", "message_update"):
            mid = _mid_from_message_dict(cb.get(key))
            if mid:
                return mid
    return _mid_from_message_dict(update.get("message"))


async def max_subscribe_webhook(client: httpx.AsyncClient, url: str, secret: Optional[str]) -> None:
    payload: Dict[str, Any] = {
        "url": url,
        "update_types": ["message_created", "message_callback", "bot_started"],
    }
    if secret:
        payload["secret"] = secret
    r = await client.post("/subscriptions", json=payload)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and data.get("success") is False:
        raise RuntimeError(data.get("message") or "POST /subscriptions вернул success=false")


async def max_unsubscribe_webhook(client: httpx.AsyncClient, url: str) -> None:
    r = await client.delete("/subscriptions", params={"url": url})
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and data.get("success") is False:
        logger.warning("DELETE /subscriptions: %s", data.get("message"))


async def max_list_subscriptions(client: httpx.AsyncClient) -> Any:
    """GET /subscriptions — список текущих подписок на вебхук (док: dev.max.ru GET subscriptions)."""
    r = await client.get("/subscriptions")
    r.raise_for_status()
    return r.json()


class AdminState(Enum):
    NONE = "none"
    AWAITING_AD_TEXT = "awaiting_ad_text"
    AWAITING_AD_LINK = "awaiting_ad_link"
    AWAITING_CHAT_TEXT = "awaiting_chat_text"
    AWAITING_COMMENTS_MESSAGE_BUTTON_TEXT = "awaiting_comments_message_button_text"
    AWAITING_BIND_CHANNEL_INVITE = "awaiting_bind_channel_invite"
    AWAITING_BIND_COMMENTS_INVITE = "awaiting_bind_comments_invite"
    AWAITING_NEW_ADMIN = "awaiting_new_admin"
    AWAITING_NEW_PROMOTED_MASTER = "awaiting_new_promoted_master"
    AWAITING_MUTE_RANGE = "awaiting_mute_range"
    AWAITING_POST_EDIT_TEXT = "awaiting_post_edit_text"
    AWAITING_POST_EDIT_IMAGE = "awaiting_post_edit_image"


def parse_admin_ids(raw: Any) -> List[int]:
    if raw is None:
        return []
    if isinstance(raw, list):
        values = raw
    else:
        values = str(raw).split(",")
    result: List[int] = []
    for item in values:
        part = str(item).strip()
        if not part:
            continue
        try:
            result.append(int(part))
        except ValueError:
            logger.warning("Skipping invalid admin id: %s", part)
    return sorted(set(result))


def get_short_id(seq: Any) -> str:
    try:
        if not seq:
            return ""
        packed = struct.pack(">Q", int(seq))
        return base64.urlsafe_b64encode(packed).decode().rstrip("=")
    except Exception:
        return ""


def parse_hhmm(value: str) -> dtime:
    return datetime.strptime(value, "%H:%M").time()


def normalize_quiet_hours(value: str) -> str:
    raw = value.strip()
    if "-" not in raw:
        raise ValueError("Формат должен быть HH:MM-HH:MM")
    start_raw, end_raw = [part.strip() for part in raw.split("-", 1)]
    start_time = parse_hhmm(start_raw)
    end_time = parse_hhmm(end_raw)
    return f"{start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}"


def encode_post_ref(channel_id: int, message_id: str) -> str:
    raw = json.dumps({"c": channel_id, "m": str(message_id)}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_post_ref(ref: str) -> Optional[tuple[int, str]]:
    try:
        pad = "=" * (-len(ref) % 4)
        data = json.loads(base64.urlsafe_b64decode(ref + pad).decode())
        return int(data["c"]), str(data["m"])
    except Exception:
        return None


def normalize_text_format(raw: Any) -> Optional[str]:
    """Значение для NewMessageBody.format: markdown | html. Остальное → None (plain)."""
    if raw is None:
        return None
    if isinstance(raw, dict):
        inner = raw.get("type") or raw.get("name") or raw.get("value") or raw.get("format")
        if inner is not None:
            return normalize_text_format(inner)
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return None
    s = str(raw).strip().lower().replace("-", "_")
    if s in ("markdown", "md", "mrkdwn"):
        return "markdown"
    if s in ("html", "text_html"):
        return "html"
    if s in ("plain", "plaintext", "text", "none", "plain_text"):
        return None
    return None


def text_suggests_markdown(s: str) -> bool:
    """
    Клиент MAX часто не присылает body.format у исходящего сообщения админа.
    Если в трекере нет text_format, но в тексте есть типичные маркеры markdown — считаем format=markdown.
    """
    if not s or not isinstance(s, str):
        return False
    if "**" in s or "`" in s or "~~" in s:
        return True
    if "](" in s and "[" in s:
        return True
    if re.search(r"(?<!\*)\*[^*\n]+\*(?!\*)", s):
        return True
    return False


def extract_text_format_from_body(body: Dict[str, Any]) -> Optional[str]:
    """Ищем enum format (markdown/html). Поле markup здесь не смотрим — это массив span-ов, см. copy_markup_from_body."""
    for key in (
        "format",
        "text_format",
        "textFormat",
        "parse_mode",
        "parseMode",
        "text_style",
        "textStyle",
    ):
        if key not in body:
            continue
        fmt = normalize_text_format(body.get(key))
        if fmt:
            return fmt
    return None


# Типы из webhook канала → обёртки по гайду MAX (format=markdown)
_MARKUP_TYPE_TO_MARKDOWN: Dict[str, tuple[str, str]] = {
    "emphasized": ("*", "*"),
    "emphasis": ("*", "*"),
    "em": ("*", "*"),
    "italic": ("*", "*"),
    "strong": ("**", "**"),
    "bold": ("**", "**"),
    "strikethrough": ("~~", "~~"),
    "underline": ("++", "++"),
    "code": ("`", "`"),
    "monospace": ("`", "`"),
}


def _markdown_pair_for_span_type(typ: str) -> Optional[tuple[str, str]]:
    return _MARKUP_TYPE_TO_MARKDOWN.get((typ or "").strip().lower())


def _span_url_from_dict(s: Dict[str, Any]) -> Optional[str]:
    """Ссылка в span: разные клиенты/API могут называть поле по-разному."""
    for key in ("url", "link", "href", "uri", "target"):
        v = s.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


# Как может называться цитата в span.type (MAX/клиенты часто не «blockquote»)
_BLOCKQUOTE_SPAN_TYPES: frozenset[str] = frozenset(
    {
        "blockquote",
        "quote",
        "block_quote",
        "blockquote_open",
        "citation",
        "cite",
        "quotation",
        "text_quote",
        "textquote",
        "collapsed_quote",
        "expandable_blockquote",
        "expandable_quote",
        "quote_open",
        "quote_close",
    }
)


def _span_looks_like_blockquote(s: Dict[str, Any], typ: str) -> bool:
    if typ in _BLOCKQUOTE_SPAN_TYPES:
        return True
    for key in ("style", "block_type", "blockType", "kind", "variant"):
        v = s.get(key)
        if isinstance(v, str) and v.strip().lower() in ("quote", "blockquote", "citation", "cite"):
            return True
    if s.get("blockquote") is True or s.get("is_quote") is True or s.get("isQuote") is True:
        return True
    return False


def _heading_level_from_type_and_dict(typ: str, s: Dict[str, Any]) -> Optional[int]:
    """Уровень заголовка 1..6 из type (heading_2) или полей level/depth/size."""
    raw = (typ or "").strip().lower()
    m = re.match(r"^heading[_\s-]?(\d)$", raw)
    if m:
        return max(1, min(6, int(m.group(1))))
    if len(raw) == 2 and raw[0] == "h" and raw[1].isdigit():
        return max(1, min(6, int(raw[1])))
    for key in ("level", "depth", "size", "header_level"):
        v = s.get(key)
        if v is None:
            continue
        try:
            return max(1, min(6, int(v)))
        except (TypeError, ValueError):
            continue
    if raw in ("heading", "header", "title"):
        return 1
    return None


def _span_to_markdown_replacement(out: str, start: int, end: int, s: Dict[str, Any]) -> Optional[str]:
    """
    Замена для out[start:end] в markdown. None — не умеем (фрагмент пропускается).
    Сначала ссылки и блоки (часто с полем url / level без совпадения по type в словаре).
    """
    chunk = out[start:end]
    typ = str(s.get("type", "") or "").strip().lower()

    url = _span_url_from_dict(s)
    if url:
        return f"[{chunk}]({url})"

    if _span_looks_like_blockquote(s, typ):
        return "\n".join("> " + line for line in chunk.split("\n"))

    hl = _heading_level_from_type_and_dict(typ, s)
    if hl is not None:
        return ("#" * hl) + " " + chunk.lstrip()

    pair = _markdown_pair_for_span_type(typ)
    if pair:
        left, right = pair
        return left + chunk + right

    if typ in ("link", "text_link", "hyperlink", "url"):
        logger.warning(
            "span type=%r без url в объекте span, ключи=%s — пропуск",
            typ,
            sorted(s.keys()),
        )
        return None

    return None


def apply_markup_spans_as_markdown(text: str, markup: List[Dict[str, Any]]) -> str:
    """
    Исходящие POST/PUT: в ответах API разметка часто не отображается только по полю markup.
    Конвертируем spans в format=markdown и символы в строке text — так клиент показывает выделение
    (в т.ч. при запросе с клавиатурой). Ссылки (поле url), заголовки, цитаты, базовые стили.
    """
    if not text or not markup:
        return text
    spans: List[tuple[int, int, Dict[str, Any]]] = []
    for s in markup:
        if not isinstance(s, dict):
            continue
        try:
            start = int(s["from"])
            ln = int(s["length"])
        except (KeyError, TypeError, ValueError):
            continue
        if ln <= 0 or start < 0:
            continue
        if start >= len(text):
            logger.warning(
                "apply_markup_spans_as_markdown: span from=%s за пределами текста len=%s (возможны индексы в UTF-16)",
                start,
                len(text),
            )
            continue
        spans.append((start, min(ln, len(text) - start), s))
    if not spans:
        return text
    spans.sort(key=lambda x: (-x[0], x[1]))
    out = text
    for start, ln, s in spans:
        end = start + ln
        if end > len(out):
            end = len(out)
        replacement = _span_to_markdown_replacement(out, start, end, s)
        if replacement is None:
            logger.warning(
                "apply_markup_spans_as_markdown: неизвестный span type=%r keys=%s — пропуск",
                s.get("type"),
                sorted(s.keys()),
            )
            continue
        out = out[:start] + replacement + out[end:]
    return out


def normalize_outbound_message(
    text: str,
    text_format: Optional[str],
    markup: Optional[List[Dict[str, Any]]],
) -> tuple[str, Optional[str], Optional[List[Dict[str, Any]]]]:
    """
    Готовим text/format/markup для API.
    Явный format=markdown|html — как есть.
    Иначе span-markup конвертируем в markdown-строку + format=markdown (иначе клиент часто
    игнорирует только массив markup, в том числе при запросе с клавиатурой).
    Если конвертация не меняет текст — fallback: исходный text + массив markup.
    При format=markdown и непустом markup одновременно отображение часто ломается: встраиваем spans в строку (*…*) и не шлём markup.
    """
    if text_format in ("markdown", "html"):
        if text_format == "markdown" and markup:
            md = apply_markup_spans_as_markdown(text, markup)
            if md != text:
                return md, "markdown", None
        return text, text_format, markup if markup else None
    if markup:
        md = apply_markup_spans_as_markdown(text, markup)
        if md != text:
            return md, "markdown", None
        logger.warning(
            "normalize_outbound_message: span→markdown не изменил текст (len=%s); "
            "в запросе передаём массив markup без format (fallback)",
            len(text or ""),
        )
        return text, None, markup
    return text, None, None


def copy_markup_from_body(body: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """
    Реальный MAX: в канале приходит body.markup = [{\"from\", \"length\", \"type\"}, ...], без format.
    Храним копию; при исходящих запросах normalize_outbound_message переводит spans в markdown + format.
    """
    raw = body.get("markup")
    if not isinstance(raw, list) or not raw:
        return None
    out: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            logger.warning("copy_markup_from_body: пропуск элемента не-dict: %r", item)
            continue
        out.append(dict(item))
    return out or None


def markup_from_admin_body(body: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """
    None — нет span-разметки в сообщении (ключ markup отсутствует, null или пустой []).
    Клиент MAX часто шлёт markup: [] даже без явного «сброса» — это не то же самое, что
    явный сброс на сервере; не [] возвращаем, чтобы дальше брать трекер / не слать markup: [] в PUT.
    Non-empty list — реальные spans от клиента.
    """
    if "markup" not in body:
        return None
    raw = body.get("markup")
    if raw is None or raw == []:
        return None
    if not isinstance(raw, list):
        return None
    out: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            return None
        out.append(dict(item))
    return out or None


def message_body_text_and_format(body: Dict[str, Any]) -> tuple[str, Optional[str]]:
    """Текст и format (markdown/html) из body входящего сообщения."""
    text = body.get("text") or ""
    if not isinstance(text, str):
        text = str(text)
    return text, extract_text_format_from_body(body)


def message_body_text_format_markup(
    body: Dict[str, Any],
) -> tuple[str, Optional[str], Optional[List[Dict[str, Any]]]]:
    """Текст, format и массив markup (entity spans), как в webhook канала."""
    text, tf = message_body_text_and_format(body)
    return text, tf, copy_markup_from_body(body)


def tracked_markup_for_api(tr: Optional[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    """Сохранённый в трекере markup для PUT/POST. None — поля не было (ключ не передаём)."""
    if not tr or "markup" not in tr:
        return None
    m = tr.get("markup")
    if not isinstance(m, list):
        return None
    return [dict(x) for x in m if isinstance(x, dict)]


def format_debug_snapshot(body: Dict[str, Any]) -> Dict[str, Any]:
    """Поля body, которые могут относиться к разметке (для логов)."""
    out: Dict[str, Any] = {}
    if not isinstance(body, dict):
        return out
    for k in sorted(body.keys()):
        low = k.lower()
        if any(
            s in low
            for s in ("format", "markup", "parse", "entity", "element", "block", "style", "markdown", "html")
        ):
            v = body[k]
            if k == "text" and isinstance(v, str) and len(v) > 180:
                v = v[:180] + "…"
            out[k] = v
    return out


def deep_truncate_strings(obj: Any, max_len: int = 6000) -> Any:
    """Для логов: обрезает длинные строки (body.text, payload вложений)."""
    if isinstance(obj, str):
        if len(obj) > max_len:
            return obj[:max_len] + f"... (truncated, total len={len(obj)})"
        return obj
    if isinstance(obj, dict):
        return {k: deep_truncate_strings(v, max_len) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_truncate_strings(x, max_len) for x in obj]
    if isinstance(obj, (bytes, bytearray)):
        return f"<bytes len={len(obj)}>"
    return obj


def json_for_log(obj: Any, *, max_str: int = 6000) -> str:
    """JSON для логов с безопасной сериализацией и обрезкой длинных строк."""
    try:
        return json.dumps(deep_truncate_strings(obj, max_str), ensure_ascii=False, default=str, indent=2)
    except Exception as e:
        return f"<json_for_log failed: {e}>"


def log_channel_post_body_from_api(msg: Dict[str, Any], channel_id: int) -> None:
    """
    Фактический webhook message.body по посту канала.
    Документацию MAX нельзя считать исчерпывающей — смотрим, что реально приходит.
    """
    body = msg.get("body")
    if not isinstance(body, dict):
        logger.info(
            "MAX channel_post chat_id=%s: body отсутствует или не dict (type=%s)",
            channel_id,
            type(body).__name__,
        )
        return
    snap = format_debug_snapshot(body)
    logger.info(
        "MAX channel_post chat_id=%s: ключи message.body=%s | снимок полей про разметку: %s",
        channel_id,
        sorted(body.keys()),
        json.dumps(snap, ensure_ascii=False, default=str),
    )
    raw_mu = body.get("markup")
    empty_markup = not isinstance(raw_mu, list) or len(raw_mu) == 0
    if empty_markup:
        logger.info(
            "MAX channel_post chat_id=%s: полный message.body (markup пустой — весь text и остальные поля):\n%s",
            channel_id,
            json_for_log(body),
        )


def clean_media_attachments_from_body(
    attachments: List[Dict[str, Any]], *, strip_ref_fields: bool = True
) -> List[Dict[str, Any]]:
    """Медиа и прочие вложения без клавиатуры.

    strip_ref_fields: для постов из канала убираем часть полей payload (как при пересылке в чат).
    Для нового фото из лички админа (смена картинки) передаём strip_ref_fields=False, чтобы не терять url/token.
    """
    drop = ("callback_id", "url", "size", "width", "height", "duration")
    clean: List[Dict[str, Any]] = []
    for item in attachments:
        if item.get("type") == "inline_keyboard":
            continue
        payload = item.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        if strip_ref_fields:
            safe_payload = {key: value for key, value in payload.items() if key not in drop}
        else:
            safe_payload = {key: value for key, value in payload.items() if key != "callback_id"}
        clean.append({"type": item.get("type"), "payload": safe_payload})
    return clean


def is_time_in_range(now_value: dtime, range_value: str) -> bool:
    if not range_value:
        return False
    start_raw, end_raw = range_value.split("-", 1)
    start_time = parse_hhmm(start_raw)
    end_time = parse_hhmm(end_raw)
    if start_time <= end_time:
        return start_time <= now_value <= end_time
    return now_value >= start_time or now_value <= end_time


def normalize_max_url(url: str) -> str:
    u = (url or "").strip()
    if not u.startswith("http"):
        u = "https://" + u
    return u.rstrip("/")


def extract_join_token(url: str) -> str:
    m = re.search(r"/join/([^/?#]+)", url, re.IGNORECASE)
    return m.group(1) if m else ""


def links_match(a: str, b: str) -> bool:
    return normalize_max_url(a).lower() == normalize_max_url(b).lower()


def try_parse_chat_id_from_text(text: str) -> Optional[int]:
    raw = text.strip()
    if re.fullmatch(r"-?\d+", raw):
        try:
            return int(raw)
        except ValueError:
            return None
    m = re.search(r"/c/(-?\d+)", raw)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def membership_summary(m: dict) -> str:
    try:
        return json.dumps(
            {
                "is_owner": m.get("is_owner"),
                "is_admin": m.get("is_admin"),
                "permissions": m.get("permissions"),
            },
            ensure_ascii=False,
        )
    except Exception:
        return str(m)


def check_channel_admin_permissions(m: dict) -> tuple[bool, str]:
    """Редактирование постов в канале: владелец; или право редактировать (в API бывает edit или edit_message)."""
    if m.get("is_owner"):
        return True, "owner"
    perms = set(m.get("permissions") or [])
    # Документация: edit_message / post_edit_delete_message; на практике приходит короткое «edit»
    if perms & {"edit", "edit_message", "post_edit_delete_message"}:
        return True, "explicit_edit_permission"
    if m.get("is_admin") and not perms:
        return True, "admin_no_explicit_permissions"
    if m.get("is_admin"):
        return False, f"admin_but_no_edit_flags permissions={sorted(perms)}"
    return False, f"no_owner_or_edit is_admin={m.get('is_admin')} permissions={sorted(perms)}"


def check_comments_chat_admin_permissions(m: dict) -> tuple[bool, str]:
    """Чат комментариев: владелец; или право писать (write)."""
    if m.get("is_owner"):
        return True, "owner"
    perms = set(m.get("permissions") or [])
    if "write" in perms:
        return True, "write"
    if m.get("is_admin") and not perms:
        return True, "admin_no_explicit_permissions"
    if m.get("is_admin"):
        return False, f"admin_but_no_write permissions={sorted(perms)}"
    return False, f"no_owner_or_write is_admin={m.get('is_admin')} permissions={sorted(perms)}"


class Config:
    def __init__(self, filename: str = "config.json"):
        self.filename = filename
        self.ad_text = os.environ.get("AD_TEXT", "Реклама")
        self.ad_url = os.environ.get("AD_URL", "https://max.ru")
        self.comments_chat_text = os.environ.get("COMMENTS_CHAT_TEXT", "Чат комментариев")
        self.comments_message_button_text = os.environ.get(
            "COMMENTS_MESSAGE_BUTTON_TEXT", "💬 Перейти к сообщению"
        )
        self.root_admin_ids = parse_admin_ids(os.environ.get("ADMIN_USER_IDS", ""))
        self.delegate_parent: Dict[int, int] = {}
        self.promoted_master_ids: List[int] = []
        self.ad_click_total: int = 0
        self.channel_bindings: List[Dict[str, Any]] = []
        self.tracked_posts: List[Dict[str, Any]] = []

        self.load()
        self.promoted_master_ids = sorted(
            set(x for x in self.promoted_master_ids if x not in self.root_admin_ids)
        )
        logger.info(
            "Config initialized: bindings=%s root_masters=%s promoted_masters=%s delegates=%s",
            len(self.channel_bindings),
            self.root_admin_ids,
            self.promoted_master_ids,
            len(self.delegate_parent),
        )

    def account_root_for(self, user_id: int) -> int:
        cur = user_id
        seen: set[int] = set()
        while cur in self.delegate_parent:
            if cur in seen:
                return user_id
            seen.add(cur)
            cur = self.delegate_parent[cur]
        return cur

    def load(self) -> None:
        if not os.path.exists(self.filename):
            return
        try:
            with open(self.filename, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.ad_text = data.get("ad_text", self.ad_text)
            self.ad_url = data.get("ad_url", self.ad_url)
            self.comments_chat_text = data.get("comments_chat_text", self.comments_chat_text)
            self.comments_message_button_text = data.get(
                "comments_message_button_text", self.comments_message_button_text
            )
            self.ad_click_total = int(data.get("ad_click_total", self.ad_click_total))
            raw_pm = data.get("promoted_master_ids")
            self.promoted_master_ids = parse_admin_ids(raw_pm) if raw_pm is not None else []
            raw_dp = data.get("delegate_parent") or {}
            self.delegate_parent = {}
            if isinstance(raw_dp, dict):
                for k, v in raw_dp.items():
                    try:
                        self.delegate_parent[int(k)] = int(v)
                    except (TypeError, ValueError):
                        logger.warning("Skipping invalid delegate_parent entry: %s -> %s", k, v)
            legacy_admins = [
                x for x in parse_admin_ids(data.get("admin_ids")) if x not in self.root_admin_ids
            ]
            if not self.delegate_parent and legacy_admins:
                root = legacy_admins[0]
                for a in legacy_admins[1:]:
                    self.delegate_parent[a] = root
            migration_root: Optional[int] = None
            if legacy_admins:
                migration_root = legacy_admins[0]
            elif self.root_admin_ids and len(self.root_admin_ids) == 1:
                migration_root = self.root_admin_ids[0]
            self.channel_bindings = self._load_channel_bindings(data, migration_root=migration_root)
            self.tracked_posts = self._load_tracked_posts(data)
            logger.info("Config loaded from file.")
        except Exception as e:
            logger.error("Failed to load config file: %s", e)

    def _load_channel_bindings(
        self, data: Dict[str, Any], migration_root: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        migrate_mute = bool(data.get("chat_mute_enabled", False))
        migrate_qh = str(data.get("quiet_hours", "")).strip()

        def one_binding(
            item: Dict[str, Any],
            *,
            cid: int,
            ccid: int,
            link: str,
            mute_en: bool,
            qh: str,
            default_root: Optional[int],
        ) -> Optional[Dict[str, Any]]:
            ar = item.get("account_root_id")
            cb = item.get("created_by")
            try:
                if ar is not None:
                    account_root_id = int(ar)
                elif default_root is not None:
                    account_root_id = default_root
                else:
                    return None
                if cb is not None:
                    created_by = int(cb)
                elif default_root is not None:
                    created_by = default_root
                else:
                    return None
            except (TypeError, ValueError):
                return None
            return {
                "channel_id": cid,
                "comments_chat_id": ccid,
                "comments_chat_link": link,
                "channel_title": (item.get("channel_title") or "") or None,
                "comments_chat_title": (item.get("comments_chat_title") or "") or None,
                "chat_mute_enabled": mute_en,
                "quiet_hours": qh,
                "account_root_id": account_root_id,
                "created_by": created_by,
            }

        raw = data.get("channel_bindings")
        if isinstance(raw, list) and raw:
            out: List[Dict[str, Any]] = []
            for item in raw:
                if not isinstance(item, dict):
                    continue
                try:
                    cid = int(item["channel_id"])
                    ccid = int(item["comments_chat_id"])
                    link = str(item.get("comments_chat_link", "")).strip()
                except (KeyError, TypeError, ValueError):
                    continue
                if not link:
                    continue
                if "chat_mute_enabled" in item:
                    mute_en = bool(item["chat_mute_enabled"])
                else:
                    mute_en = migrate_mute
                if "quiet_hours" in item:
                    qh = str(item.get("quiet_hours") or "").strip()
                else:
                    qh = migrate_qh
                b = one_binding(
                    item,
                    cid=cid,
                    ccid=ccid,
                    link=link,
                    mute_en=mute_en,
                    qh=qh,
                    default_root=migration_root,
                )
                if b:
                    out.append(b)
            return out
        legacy_ch = data.get("channel_id")
        legacy_cc = data.get("comments_chat_id")
        legacy_link = data.get("comments_chat_link", "")
        try:
            if legacy_ch is not None and legacy_cc is not None and legacy_link:
                cid = int(legacy_ch)
                ccid = int(legacy_cc)
                link = str(legacy_link).strip()
                if link:
                    item = {
                        "channel_id": cid,
                        "comments_chat_id": ccid,
                        "comments_chat_link": link,
                        "channel_title": None,
                        "comments_chat_title": None,
                        "chat_mute_enabled": migrate_mute,
                        "quiet_hours": migrate_qh,
                    }
                    b = one_binding(
                        item,
                        cid=cid,
                        ccid=ccid,
                        link=link,
                        mute_en=migrate_mute,
                        qh=migrate_qh,
                        default_root=migration_root,
                    )
                    return [b] if b else []
        except (TypeError, ValueError):
            pass
        return []

    def binding_for_channel(self, channel_id: int) -> Optional[Dict[str, Any]]:
        for b in self.channel_bindings:
            if int(b["channel_id"]) == int(channel_id):
                return b
        return None

    def binding_for_comments_chat(self, comments_chat_id: int) -> Optional[Dict[str, Any]]:
        for b in self.channel_bindings:
            if int(b["comments_chat_id"]) == int(comments_chat_id):
                return b
        return None

    def all_channel_ids(self) -> set[int]:
        return {int(b["channel_id"]) for b in self.channel_bindings}

    def all_comments_chat_ids(self) -> set[int]:
        return {int(b["comments_chat_id"]) for b in self.channel_bindings}

    def _load_tracked_posts(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        raw = data.get("tracked_posts")
        if not isinstance(raw, list):
            return []
        out: List[Dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                ma = item.get("media_attachments")
                if not isinstance(ma, list):
                    ma = []
                tf = normalize_text_format(item.get("text_format"))
                if tf is None:
                    tf = normalize_text_format(item.get("format"))
                row: Dict[str, Any] = {
                    "channel_id": int(item["channel_id"]),
                    "message_id": str(item["message_id"]),
                    "text": str(item.get("text", "")),
                    "message_link": str(item.get("message_link", "")),
                    "saved_at": float(item.get("saved_at", 0)),
                    "chat_message_id": str(item.get("chat_message_id", "") or ""),
                    "media_attachments": ma,
                }
                if tf in ("markdown", "html"):
                    row["text_format"] = tf
                mk = item.get("markup")
                if isinstance(mk, list):
                    row["markup"] = [dict(x) for x in mk if isinstance(x, dict)]
                out.append(row)
            except (KeyError, TypeError, ValueError):
                continue
        self._prune_tracked_posts_list(out)
        return out

    def _prune_tracked_posts_list(self, posts: List[Dict[str, Any]]) -> None:
        cutoff = time.time() - TRACKED_POST_TTL_SEC
        posts[:] = [p for p in posts if float(p.get("saved_at", 0)) >= cutoff]

    def prune_tracked_posts(self) -> None:
        self._prune_tracked_posts_list(self.tracked_posts)

    def register_tracked_post(
        self,
        channel_id: int,
        message_id: str,
        text: str,
        message_link: str,
        *,
        chat_message_id: str | None = None,
        media_attachments: Optional[List[Dict[str, Any]]] = None,
        text_format: Optional[str] = None,
        markup: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self.prune_tracked_posts()
        now = time.time()
        mid = str(message_id)

        def apply_text_format(p: Dict[str, Any]) -> None:
            if text_format is None:
                return
            if text_format in ("markdown", "html"):
                p["text_format"] = text_format
            else:
                p.pop("text_format", None)

        def apply_markup_field(p: Dict[str, Any]) -> None:
            if markup is None:
                return
            if markup:
                p["markup"] = [dict(x) for x in markup]
            else:
                p.pop("markup", None)

        for p in self.tracked_posts:
            if int(p["channel_id"]) == int(channel_id) and str(p["message_id"]) == mid:
                p["text"] = text
                p["message_link"] = message_link
                p["saved_at"] = now
                if chat_message_id is not None:
                    p["chat_message_id"] = chat_message_id
                if media_attachments is not None:
                    p["media_attachments"] = media_attachments
                apply_text_format(p)
                apply_markup_field(p)
                return
        entry: Dict[str, Any] = {
            "channel_id": int(channel_id),
            "message_id": mid,
            "text": text,
            "message_link": message_link,
            "saved_at": now,
            "chat_message_id": chat_message_id if chat_message_id is not None else "",
            "media_attachments": list(media_attachments) if media_attachments is not None else [],
        }
        apply_text_format(entry)
        apply_markup_field(entry)
        self.tracked_posts.append(entry)

    def find_tracked_post(self, channel_id: int, message_id: str) -> Optional[Dict[str, Any]]:
        mid = str(message_id)
        for p in self.tracked_posts:
            if int(p["channel_id"]) == int(channel_id) and str(p["message_id"]) == mid:
                return p
        return None

    def sorted_tracked_posts(self) -> List[Dict[str, Any]]:
        self.prune_tracked_posts()
        return sorted(self.tracked_posts, key=lambda p: float(p.get("saved_at", 0)), reverse=True)

    def sorted_tracked_posts_for_channel(self, channel_id: int) -> List[Dict[str, Any]]:
        self.prune_tracked_posts()
        sub = [p for p in self.tracked_posts if int(p["channel_id"]) == int(channel_id)]
        return sorted(sub, key=lambda p: float(p.get("saved_at", 0)), reverse=True)

    def remove_tracked_posts_for_channel(self, channel_id: int) -> None:
        self.tracked_posts = [p for p in self.tracked_posts if int(p["channel_id"]) != int(channel_id)]

    def save(self) -> None:
        self.prune_tracked_posts()
        try:
            with open(self.filename, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "ad_text": self.ad_text,
                        "ad_url": self.ad_url,
                        "comments_chat_text": self.comments_chat_text,
                        "comments_message_button_text": self.comments_message_button_text,
                        "ad_click_total": self.ad_click_total,
                        "promoted_master_ids": self.promoted_master_ids,
                        "delegate_parent": {str(k): v for k, v in sorted(self.delegate_parent.items())},
                        "channel_bindings": self.channel_bindings,
                        "tracked_posts": self.tracked_posts,
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            logger.info("Config saved to file.")
        except Exception as e:
            logger.error("Failed to save config file: %s", e)


class MaxBot:
    def __init__(self, token: str, config: Config):
        self.token = token
        self.config = config
        self.headers = {"Authorization": self.token}
        self.client = httpx.AsyncClient(base_url=API_BASE, headers=self.headers, timeout=60.0)
        self.bot_id: int | None = None
        self.admin_states: Dict[int, AdminState] = {}
        self.channel_bind_draft: Dict[int, Dict[str, Any]] = {}
        self.mute_range_channel_id: Dict[int, int] = {}
        self.post_edit_ref: Dict[int, Dict[str, Any]] = {}

    def is_master_env(self, user_id: int | None) -> bool:
        return user_id is not None and user_id in self.config.root_admin_ids

    def is_master(self, user_id: int | None) -> bool:
        return self.is_master_env(user_id) or (
            user_id is not None and user_id in self.config.promoted_master_ids
        )

    def participant_user_ids(self) -> set[int]:
        s: set[int] = set()
        s.update(self.config.root_admin_ids)
        s.update(self.config.promoted_master_ids)
        for k, v in self.config.delegate_parent.items():
            s.add(int(k))
            s.add(int(v))
        for b in self.config.channel_bindings:
            s.add(int(b["account_root_id"]))
            s.add(int(b["created_by"]))
        return s

    def can_use_user_menu(self, user_id: int | None) -> bool:
        if user_id is None:
            return False
        if self.is_master(user_id):
            return True
        return user_id in self.participant_user_ids()

    def can_access_channel(self, user_id: int | None, b: Dict[str, Any]) -> bool:
        if user_id is None:
            return False
        if self.is_master(user_id):
            return True
        ar = int(b["account_root_id"])
        cb = int(b["created_by"])
        if self.config.account_root_for(user_id) != ar:
            return False
        if user_id == ar:
            return True
        parent = self.config.delegate_parent.get(user_id)
        return cb == user_id or (parent is not None and cb == parent)

    def bindings_visible(self, user_id: int) -> List[Dict[str, Any]]:
        return [b for b in self.config.channel_bindings if self.can_access_channel(user_id, b)]

    def direct_delegate_ids(self, user_id: int) -> List[int]:
        return sorted(k for k, v in self.config.delegate_parent.items() if v == user_id)

    def channel_label_for_user(self, user_id: int, b: Dict[str, Any]) -> str:
        base = str(b.get("channel_title") or rep.channel_title_fallback(int(b["channel_id"])))[:60]
        if int(b["created_by"]) != int(user_id):
            return rep.DELEGATED_CHANNEL_EMOJI + base
        return base

    def binding_in_quiet_hours(self, binding: Dict[str, Any]) -> bool:
        if not binding.get("chat_mute_enabled"):
            return False
        qh = str(binding.get("quiet_hours") or "").strip()
        if not qh:
            return False
        return is_time_in_range(datetime.now(MOSCOW_TZ).time(), qh)

    async def get_me(self) -> None:
        try:
            r = await self.client.get("/me")
            r.raise_for_status()
            data = r.json()
            self.bot_id = data.get("user_id")
            logger.info("Logged in as bot ID %s (@%s)", self.bot_id, data.get("username"))
        except Exception as e:
            logger.critical("Failed to get bot info: %s", e)
            sys.exit(1)

    async def send_message(
        self,
        chat_id: int,
        text: str,
        attachments: Optional[List[Dict]] = None,
        text_format: Optional[str] = None,
        markup: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[Dict]:
        try:
            text, text_format, markup = normalize_outbound_message(text, text_format, markup)
            payload: Dict[str, Any] = {"text": text}
            if text_format in ("markdown", "html"):
                payload["format"] = text_format
            if markup:
                payload["markup"] = markup
            if attachments:
                payload["attachments"] = attachments
            params = {"user_id": chat_id} if chat_id > 0 else {"chat_id": chat_id}
            r = await self.client.post("/messages", params=params, json=payload)
            r.raise_for_status()
            return r.json().get("message")
        except Exception as e:
            logger.error("Failed to send message to %s: %s", chat_id, e)
            return None

    async def show_menu_or_edit(
        self,
        user_id: int,
        text: str,
        attachments: Optional[List[Dict]] = None,
        *,
        text_format: Optional[str] = None,
        markup: Optional[List[Dict[str, Any]]] = None,
        edit_message_id: Optional[str] = None,
    ) -> None:
        """Меню с клавиатурой: при нажатии кнопки правит то же сообщение, иначе отправляет новое."""
        if edit_message_id:
            ok = await self.edit_message(
                edit_message_id,
                text,
                attachments,
                text_format=text_format,
                markup=markup,
            )
            if ok:
                return
            logger.warning("Не удалось отредактировать сообщение mid=%s, отправляем новое", edit_message_id)
        await self.send_message(user_id, text, attachments, text_format=text_format, markup=markup)

    async def replace_with_prompt_or_send(
        self,
        user_id: int,
        text: str,
        *,
        edit_message_id: Optional[str] = None,
        attachments: Optional[List[Dict]] = None,
    ) -> None:
        """Запрос ввода: правит сообщение; по умолчанию без клавиатуры, можно передать (например «Назад»)."""
        att = attachments if attachments is not None else []
        if edit_message_id:
            ok = await self.edit_message(edit_message_id, text, attachments=att)
            if ok:
                return
            logger.warning("Не удалось заменить сообщение на запрос ввода mid=%s", edit_message_id)
        await self.send_message(user_id, text, att)

    async def edit_message(
        self,
        message_id: str,
        text: str,
        attachments: Optional[List[Dict]] = None,
        text_format: Optional[str] = None,
        markup: Optional[List[Dict[str, Any]]] = None,
        *,
        log_api_response_as: Optional[str] = None,
        log_outbound_payload: bool = False,
    ) -> bool:
        try:
            text, text_format, markup = normalize_outbound_message(text, text_format, markup)
            payload: Dict[str, Any] = {"text": text}
            if text_format in ("markdown", "html"):
                payload["format"] = text_format
            # Пустой массив в JSON часто ломает отображение markdown; не шлём ключ, если spans нет.
            if markup:
                payload["markup"] = markup
            if attachments is not None:
                payload["attachments"] = attachments
            if log_outbound_payload:
                logger.info(
                    "edit_message PUT /messages outbound message_id=%s context=%s\n%s",
                    message_id,
                    log_api_response_as or "",
                    json_for_log({"params": {"message_id": message_id}, "json": payload}),
                )
            r = await self.client.put("/messages", params={"message_id": message_id}, json=payload)
            if r.status_code != 200:
                logger.error("Edit failed: %s %s", r.status_code, r.text)
            r.raise_for_status()
            if log_api_response_as:
                try:
                    j = r.json()
                    m = j.get("message") if isinstance(j, dict) else None
                    b = m.get("body") if isinstance(m, dict) else None
                    if isinstance(b, dict):
                        logger.info(
                            "%s: ответ PUT /messages — ключи message.body=%s | разметка: %s",
                            log_api_response_as,
                            sorted(b.keys()),
                            json.dumps(format_debug_snapshot(b), ensure_ascii=False, default=str),
                        )
                    else:
                        logger.info(
                            "%s: ответ PUT /messages без message.body (keys=%s)",
                            log_api_response_as,
                            sorted(j.keys()) if isinstance(j, dict) else type(j).__name__,
                        )
                except Exception as ex:
                    logger.info("%s: не разобрали JSON ответа edit: %s", log_api_response_as, ex)
            return True
        except Exception as e:
            logger.error("Failed to edit message %s: %s", message_id, e)
            return False

    async def delete_message(self, message_id: str) -> bool:
        try:
            r = await self.client.delete("/messages", params={"message_id": message_id})
            if r.status_code != 200:
                logger.error("Delete failed: %s %s", r.status_code, r.text)
            r.raise_for_status()
            return True
        except Exception as e:
            logger.error("Failed to delete message %s: %s", message_id, e)
            return False

    def get_standard_buttons(self, include_ad: bool = True) -> List[List[Dict]]:
        """Реклама: callback для учёта кликов; открытие ссылки — в личном ответе бота после нажатия."""
        buttons: List[List[Dict]] = []
        if include_ad and self.config.ad_text and self.config.ad_url:
            buttons.append(
                [{"type": "callback", "text": self.config.ad_text, "payload": "mst_ad_click"}]
            )
        return buttons

    def build_channel_keyboard_attachment(self, binding: Dict[str, Any], message_link: str) -> List[Dict]:
        comments_invite_link = str(binding.get("comments_chat_link", "")).strip()
        channel_buttons_row: List[Dict] = []
        if comments_invite_link:
            channel_buttons_row.append(
                {"type": "link", "text": self.config.comments_chat_text, "url": comments_invite_link}
            )
        if message_link and (self.config.comments_message_button_text or "").strip():
            channel_buttons_row.append(
                {
                    "type": "link",
                    "text": self.config.comments_message_button_text.strip(),
                    "url": message_link,
                }
            )
        if not channel_buttons_row:
            return []
        return [{"type": "inline_keyboard", "payload": {"buttons": [channel_buttons_row]}}]

    def build_comments_chat_copy_attachments(self, media_attachments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        copy_attachments = list(media_attachments)
        ad_buttons = self.get_standard_buttons(include_ad=True)
        if ad_buttons:
            copy_attachments.append({"type": "inline_keyboard", "payload": {"buttons": ad_buttons}})
        return copy_attachments

    async def apply_channel_post_text_edit(
        self,
        channel_id: int,
        message_id: str,
        new_text: str,
        message_link: str,
        media_attachments: Optional[List[Dict[str, Any]]] = None,
        chat_message_id: Optional[str] = None,
        text_format: Optional[str] = None,
        markup: Optional[List[Dict[str, Any]]] = None,
        *,
        log_outbound_payload: bool = False,
    ) -> bool:
        binding = self.config.binding_for_channel(channel_id)
        if not binding:
            return False
        media = list(media_attachments or [])
        kb = self.build_channel_keyboard_attachment(binding, message_link)
        channel_attachments = media + kb
        if not await self.edit_message(
            str(message_id),
            new_text,
            channel_attachments,
            text_format=text_format,
            markup=markup,
            log_api_response_as="apply_channel_post_text_edit channel",
            log_outbound_payload=log_outbound_payload,
        ):
            return False
        chat_mid = (chat_message_id or "").strip()
        if not chat_mid:
            return True
        chat_copy = self.build_comments_chat_copy_attachments(media)
        if not await self.edit_message(
            chat_mid,
            new_text,
            chat_copy,
            text_format=text_format,
            markup=markup,
            log_api_response_as="apply_channel_post_text_edit comments_chat",
            log_outbound_payload=log_outbound_payload,
        ):
            logger.error("Не удалось обновить копию поста в чате (message_id=%s)", chat_mid)
            return False
        return True

    async def fetch_chat_by_id(self, chat_id: int) -> Optional[Dict[str, Any]]:
        try:
            r = await self.client.get(f"/chats/{chat_id}")
            if r.status_code != 200:
                logger.warning("GET /chats/%s -> %s %s", chat_id, r.status_code, r.text)
                return None
            data = r.json()
            return data if isinstance(data, dict) else None
        except Exception as e:
            logger.error("fetch_chat_by_id %s: %s", chat_id, e)
            return None

    async def find_chat_by_invite_url(self, url: str) -> tuple[Optional[int], Optional[Dict[str, Any]], str]:
        norm = normalize_max_url(url)
        token = extract_join_token(norm)
        marker: int | None = None
        while True:
            params: Dict[str, Any] = {"count": 100}
            if marker is not None:
                params["marker"] = marker
            try:
                r = await self.client.get("/chats", params=params)
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                return None, None, rep.chat_list_fetch_error(str(e))
            chats = data.get("chats") or []
            if not isinstance(chats, list):
                chats = []
            for c in chats:
                if not isinstance(c, dict):
                    continue
                cid = c.get("chat_id")
                clink = (c.get("link") or "").strip()
                if clink and links_match(clink, norm):
                    return int(cid) if cid is not None else None, c, ""
                if token and clink:
                    if extract_join_token(clink) == token:
                        return int(cid) if cid is not None else None, c, ""
            next_m = data.get("marker")
            if next_m is None or not chats:
                break
            try:
                marker = int(next_m)
            except (TypeError, ValueError):
                break
        return None, None, rep.CHAT_NOT_IN_BOT_LIST

    async def resolve_chat_from_input(self, text: str) -> tuple[Optional[int], Optional[Dict[str, Any]], str]:
        raw = text.strip()
        if not raw:
            return None, None, rep.EMPTY_INPUT
        maybe_id = try_parse_chat_id_from_text(raw)
        if maybe_id is not None:
            info = await self.fetch_chat_by_id(maybe_id)
            if info:
                return maybe_id, info, ""
            return None, None, rep.chat_not_found_by_id(maybe_id)
        if not raw.startswith("http"):
            raw = "https://" + raw.lstrip("/")
        return await self.find_chat_by_invite_url(raw)

    async def get_bot_membership(self, chat_id: int) -> tuple[Optional[Dict[str, Any]], str]:
        try:
            r = await self.client.get(f"/chats/{chat_id}/members/me")
            if r.status_code != 200:
                body = (r.text or "").strip()
                snippet = (body[:800] + "…") if len(body) > 800 else body
                logger.warning(
                    "GET /chats/%s/members/me failed: HTTP %s body=%r",
                    chat_id,
                    r.status_code,
                    snippet,
                )
                return None, rep.membership_http_error(r.status_code)
            data = r.json()
            if not isinstance(data, dict):
                logger.warning("GET /chats/%s/members/me: unexpected JSON type %s", chat_id, type(data))
                return None, rep.MEMBERSHIP_BAD_RESPONSE
            logger.info(
                "members/me chat_id=%s membership=%s",
                chat_id,
                membership_summary(data),
            )
            return data, ""
        except Exception as e:
            logger.exception("GET /chats/%s/members/me exception", chat_id)
            return None, str(e) or repr(e)

    async def handle_update(self, update: Dict[str, Any]) -> None:
        update_type = update.get("update_type")
        if update_type == "message_created":
            await self.on_message_created(update.get("message", {}))
        elif update_type == "message_callback":
            await self.on_callback(update)
        elif update_type == "bot_started":
            logger.info("Webhook bot_started: %s", update)
            await self.on_bot_started(update)
        else:
            logger.warning("Неизвестный update_type=%r (добавьте обработку или расширьте подписку)", update_type)

    async def on_message_created(self, msg: Dict[str, Any]) -> None:
        sender = msg.get("sender", {})
        sender_id = int(sender.get("user_id")) if sender.get("user_id") else None
        recipient = msg.get("recipient", {})
        raw_chat_id = recipient.get("chat_id") or recipient.get("chat", {}).get("chat_id") or recipient.get("user_id")
        chat_id = int(raw_chat_id) if raw_chat_id is not None else None
        message_id = msg.get("body", {}).get("mid")
        text_preview = ((msg.get("body") or {}).get("text") or "")[:120]

        logger.info(
            "message_created sender_id=%s chat_id=%s mid=%s text=%r",
            sender_id,
            chat_id,
            message_id,
            text_preview,
        )
        if chat_id is None:
            logger.warning("message_created: chat_id=None (проверьте формат recipient в MAX), recipient=%s", recipient)

        if sender_id and self.bot_id and sender_id == self.bot_id:
            return

        if chat_id is not None and chat_id in self.config.all_channel_ids():
            await self.process_channel_post(msg)
            return

        if chat_id is not None and chat_id in self.config.all_comments_chat_ids() and message_id:
            bind = self.config.binding_for_comments_chat(chat_id)
            if bind and self.binding_in_quiet_hours(bind):
                logger.info(
                    "Deleting message %s due to quiet hours %s (channel_id=%s)",
                    message_id,
                    bind.get("quiet_hours"),
                    bind.get("channel_id"),
                )
                await self.delete_message(message_id)
                return

        public_ids = self.config.all_channel_ids() | self.config.all_comments_chat_ids()
        is_private_dm = chat_id is None or chat_id not in public_ids
        if is_private_dm and sender_id is not None:
            await self.process_private_message(msg)
            return

        if sender_id is not None and (chat_id is None or chat_id not in public_ids):
            await self.send_message(sender_id, rep.GUEST_NO_ACCESS)

    async def process_channel_post(self, msg: Dict[str, Any]) -> None:
        message_id = msg.get("body", {}).get("mid")
        recipient = msg.get("recipient", {})
        raw_chat_id = recipient.get("chat_id") or recipient.get("chat", {}).get("chat_id")
        channel_id = int(raw_chat_id) if raw_chat_id is not None else None
        binding = self.config.binding_for_channel(channel_id) if channel_id is not None else None
        if not binding:
            logger.warning("No channel binding for chat_id=%s", channel_id)
            return

        msg_body = msg.get("body") or {}
        if not isinstance(msg_body, dict):
            msg_body = {}
        log_channel_post_body_from_api(msg, channel_id)
        text, text_fmt, markup_spans = message_body_text_format_markup(msg_body)
        canon_text, canon_tf, canon_mk = normalize_outbound_message(text, text_fmt, markup_spans)
        logger.info(
            "MAX channel_post chat_id=%s: входной text_format=%r, spans=%s → канонический format=%r (для трекера и API)",
            channel_id,
            text_fmt,
            len(markup_spans) if markup_spans else 0,
            canon_tf,
        )
        attachments = msg_body.get("attachments") or []

        clean_attachments = clean_media_attachments_from_body(attachments)

        comments_chat_id = int(binding["comments_chat_id"])

        chat_message_id = ""
        short_message_id = ""
        if comments_chat_id:
            copy_attachments = self.build_comments_chat_copy_attachments(clean_attachments)

            forwarded = await self.send_message(
                comments_chat_id,
                canon_text,
                copy_attachments,
                text_format=canon_tf,
                markup=canon_mk,
            )
            if forwarded:
                body = forwarded.get("body", {})
                if isinstance(body, dict):
                    logger.info(
                        "MAX channel_post: ответ POST /messages (копия в чат) ключи body=%s | разметка: %s",
                        sorted(body.keys()),
                        json.dumps(format_debug_snapshot(body), ensure_ascii=False, default=str),
                    )
                short_message_id = get_short_id(body.get("seq")) or str(body.get("mid")).split(".")[-1]
                raw_chat_mid = body.get("mid")
                if raw_chat_mid is not None:
                    chat_message_id = str(raw_chat_mid)

        message_link = ""
        if comments_chat_id and short_message_id:
            message_link = f"https://max.ru/c/{comments_chat_id}/{short_message_id}"

        kb_att = self.build_channel_keyboard_attachment(binding, message_link)
        channel_attachments = list(clean_attachments)
        channel_attachments.extend(kb_att)

        if message_id:
            ok = await self.edit_message(
                message_id,
                canon_text,
                channel_attachments,
                text_format=canon_tf,
                markup=canon_mk,
                log_api_response_as=f"process_channel_post channel mid={message_id}",
            )
            if ok and kb_att:
                self.config.register_tracked_post(
                    int(channel_id),
                    str(message_id),
                    canon_text,
                    message_link,
                    chat_message_id=chat_message_id,
                    media_attachments=clean_attachments,
                    text_format=canon_tf if canon_tf is not None else "",
                    markup=canon_mk if canon_mk is not None else [],
                )
                self.config.save()

    def _reset_fsm(self, sender_id: int) -> None:
        self.admin_states[sender_id] = AdminState.NONE
        self.channel_bind_draft.pop(sender_id, None)
        self.mute_range_channel_id.pop(sender_id, None)
        self.post_edit_ref.pop(sender_id, None)

    async def on_bot_started(self, update: Dict[str, Any]) -> None:
        user_id: int | None = None
        for key in ("user", "chat", "sender", "recipient"):
            block = update.get(key)
            if isinstance(block, dict):
                uid = block.get("user_id")
                if uid is not None:
                    try:
                        user_id = int(uid)
                        break
                    except (TypeError, ValueError):
                        pass
        if user_id is None:
            logger.info("bot_started: user_id не найден в update")
            return
        if self.can_use_user_menu(user_id):
            await self.send_user_menu(user_id)

    async def process_private_message(
        self, msg: Dict[str, Any], *, treat_as_start: bool = False
    ) -> None:
        raw_sid = msg.get("sender", {}).get("user_id")
        if raw_sid is None:
            return
        sender_id = int(raw_sid)
        text = (msg.get("body", {}).get("text") or "").strip()
        tl = text.lower()

        def _cmd(name: str) -> bool:
            return tl == name or tl.startswith(name + "@")

        if not self.can_use_user_menu(sender_id):
            await self.send_message(sender_id, rep.ACCESS_DENIED)
            return

        if _cmd("/admin"):
            if not self.is_master(sender_id):
                await self.send_message(sender_id, rep.MASTER_ONLY_ADMIN_CMD)
                return
            self._reset_fsm(sender_id)
            await self.send_master_menu(sender_id)
            return

        if _cmd("/stats"):
            if not self.is_master(sender_id):
                await self.send_message(sender_id, rep.MASTER_ONLY_STATS_CMD)
                return
            await self.send_message(sender_id, rep.stats_line_callback_total(self.config.ad_click_total))
            return

        if _cmd("/start") or treat_as_start:
            self._reset_fsm(sender_id)
            await self.send_user_menu(sender_id)
            return

        state = self.admin_states.get(sender_id, AdminState.NONE)

        master_states = (
            AdminState.AWAITING_AD_TEXT,
            AdminState.AWAITING_AD_LINK,
            AdminState.AWAITING_CHAT_TEXT,
            AdminState.AWAITING_COMMENTS_MESSAGE_BUTTON_TEXT,
            AdminState.AWAITING_NEW_PROMOTED_MASTER,
        )
        if state in master_states and not self.is_master(sender_id):
            self._reset_fsm(sender_id)
            await self.send_message(sender_id, rep.SESSION_MASTER_RESET)
            return

        if state == AdminState.AWAITING_NEW_PROMOTED_MASTER and not self.is_master_env(sender_id):
            self.admin_states[sender_id] = AdminState.NONE
            await self.send_message(sender_id, rep.ONLY_ENV_ADDS_MASTERS)
            await self.send_master_menu(sender_id)
            return

        if state == AdminState.AWAITING_AD_TEXT:
            self.config.ad_text = text
            self.config.save()
            self.admin_states[sender_id] = AdminState.NONE
            await self.send_message(sender_id, rep.AD_TEXT_CHANGED.format(text=text))
            await self.send_master_ad_submenu(sender_id)
            return

        if state == AdminState.AWAITING_AD_LINK:
            if not text.startswith("http://") and not text.startswith("https://"):
                await self.send_message(sender_id, rep.AD_LINK_INVALID)
                return
            self.config.ad_url = text
            self.config.save()
            self.admin_states[sender_id] = AdminState.NONE
            await self.send_message(sender_id, rep.AD_LINK_CHANGED)
            await self.send_master_ad_submenu(sender_id)
            return

        if state == AdminState.AWAITING_CHAT_TEXT:
            self.config.comments_chat_text = text
            self.config.save()
            self.admin_states[sender_id] = AdminState.NONE
            await self.send_message(sender_id, rep.CHAT_BTN_TEXT_CHANGED.format(text=text))
            await self.send_master_btns_submenu(sender_id)
            return

        if state == AdminState.AWAITING_NEW_PROMOTED_MASTER:
            try:
                mid = int(text)
            except ValueError:
                await self.send_message(sender_id, rep.PROMOTED_NEED_NUMERIC)
                return
            if mid in self.config.root_admin_ids:
                await self.send_message(sender_id, rep.PROMOTED_ALREADY_ENV)
                await self.send_master_masters_submenu(sender_id)
                return
            if mid in self.config.promoted_master_ids:
                await self.send_message(sender_id, rep.PROMOTED_ALREADY_LIST)
                await self.send_master_masters_submenu(sender_id)
                return
            self.config.promoted_master_ids = sorted(set(self.config.promoted_master_ids + [mid]))
            self.config.save()
            self.admin_states[sender_id] = AdminState.NONE
            await self.send_message(sender_id, rep.PROMOTED_ADDED.format(mid=mid))
            await self.send_master_masters_submenu(sender_id)
            return

        if state == AdminState.AWAITING_BIND_CHANNEL_INVITE:
            cid, info, err = await self.resolve_chat_from_input(text)
            if err or cid is None:
                await self.send_message(sender_id, err or rep.ERR_RESOLVE_CHANNEL_DEFAULT)
                return
            mem, merr = await self.get_bot_membership(cid)
            if merr or not mem:
                await self.send_message(sender_id, merr or rep.ERR_BOT_MEMBERSHIP_DEFAULT)
                return
            ok_ch, reason_ch = check_channel_admin_permissions(mem)
            if not ok_ch:
                logger.warning(
                    "Канал chat_id=%s: проверка прав не пройдена (%s) raw=%s",
                    cid,
                    reason_ch,
                    membership_summary(mem),
                )
                await self.send_message(sender_id, rep.CHANNEL_NEED_BOT_ADMIN_EDIT)
                return
            logger.info("Канал chat_id=%s: проверка прав OK (%s)", cid, reason_ch)
            for b in self.config.channel_bindings:
                if int(b["channel_id"]) == cid:
                    await self.send_message(sender_id, rep.CHANNEL_ALREADY_BOUND)
                    self.admin_states[sender_id] = AdminState.NONE
                    self.channel_bind_draft.pop(sender_id, None)
                    await self.send_channels_submenu(sender_id)
                    return
            title = (info or {}).get("title") if info else None
            self.channel_bind_draft[sender_id] = {
                "channel_id": cid,
                "channel_title": title,
            }
            self.admin_states[sender_id] = AdminState.AWAITING_BIND_COMMENTS_INVITE
            await self.send_message(sender_id, rep.CHANNEL_STEP_COMMENTS)
            return

        if state == AdminState.AWAITING_BIND_COMMENTS_INVITE:
            draft = self.channel_bind_draft.get(sender_id)
            if not draft or "channel_id" not in draft:
                self.admin_states[sender_id] = AdminState.NONE
                await self.send_message(sender_id, rep.BIND_SESSION_RESET)
                await self.send_channels_submenu(sender_id)
                return
            ccid, cinfo, err = await self.resolve_chat_from_input(text)
            if err or ccid is None:
                await self.send_message(sender_id, err or rep.ERR_RESOLVE_CHAT_DEFAULT)
                return
            if ccid == int(draft["channel_id"]):
                await self.send_message(sender_id, rep.COMMENTS_SAME_AS_CHANNEL)
                return
            mem, merr = await self.get_bot_membership(ccid)
            if merr or not mem:
                await self.send_message(sender_id, merr or rep.ERR_BOT_MEMBERSHIP_DEFAULT)
                return
            ok_cc, reason_cc = check_comments_chat_admin_permissions(mem)
            if not ok_cc:
                logger.warning(
                    "Чат комментариев chat_id=%s: проверка прав не пройдена (%s) raw=%s",
                    ccid,
                    reason_cc,
                    membership_summary(mem),
                )
                await self.send_message(sender_id, rep.COMMENTS_NEED_BOT_ADMIN)
                return
            logger.info("Чат комментариев chat_id=%s: проверка прав OK (%s)", ccid, reason_cc)
            t = text.strip()
            if try_parse_chat_id_from_text(t) is not None:
                invite_url = ((cinfo or {}).get("link") or "").strip()
            else:
                invite_url = normalize_max_url(t if t.startswith("http") else "https://" + t.lstrip("/"))
            if not invite_url:
                await self.send_message(sender_id, rep.INVITE_SAVE_FAILED)
                return
            ch_title = (cinfo or {}).get("title") if cinfo else None
            ar = self.config.account_root_for(sender_id)
            new_binding = {
                "channel_id": int(draft["channel_id"]),
                "comments_chat_id": ccid,
                "comments_chat_link": invite_url,
                "channel_title": draft.get("channel_title") or None,
                "comments_chat_title": ch_title or None,
                "chat_mute_enabled": False,
                "quiet_hours": "",
                "account_root_id": ar,
                "created_by": sender_id,
            }
            self.config.channel_bindings.append(new_binding)
            self.config.save()
            self.channel_bind_draft.pop(sender_id, None)
            self.admin_states[sender_id] = AdminState.NONE
            await self.send_message(sender_id, rep.CHANNEL_PAIR_CONNECTED)
            await self.send_channels_submenu(sender_id)
            return

        if state == AdminState.AWAITING_COMMENTS_MESSAGE_BUTTON_TEXT:
            self.config.comments_message_button_text = text
            self.config.save()
            self.admin_states[sender_id] = AdminState.NONE
            await self.send_message(sender_id, rep.MSG_BTN_TEXT_CHANGED.format(text=text))
            await self.send_master_btns_submenu(sender_id)
            return

        if state == AdminState.AWAITING_NEW_ADMIN:
            try:
                new_admin_id = int(text)
            except ValueError:
                await self.send_message(sender_id, rep.DELEGATE_NEED_NUMERIC)
                return
            if self.is_master(new_admin_id):
                await self.send_message(sender_id, rep.DELEGATE_NO_MASTER)
                return
            if new_admin_id in self.config.delegate_parent:
                await self.send_message(sender_id, rep.DELEGATE_ALREADY_IN_TREE)
                return
            if any(int(b["account_root_id"]) == new_admin_id for b in self.config.channel_bindings):
                await self.send_message(sender_id, rep.DELEGATE_HAS_OWN_ACCOUNT)
                return
            self.config.delegate_parent[new_admin_id] = sender_id
            self.config.save()
            self.admin_states[sender_id] = AdminState.NONE
            await self.send_message(sender_id, rep.DELEGATE_ADDED.format(uid=new_admin_id))
            await self.send_delegates_submenu(sender_id)
            return

        if state == AdminState.AWAITING_MUTE_RANGE:
            mcid = self.mute_range_channel_id.get(sender_id)
            if mcid is None:
                self.admin_states[sender_id] = AdminState.NONE
                await self.send_channels_submenu(sender_id)
                return
            bmute = self.config.binding_for_channel(int(mcid))
            if not bmute or not self.can_access_channel(sender_id, bmute):
                self.mute_range_channel_id.pop(sender_id, None)
                self.admin_states[sender_id] = AdminState.NONE
                await self.send_message(sender_id, rep.NO_ACCESS_CHANNEL)
                await self.send_channels_submenu(sender_id)
                return
            try:
                qh = normalize_quiet_hours(text)
            except ValueError:
                await self.send_message(sender_id, rep.MUTE_RANGE_FORMAT)
                return
            updated = False
            for b in self.config.channel_bindings:
                if int(b["channel_id"]) == int(mcid):
                    b["quiet_hours"] = qh
                    updated = True
                    break
            if not updated:
                self.mute_range_channel_id.pop(sender_id, None)
                self.admin_states[sender_id] = AdminState.NONE
                await self.send_message(sender_id, rep.BINDING_NOT_FOUND)
                await self.send_channels_submenu(sender_id)
                return
            self.config.save()
            self.admin_states[sender_id] = AdminState.NONE
            self.mute_range_channel_id.pop(sender_id, None)
            await self.send_message(sender_id, rep.MUTE_RANGE_UPDATED.format(qh=qh))
            await self.send_chat_mute_submenu(sender_id, mcid)
            return

        if state == AdminState.AWAITING_POST_EDIT_IMAGE:
            ctx = self.post_edit_ref.get(sender_id)
            if not ctx:
                self.admin_states[sender_id] = AdminState.NONE
                await self.send_channels_submenu(sender_id)
                return
            cid = int(ctx["channel_id"])
            bpe = self.config.binding_for_channel(cid)
            if not bpe or not self.can_access_channel(sender_id, bpe):
                self.admin_states[sender_id] = AdminState.NONE
                self.post_edit_ref.pop(sender_id, None)
                await self.send_message(sender_id, rep.NO_ACCESS_CHANNEL)
                await self.send_channels_submenu(sender_id)
                return
            mid = str(ctx["message_id"])
            page = int(ctx.get("return_page", 0))
            ml = str(ctx.get("message_link", ""))
            raw_attachments = msg.get("body", {}).get("attachments") or []
            new_media = clean_media_attachments_from_body(raw_attachments, strip_ref_fields=False)
            if not new_media:
                await self.send_message(sender_id, rep.POST_EDIT_NO_IMAGE)
                return
            tr = self.config.find_tracked_post(cid, mid)
            if not tr:
                self.admin_states[sender_id] = AdminState.NONE
                self.post_edit_ref.pop(sender_id, None)
                await self.send_message(sender_id, rep.POST_NOT_FOUND)
                await self.send_posts_list(sender_id, cid, page)
                return
            post_text = str(tr.get("text") or "")
            chat_mid = (tr.get("chat_message_id") or "").strip()
            tf = normalize_text_format(tr.get("text_format"))
            mu = tracked_markup_for_api(tr)
            ok = await self.apply_channel_post_text_edit(
                cid,
                mid,
                post_text,
                ml,
                media_attachments=new_media,
                chat_message_id=chat_mid or None,
                text_format=tf,
                markup=mu,
            )
            if ok:
                self.config.register_tracked_post(cid, mid, post_text, ml, media_attachments=new_media, text_format=tf)
                self.config.save()
                self.admin_states[sender_id] = AdminState.NONE
                self.post_edit_ref.pop(sender_id, None)
                if chat_mid:
                    await self.send_message(sender_id, rep.IMAGE_UPDATED_BOTH)
                else:
                    await self.send_message(sender_id, rep.IMAGE_UPDATED_CHANNEL)
                await self.send_post_detail(sender_id, cid, mid, page)
            else:
                await self.send_message(sender_id, rep.IMAGE_UPDATE_FAILED)
            return

        if state == AdminState.AWAITING_POST_EDIT_TEXT:
            ctx = self.post_edit_ref.get(sender_id)
            if not ctx:
                self.admin_states[sender_id] = AdminState.NONE
                await self.send_channels_submenu(sender_id)
                return
            cid = int(ctx["channel_id"])
            bpt = self.config.binding_for_channel(cid)
            if not bpt or not self.can_access_channel(sender_id, bpt):
                self.admin_states[sender_id] = AdminState.NONE
                self.post_edit_ref.pop(sender_id, None)
                await self.send_message(sender_id, rep.NO_ACCESS_CHANNEL)
                await self.send_channels_submenu(sender_id)
                return
            mid = str(ctx["message_id"])
            page = int(ctx.get("return_page", 0))
            ml = str(ctx.get("message_link", ""))
            tr = self.config.find_tracked_post(cid, mid)
            media = list(tr.get("media_attachments") or []) if tr else []
            chat_mid = (tr.get("chat_message_id") or "").strip() if tr else ""
            admin_body = msg.get("body") or {}
            if not isinstance(admin_body, dict):
                admin_body = {}
            fmt_from_admin = extract_text_format_from_body(admin_body)
            text_format = fmt_from_admin if fmt_from_admin else (
                normalize_text_format(tr.get("text_format")) if tr else None
            )
            if text_format is None and text and text_suggests_markdown(text):
                text_format = "markdown"
            mu_ad = markup_from_admin_body(admin_body)
            prev_plain = (str(tr.get("text") or "").strip()) if tr else ""
            if mu_ad is not None:
                markup_for_edit = mu_ad
            elif text == prev_plain:
                markup_for_edit = tracked_markup_for_api(tr)
            else:
                markup_for_edit = None
            tr_snapshot: Dict[str, Any] = {}
            if tr:
                tr_snapshot = {
                    "text": tr.get("text"),
                    "text_format": tr.get("text_format"),
                    "markup": tr.get("markup"),
                    "message_link": tr.get("message_link"),
                    "chat_message_id": tr.get("chat_message_id"),
                    "media_attachments": tr.get("media_attachments"),
                }
            logger.info(
                "admin post_edit_text INCOMING webhook (полное сообщение updates):\n%s",
                json_for_log(msg),
            )
            logger.info(
                "admin post_edit_text message.body (текст, markup, format и др.):\n%s",
                json_for_log(admin_body),
            )
            logger.info(
                "admin post_edit_text снимок трекера до правки:\n%s",
                json_for_log(tr_snapshot),
            )
            logger.info(
                "admin post_edit_text вычислено: fmt_from_admin=%r text_format=%r mu_ad=%s "
                "markup_for_edit=%s prev_plain_len=%s text_len=%s text==prev_plain=%s channel_id=%s message_id=%s",
                fmt_from_admin,
                text_format,
                "нет spans (ключ пустой/отсутствует)"
                if mu_ad is None
                else f"{len(mu_ad)} span(s)",
                "None"
                if markup_for_edit is None
                else (f"{len(markup_for_edit)} span(s)" if markup_for_edit else "[]"),
                len(prev_plain),
                len(text or ""),
                text == prev_plain,
                cid,
                mid,
            )
            ok = await self.apply_channel_post_text_edit(
                cid,
                mid,
                text,
                ml,
                media_attachments=media,
                chat_message_id=chat_mid or None,
                text_format=text_format,
                markup=markup_for_edit,
                log_outbound_payload=True,
            )
            if ok:
                reg_t, reg_tf, reg_mk = normalize_outbound_message(
                    text, text_format, markup_for_edit
                )
                logger.info(
                    "admin post_edit_text сохранение в трекер (после normalize_outbound_message):\n%s",
                    json_for_log(
                        {
                            "text": reg_t,
                            "text_format": reg_tf,
                            "markup": reg_mk,
                        }
                    ),
                )
                self.config.register_tracked_post(
                    cid,
                    mid,
                    reg_t,
                    ml,
                    media_attachments=media,
                    text_format=reg_tf if reg_tf is not None else "",
                    markup=reg_mk if reg_mk is not None else [],
                )
                self.config.save()
                self.admin_states[sender_id] = AdminState.NONE
                self.post_edit_ref.pop(sender_id, None)
                if chat_mid:
                    await self.send_message(sender_id, rep.TEXT_UPDATED_BOTH)
                else:
                    await self.send_message(sender_id, rep.TEXT_UPDATED_CHANNEL)
                await self.send_post_detail(sender_id, cid, mid, page)
            else:
                await self.send_message(sender_id, rep.POST_EDIT_FAILED)
            return

        self._reset_fsm(sender_id)
        await self.send_user_menu(sender_id)

    async def send_user_menu(
        self,
        user_id: int,
        *,
        edit_message_id: Optional[str] = None,
        prepend: Optional[str] = None,
    ) -> None:
        buttons = [
            [{"type": "callback", "text": rep.BTN_CHANNELS, "payload": "usr_channels"}],
            [{"type": "callback", "text": rep.BTN_DELEGATES, "payload": "usr_delegates"}],
        ]
        text = _menu_prepend(rep.USER_MENU_INTRO, prepend)
        await self.show_menu_or_edit(
            user_id,
            text,
            [{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
            edit_message_id=edit_message_id,
        )

    async def send_master_menu(
        self,
        user_id: int,
        *,
        edit_message_id: Optional[str] = None,
        prepend: Optional[str] = None,
    ) -> None:
        buttons: List[List[Dict]] = [
            [{"type": "callback", "text": rep.BTN_AD_LINK, "payload": "mst_ad"}],
            [{"type": "callback", "text": rep.BTN_POST_BUTTONS, "payload": "mst_btns"}],
            [{"type": "callback", "text": rep.BTN_STATS, "payload": "mst_stats"}],
        ]
        if self.is_master_env(user_id):
            buttons.append([{"type": "callback", "text": rep.BTN_MASTER_ADMINS, "payload": "mst_masters"}])
        text = _menu_prepend(rep.MASTER_MENU_INTRO, prepend)
        await self.show_menu_or_edit(
            user_id,
            text,
            [{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
            edit_message_id=edit_message_id,
        )

    async def send_posts_list(
        self,
        user_id: int,
        channel_id: int,
        page: int,
        *,
        edit_message_id: Optional[str] = None,
        prepend: Optional[str] = None,
    ) -> None:
        b = self.config.binding_for_channel(channel_id)
        if not b or not self.can_access_channel(user_id, b):
            await self.send_channels_submenu(
                user_id,
                edit_message_id=edit_message_id,
                prepend=rep.CHANNEL_NOT_FOUND_OR_NO_ACCESS,
            )
            return
        posts = self.config.sorted_tracked_posts_for_channel(channel_id)
        total = len(posts)
        title = str(b.get("channel_title") or rep.channel_title_fallback(channel_id))[:80]
        if total == 0:
            empty_text = _menu_prepend(rep.POSTS_EMPTY.format(title=title), prepend)
            await self.show_menu_or_edit(
                user_id,
                empty_text,
                [
                    {
                        "type": "inline_keyboard",
                        "payload": {
                            "buttons": [
                                [{"type": "callback", "text": rep.BTN_BACK, "payload": f"usr_ch_detail:{channel_id}"}]
                            ]
                        },
                    }
                ],
                edit_message_id=edit_message_id,
            )
            return
        page_size = POSTS_PAGE_SIZE
        max_page = max(0, (total - 1) // page_size)
        page = max(0, min(page, max_page))
        start = page * page_size
        chunk = posts[start : start + page_size]
        text = _menu_prepend(rep.posts_list_caption(title, page, max_page, total), prepend)
        buttons: List[List[Dict]] = []
        for p in chunk:
            cid = int(p["channel_id"])
            mid = str(p["message_id"])
            raw_txt = p.get("text") or ""
            preview = raw_txt.replace("\n", " ").strip()[:55]
            if len(raw_txt) > 55:
                preview += "…"
            if not preview:
                preview = f"…{mid[-12:]}"
            label = preview[:60]
            ref = encode_post_ref(cid, mid)
            buttons.append(
                [
                    {
                        "type": "callback",
                        "text": label,
                        "payload": f"usr_post_detail:{ref}:{page}:{channel_id}",
                    }
                ]
            )
        nav: List[Dict] = []
        if page > 0:
            nav.append(
                {"type": "callback", "text": rep.NAV_PREV, "payload": f"usr_ch_posts:{channel_id}:{page - 1}"}
            )
        if page < max_page:
            nav.append(
                {"type": "callback", "text": rep.NAV_NEXT, "payload": f"usr_ch_posts:{channel_id}:{page + 1}"}
            )
        if nav:
            buttons.append(nav)
        buttons.append([{"type": "callback", "text": rep.BTN_BACK, "payload": f"usr_ch_detail:{channel_id}"}])
        await self.show_menu_or_edit(
            user_id,
            text,
            [{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
            edit_message_id=edit_message_id,
        )

    async def send_post_detail(
        self,
        user_id: int,
        channel_id: int,
        message_id: str,
        return_page: int,
        *,
        edit_message_id: Optional[str] = None,
    ) -> None:
        b = self.config.binding_for_channel(channel_id)
        if not b or not self.can_access_channel(user_id, b):
            await self.send_channels_submenu(
                user_id,
                edit_message_id=edit_message_id,
                prepend=rep.NO_ACCESS_CHANNEL,
            )
            return
        self.config.prune_tracked_posts()
        p = self.config.find_tracked_post(channel_id, message_id)
        if not p:
            await self.send_posts_list(
                user_id,
                channel_id,
                return_page,
                edit_message_id=edit_message_id,
                prepend=rep.POST_NOT_FOUND,
            )
            return
        body = (p.get("text") or "").strip() or rep.EMPTY_POST_PLACEHOLDER
        ref = encode_post_ref(channel_id, message_id)
        msg_text = f"{rep.POST_DETAIL_PREFIX}{body}"
        buttons = [
            [{"type": "callback", "text": rep.BTN_CHANGE_POST_TEXT, "payload": f"usr_post_edit:{ref}:{return_page}:{channel_id}"}],
            [{"type": "callback", "text": rep.BTN_CHANGE_POST_IMAGE, "payload": f"usr_post_edit_img:{ref}:{return_page}:{channel_id}"}],
            [{"type": "callback", "text": rep.BTN_BACK, "payload": f"usr_ch_posts:{channel_id}:{return_page}"}],
        ]
        kb = {"type": "inline_keyboard", "payload": {"buttons": buttons}}
        media = list(p.get("media_attachments") or [])
        preview_fmt = normalize_text_format(p.get("text_format"))
        preview_mu = tracked_markup_for_api(p)
        if media:
            await self.show_menu_or_edit(
                user_id,
                msg_text,
                media + [kb],
                text_format=preview_fmt,
                markup=preview_mu,
                edit_message_id=edit_message_id,
            )
        else:
            await self.show_menu_or_edit(
                user_id,
                msg_text,
                [kb],
                text_format=preview_fmt,
                markup=preview_mu,
                edit_message_id=edit_message_id,
            )

    async def send_master_ad_submenu(
        self,
        user_id: int,
        *,
        edit_message_id: Optional[str] = None,
        prepend: Optional[str] = None,
    ) -> None:
        buttons = [
            [{"type": "callback", "text": rep.BTN_CHANGE_TEXT, "payload": "mst_set_adtxt"}],
            [{"type": "callback", "text": rep.BTN_CHANGE_LINK, "payload": "mst_set_adurl"}],
            [{"type": "callback", "text": rep.BTN_BACK, "payload": "mst_menu"}],
        ]
        text = _menu_prepend(
            rep.MASTER_AD_SUBMENU.format(ad_text=self.config.ad_text, ad_url=self.config.ad_url),
            prepend,
        )
        await self.show_menu_or_edit(
            user_id,
            text,
            [{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
            edit_message_id=edit_message_id,
        )

    async def send_master_btns_submenu(
        self,
        user_id: int,
        *,
        edit_message_id: Optional[str] = None,
        prepend: Optional[str] = None,
    ) -> None:
        buttons = [
            [{"type": "callback", "text": rep.BTN_CHAT_ENTRY_TEXT, "payload": "mst_set_chtxt"}],
            [{"type": "callback", "text": rep.BTN_MSG_LINK_TEXT, "payload": "mst_set_msgbtn"}],
            [{"type": "callback", "text": rep.BTN_BACK, "payload": "mst_menu"}],
        ]
        text = _menu_prepend(
            rep.MASTER_BTNS_SUBMENU.format(
                chat_btn=self.config.comments_chat_text,
                msg_btn=self.config.comments_message_button_text,
            ),
            prepend,
        )
        await self.show_menu_or_edit(
            user_id,
            text,
            [{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
            edit_message_id=edit_message_id,
        )

    async def send_master_masters_submenu(
        self,
        user_id: int,
        *,
        edit_message_id: Optional[str] = None,
        prepend: Optional[str] = None,
    ) -> None:
        lines = [rep.MASTER_LIST_HEADER]
        pm = self.config.promoted_master_ids
        lines.append(rep.master_list_line(", ".join(str(x) for x in pm)))
        buttons: List[List[Dict]] = [
            [{"type": "callback", "text": rep.BTN_ADD_MASTER, "payload": "mst_add_master"}],
        ]
        for mid in pm:
            buttons.append(
                [{"type": "callback", "text": f"{rep.BTN_DELETE} {mid}", "payload": f"mst_rm_master:{mid}"}]
            )
        buttons.append([{"type": "callback", "text": rep.BTN_BACK, "payload": "mst_menu"}])
        text = _menu_prepend("\n".join(lines), prepend)
        await self.show_menu_or_edit(
            user_id,
            text,
            [{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
            edit_message_id=edit_message_id,
        )

    async def send_channels_submenu(
        self,
        user_id: int,
        *,
        edit_message_id: Optional[str] = None,
        prepend: Optional[str] = None,
    ) -> None:
        bindings = self.bindings_visible(user_id)
        lines = [
            rep.CHANNELS_HEADER,
            rep.CHANNELS_DELEGATED_HINT.format(emoji=rep.DELEGATED_CHANNEL_EMOJI.strip()),
        ]
        if not bindings:
            lines.append(rep.CHANNELS_EMPTY)
        else:
            for i, b in enumerate(bindings, start=1):
                cid = b["channel_id"]
                ccid = b["comments_chat_id"]
                ct = b.get("channel_title") or f"id {cid}"
                cct = b.get("comments_chat_title") or f"id {ccid}"
                lines.append(rep.channel_list_line(i, ct, int(cid), cct, int(ccid)))
        buttons: List[List[Dict]] = [[{"type": "callback", "text": rep.BTN_ADD_CHANNEL, "payload": "usr_add_ch"}]]
        for b in bindings:
            cid = int(b["channel_id"])
            label = self.channel_label_for_user(user_id, b)[:60]
            buttons.append(
                [{"type": "callback", "text": label, "payload": f"usr_ch_detail:{cid}"}]
            )
        buttons.append([{"type": "callback", "text": rep.BTN_BACK, "payload": "usr_menu"}])
        text = _menu_prepend("\n".join(lines), prepend)
        await self.show_menu_or_edit(
            user_id,
            text,
            [{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
            edit_message_id=edit_message_id,
        )

    async def send_channel_detail_submenu(
        self,
        user_id: int,
        channel_id: int,
        *,
        edit_message_id: Optional[str] = None,
    ) -> None:
        b = self.config.binding_for_channel(channel_id)
        if not b or not self.can_access_channel(user_id, b):
            await self.send_channels_submenu(
                user_id,
                edit_message_id=edit_message_id,
                prepend=rep.CHANNEL_NOT_FOUND_OR_NO_ACCESS,
            )
            return
        cid = int(b["channel_id"])
        ccid = int(b["comments_chat_id"])
        ct = b.get("channel_title") or f"id {cid}"
        cct = b.get("comments_chat_title") or f"id {ccid}"
        text = rep.channel_detail_text(ct, cid, cct, ccid)
        buttons = [
            [{"type": "callback", "text": rep.BTN_MUTE, "payload": f"usr_ch_mute:{cid}"}],
            [{"type": "callback", "text": rep.BTN_POSTS, "payload": f"usr_ch_posts:{cid}:0"}],
            [{"type": "callback", "text": rep.BTN_DELETE, "payload": f"usr_rm_ch:{cid}"}],
            [{"type": "callback", "text": rep.BTN_BACK, "payload": "usr_channels"}],
        ]
        await self.show_menu_or_edit(
            user_id,
            text,
            [{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
            edit_message_id=edit_message_id,
        )

    async def send_delegates_submenu(
        self,
        user_id: int,
        *,
        edit_message_id: Optional[str] = None,
        prepend: Optional[str] = None,
    ) -> None:
        dels = self.direct_delegate_ids(user_id)
        admins_text = ", ".join(str(x) for x in dels) or rep.LIST_EMPTY_DASH
        buttons = [
            [{"type": "callback", "text": rep.BTN_ADD_DELEGATE, "payload": "usr_add_del"}],
        ]
        for did in dels:
            buttons.append(
                [{"type": "callback", "text": f"{rep.BTN_DELETE} {did}", "payload": f"usr_rm_del:{did}"}]
            )
        buttons.append([{"type": "callback", "text": rep.BTN_BACK, "payload": "usr_menu"}])
        text = _menu_prepend(rep.DELEGATES_MENU.format(list_text=admins_text), prepend)
        await self.show_menu_or_edit(
            user_id,
            text,
            [{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
            edit_message_id=edit_message_id,
        )

    async def send_chat_mute_submenu(
        self,
        user_id: int,
        channel_id: int,
        *,
        edit_message_id: Optional[str] = None,
        prepend: Optional[str] = None,
    ) -> None:
        b = self.config.binding_for_channel(channel_id)
        if not b or not self.can_access_channel(user_id, b):
            await self.send_channels_submenu(
                user_id,
                edit_message_id=edit_message_id,
                prepend=rep.CHANNEL_NOT_FOUND_OR_NO_ACCESS,
            )
            return
        mute_en = bool(b.get("chat_mute_enabled"))
        qh = str(b.get("quiet_hours") or "").strip()
        title = str(b.get("channel_title") or rep.channel_title_fallback(channel_id))[:50]
        toggle_text = rep.BTN_MUTE_ON if mute_en else rep.BTN_MUTE_OFF
        buttons = [
            [{"type": "callback", "text": toggle_text, "payload": f"usr_toggle_mute:{channel_id}"}],
            [{"type": "callback", "text": rep.BTN_EDIT_RANGE, "payload": f"usr_mute_range:{channel_id}"}],
            [{"type": "callback", "text": rep.BTN_BACK, "payload": f"usr_ch_detail:{channel_id}"}],
        ]
        current = qh or rep.MUTE_RANGE_NOT_SET
        text = _menu_prepend(rep.mute_submenu_text(title, mute_en, current), prepend)
        await self.show_menu_or_edit(
            user_id,
            text,
            [{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
            edit_message_id=edit_message_id,
        )

    async def on_callback(self, update: Dict[str, Any]) -> None:
        callback_data = update.get("callback", {})
        payload = callback_data.get("payload")
        user_data = callback_data.get("user", {})
        sender_id = int(user_data.get("user_id")) if user_data.get("user_id") else None
        if sender_id is None or not payload:
            return
        callback_mid = message_mid_from_callback_update(update)

        if payload == "mst_ad_click":
            self.config.ad_click_total += 1
            self.config.save()
            u = (self.config.ad_url or "").strip()
            if u:
                await self.send_message(sender_id, u)
            return

        if isinstance(payload, str) and payload.startswith("mst_"):
            if not self.is_master(sender_id):
                return
            if payload == "mst_menu":
                await self.send_master_menu(sender_id, edit_message_id=callback_mid)
            elif payload == "mst_ad":
                await self.send_master_ad_submenu(sender_id, edit_message_id=callback_mid)
            elif payload == "mst_btns":
                await self.send_master_btns_submenu(sender_id, edit_message_id=callback_mid)
            elif payload == "mst_stats":
                await self.show_menu_or_edit(
                    sender_id,
                    rep.stats_line_short(self.config.ad_click_total),
                    [
                        {
                            "type": "inline_keyboard",
                            "payload": {
                                "buttons": [[{"type": "callback", "text": rep.BTN_BACK, "payload": "mst_menu"}]]
                            },
                        }
                    ],
                    edit_message_id=callback_mid,
                )
            elif payload == "mst_masters":
                if not self.is_master_env(sender_id):
                    return
                await self.send_master_masters_submenu(sender_id, edit_message_id=callback_mid)
            elif payload == "mst_add_master":
                if not self.is_master_env(sender_id):
                    return
                self.admin_states[sender_id] = AdminState.AWAITING_NEW_PROMOTED_MASTER
                await self.replace_with_prompt_or_send(
                    sender_id,
                    rep.PROMPT_NEW_MASTER_ID,
                    edit_message_id=callback_mid,
                )
            elif isinstance(payload, str) and payload.startswith("mst_rm_master:"):
                if not self.is_master_env(sender_id):
                    return
                try:
                    rm_mid = int(payload.split(":", 1)[1])
                except ValueError:
                    return
                self.config.promoted_master_ids = [x for x in self.config.promoted_master_ids if x != rm_mid]
                self.config.save()
                await self.send_master_masters_submenu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.PROMOTED_REMOVED.format(mid=rm_mid),
                )
            elif payload == "mst_set_adtxt":
                self.admin_states[sender_id] = AdminState.AWAITING_AD_TEXT
                await self.replace_with_prompt_or_send(sender_id, rep.PROMPT_AD_TEXT, edit_message_id=callback_mid)
            elif payload == "mst_set_adurl":
                self.admin_states[sender_id] = AdminState.AWAITING_AD_LINK
                await self.replace_with_prompt_or_send(
                    sender_id,
                    rep.PROMPT_AD_URL,
                    edit_message_id=callback_mid,
                )
            elif payload == "mst_set_chtxt":
                self.admin_states[sender_id] = AdminState.AWAITING_CHAT_TEXT
                await self.replace_with_prompt_or_send(sender_id, rep.PROMPT_CHAT_BTN, edit_message_id=callback_mid)
            elif payload == "mst_set_msgbtn":
                self.admin_states[sender_id] = AdminState.AWAITING_COMMENTS_MESSAGE_BUTTON_TEXT
                await self.replace_with_prompt_or_send(sender_id, rep.PROMPT_MSG_BTN, edit_message_id=callback_mid)
            return

        if not self.can_use_user_menu(sender_id):
            return

        if payload == "usr_menu":
            await self.send_user_menu(sender_id, edit_message_id=callback_mid)
        elif payload == "usr_channels":
            await self.send_channels_submenu(sender_id, edit_message_id=callback_mid)
        elif isinstance(payload, str) and payload.startswith("usr_ch_posts:"):
            rest = payload[len("usr_ch_posts:") :]
            parts = rest.split(":", 1)
            if len(parts) != 2:
                return
            try:
                ch_id = int(parts[0])
                pg = int(parts[1])
            except ValueError:
                return
            await self.send_posts_list(sender_id, ch_id, pg, edit_message_id=callback_mid)
        elif isinstance(payload, str) and payload.startswith("usr_post_detail:"):
            rest = payload[len("usr_post_detail:") :]
            parts = rest.rsplit(":", 2)
            if len(parts) != 3:
                return
            ref, page_s, ch_s = parts[0], parts[1], parts[2]
            try:
                page = int(page_s)
                list_ch = int(ch_s)
            except ValueError:
                return
            dec = decode_post_ref(ref)
            if not dec:
                await self.send_user_menu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.ERR_BAD_POST_REF,
                )
                return
            cid, mid = dec
            if int(cid) != int(list_ch):
                await self.send_posts_list(
                    sender_id,
                    list_ch,
                    page,
                    edit_message_id=callback_mid,
                    prepend=rep.ERR_CHANNEL_MISMATCH,
                )
                return
            await self.send_post_detail(sender_id, cid, mid, page, edit_message_id=callback_mid)
        elif isinstance(payload, str) and payload.startswith("usr_post_edit:"):
            rest = payload[len("usr_post_edit:") :]
            parts = rest.rsplit(":", 2)
            if len(parts) != 3:
                return
            ref, page_s, ch_s = parts[0], parts[1], parts[2]
            try:
                page = int(page_s)
                list_ch = int(ch_s)
            except ValueError:
                return
            dec = decode_post_ref(ref)
            if not dec:
                await self.send_user_menu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.ERR_BAD_REF,
                )
                return
            cid, mid = dec
            if int(cid) != int(list_ch):
                await self.send_posts_list(
                    sender_id,
                    list_ch,
                    page,
                    edit_message_id=callback_mid,
                    prepend=rep.ERR_CHANNEL_MISMATCH,
                )
                return
            b0 = self.config.binding_for_channel(cid)
            if not b0 or not self.can_access_channel(sender_id, b0):
                await self.send_channels_submenu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.NO_ACCESS_CHANNEL,
                )
                return
            tr = self.config.find_tracked_post(cid, mid)
            if not tr:
                await self.send_posts_list(
                    sender_id,
                    cid,
                    page,
                    edit_message_id=callback_mid,
                    prepend=rep.POST_NOT_FOUND,
                )
                return
            self.post_edit_ref[sender_id] = {
                "channel_id": cid,
                "message_id": mid,
                "message_link": str(tr.get("message_link", "")),
                "return_page": page,
            }
            self.admin_states[sender_id] = AdminState.AWAITING_POST_EDIT_TEXT
            await self.replace_with_prompt_or_send(sender_id, rep.PROMPT_POST_NEW_TEXT, edit_message_id=callback_mid)
        elif isinstance(payload, str) and payload.startswith("usr_post_edit_img:"):
            rest = payload[len("usr_post_edit_img:") :]
            parts = rest.rsplit(":", 2)
            if len(parts) != 3:
                return
            ref, page_s, ch_s = parts[0], parts[1], parts[2]
            try:
                page = int(page_s)
                list_ch = int(ch_s)
            except ValueError:
                return
            dec = decode_post_ref(ref)
            if not dec:
                await self.send_user_menu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.ERR_BAD_REF,
                )
                return
            cid, mid = dec
            if int(cid) != int(list_ch):
                await self.send_posts_list(
                    sender_id,
                    list_ch,
                    page,
                    edit_message_id=callback_mid,
                    prepend=rep.ERR_CHANNEL_MISMATCH,
                )
                return
            b1 = self.config.binding_for_channel(cid)
            if not b1 or not self.can_access_channel(sender_id, b1):
                await self.send_channels_submenu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.NO_ACCESS_CHANNEL,
                )
                return
            tr = self.config.find_tracked_post(cid, mid)
            if not tr:
                await self.send_posts_list(
                    sender_id,
                    cid,
                    page,
                    edit_message_id=callback_mid,
                    prepend=rep.POST_NOT_FOUND,
                )
                return
            self.post_edit_ref[sender_id] = {
                "channel_id": cid,
                "message_id": mid,
                "message_link": str(tr.get("message_link", "")),
                "return_page": page,
            }
            self.admin_states[sender_id] = AdminState.AWAITING_POST_EDIT_IMAGE
            await self.replace_with_prompt_or_send(sender_id, rep.PROMPT_POST_NEW_IMAGE, edit_message_id=callback_mid)
        elif isinstance(payload, str) and payload.startswith("usr_ch_detail:"):
            raw_id = payload.split(":", 1)[1]
            try:
                dcid = int(raw_id)
            except ValueError:
                await self.send_message(sender_id, rep.ERR_BAD_CHANNEL_ID)
                return
            await self.send_channel_detail_submenu(sender_id, dcid, edit_message_id=callback_mid)
        elif payload == "usr_add_ch":
            self.channel_bind_draft.pop(sender_id, None)
            self.admin_states[sender_id] = AdminState.AWAITING_BIND_CHANNEL_INVITE
            await self.replace_with_prompt_or_send(sender_id, rep.BIND_CHANNEL_PROMPT, edit_message_id=callback_mid)
        elif isinstance(payload, str) and payload.startswith("usr_rm_ch:"):
            raw_id = payload.split(":", 1)[1]
            try:
                remove_cid = int(raw_id)
            except ValueError:
                await self.send_message(sender_id, rep.ERR_BAD_CHANNEL_ID)
                return
            br = self.config.binding_for_channel(remove_cid)
            if not br or not self.can_access_channel(sender_id, br):
                await self.send_channels_submenu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.CHANNEL_NOT_FOUND_OR_NO_ACCESS,
                )
                return
            before = len(self.config.channel_bindings)
            self.config.channel_bindings = [b for b in self.config.channel_bindings if int(b["channel_id"]) != remove_cid]
            if len(self.config.channel_bindings) == before:
                await self.send_channels_submenu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.BINDING_REMOVE_NONE,
                )
            else:
                self.config.remove_tracked_posts_for_channel(remove_cid)
                self.config.save()
                await self.send_channels_submenu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.BINDING_REMOVED,
                )
        elif payload == "usr_delegates":
            await self.send_delegates_submenu(sender_id, edit_message_id=callback_mid)
        elif isinstance(payload, str) and payload.startswith("usr_ch_mute:"):
            raw_id = payload.split(":", 1)[1]
            try:
                mc = int(raw_id)
            except ValueError:
                await self.send_message(sender_id, rep.ERR_BAD_CHANNEL_ID)
                return
            bm = self.config.binding_for_channel(mc)
            if not bm or not self.can_access_channel(sender_id, bm):
                await self.send_channels_submenu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.CHANNEL_NOT_FOUND_OR_NO_ACCESS,
                )
                return
            await self.send_chat_mute_submenu(sender_id, mc, edit_message_id=callback_mid)
        elif payload == "usr_add_del":
            self.admin_states[sender_id] = AdminState.AWAITING_NEW_ADMIN
            await self.replace_with_prompt_or_send(
                sender_id,
                rep.PROMPT_DELEGATE_ID,
                edit_message_id=callback_mid,
            )
        elif isinstance(payload, str) and payload.startswith("usr_toggle_mute:"):
            raw_id = payload.split(":", 1)[1]
            try:
                tcid = int(raw_id)
            except ValueError:
                return
            b = self.config.binding_for_channel(tcid)
            if not b or not self.can_access_channel(sender_id, b):
                await self.send_channels_submenu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.CHANNEL_NOT_FOUND_OR_NO_ACCESS,
                )
                return
            b["chat_mute_enabled"] = not bool(b.get("chat_mute_enabled"))
            self.config.save()
            await self.send_chat_mute_submenu(
                sender_id,
                tcid,
                edit_message_id=callback_mid,
                prepend=rep.MUTE_TOGGLED.format(state=rep.mute_state_word(bool(b["chat_mute_enabled"]))),
            )
        elif isinstance(payload, str) and payload.startswith("usr_mute_range:"):
            raw_id = payload.split(":", 1)[1]
            try:
                mcid = int(raw_id)
            except ValueError:
                return
            bm2 = self.config.binding_for_channel(mcid)
            if not bm2 or not self.can_access_channel(sender_id, bm2):
                await self.send_channels_submenu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.CHANNEL_NOT_FOUND_OR_NO_ACCESS,
                )
                return
            self.mute_range_channel_id[sender_id] = mcid
            self.admin_states[sender_id] = AdminState.AWAITING_MUTE_RANGE
            mute_back_kb = [
                {
                    "type": "inline_keyboard",
                    "payload": {
                        "buttons": [
                            [
                                {
                                    "type": "callback",
                                    "text": rep.BTN_BACK,
                                    "payload": f"usr_mute_range_cancel:{mcid}",
                                }
                            ]
                        ]
                    },
                }
            ]
            await self.replace_with_prompt_or_send(
                sender_id,
                rep.PROMPT_MUTE_RANGE,
                edit_message_id=callback_mid,
                attachments=mute_back_kb,
            )
        elif isinstance(payload, str) and payload.startswith("usr_mute_range_cancel:"):
            raw_id = payload.split(":", 1)[1]
            try:
                ccancel = int(raw_id)
            except ValueError:
                return
            bc = self.config.binding_for_channel(ccancel)
            if not bc or not self.can_access_channel(sender_id, bc):
                await self.send_channels_submenu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.CHANNEL_NOT_FOUND_OR_NO_ACCESS,
                )
                return
            self.admin_states[sender_id] = AdminState.NONE
            self.mute_range_channel_id.pop(sender_id, None)
            await self.send_chat_mute_submenu(sender_id, ccancel, edit_message_id=callback_mid)
        elif isinstance(payload, str) and payload.startswith("usr_rm_del:"):
            raw_admin_id = payload.split(":", 1)[1]
            try:
                remove_admin_id = int(raw_admin_id)
            except ValueError:
                await self.send_message(sender_id, rep.ERR_BAD_USER_ID)
                return
            if self.config.delegate_parent.get(remove_admin_id) != sender_id:
                await self.send_delegates_submenu(
                    sender_id,
                    edit_message_id=callback_mid,
                    prepend=rep.DELEGATE_REMOVE_NOT_YOURS,
                )
                return
            del self.config.delegate_parent[remove_admin_id]
            self.config.save()
            await self.send_delegates_submenu(
                sender_id,
                edit_message_id=callback_mid,
                prepend=rep.DELEGATE_REMOVED.format(uid=remove_admin_id),
            )


async def run_webhook_server(
    bot: MaxBot,
    webhook_url: str,
    webhook_secret: Optional[str],
    listen_host: str,
    listen_port: int,
) -> None:
    await bot.get_me()
    try:
        full_url, path = normalize_webhook_url(webhook_url)
    except ValueError as e:
        logger.critical("%s", e)
        sys.exit(1)

    async def webhook_get(_: Request) -> JSONResponse:
        return JSONResponse(
            {
                "ok": True,
                "webhook": True,
                "detail": rep.WEBHOOK_GET_DETAIL,
            }
        )

    async def on_webhook(request: Request) -> Response:
        if webhook_secret:
            if request.headers.get("X-Max-Bot-Api-Secret") != webhook_secret:
                logger.warning(
                    "Webhook 403: секрет не совпал или заголовок X-Max-Bot-Api-Secret отсутствует "
                    "(проверьте WEBHOOK_SECRET в .env и подписку POST /subscriptions)"
                )
                return Response(status_code=403)
        try:
            body = await request.json()
        except Exception:
            return Response(status_code=400)
        if not isinstance(body, dict):
            return Response(status_code=400)
        logger.info("Webhook POST: update_type=%r", body.get("update_type"))
        try:
            await bot.handle_update(body)
        except Exception:
            logger.exception("handle_update failed (webhook)")
        return Response(status_code=200)

    async def health(_: Request) -> JSONResponse:
        return JSONResponse({"ok": True})

    routes = [
        Route(path, webhook_get, methods=["GET"]),
        Route(path, on_webhook, methods=["POST"]),
        Route("/health", health, methods=["GET"]),
    ]

    class AccessLogMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next: Any) -> Any:
            if not (request.url.path == "/health" and request.method == "GET"):
                peer = request.client.host if request.client else "?"
                logger.info("HTTP %s %s from %s", request.method, request.url.path, peer)
            return await call_next(request)

    app = Starlette(routes=routes)
    if os.environ.get("WEBHOOK_ACCESS_LOG", "1").strip() not in ("0", "false", "no"):
        app.add_middleware(AccessLogMiddleware)
    uvicorn_log = os.environ.get("LOG_LEVEL", "info").lower()
    config = uvicorn.Config(app, host=listen_host, port=listen_port, log_level=uvicorn_log)
    server = uvicorn.Server(config)
    serve_task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)
    subscribed_ok = False
    try:
        try:
            await max_subscribe_webhook(bot.client, full_url, webhook_secret)
        except Exception as e:
            logger.critical("Подписка webhook не удалась: %s", e)
            server.should_exit = True
            await serve_task
            raise SystemExit(1) from e
        subscribed_ok = True
        try:
            listed = await max_list_subscriptions(bot.client)
            listed_s = (
                json.dumps(listed, ensure_ascii=False, default=str)
                if isinstance(listed, (dict, list))
                else repr(listed)
            )
            logger.info("GET /subscriptions (текущие подписки): %s", listed_s)
        except Exception as e:
            logger.warning("GET /subscriptions не удалось: %s", e)
        logger.info(
            "Webhook mode: подписка активна, URL=%s, слушаем %s:%s path=%s",
            full_url,
            listen_host,
            listen_port,
            path,
        )
        await serve_task
    finally:
        if subscribed_ok:
            try:
                await max_unsubscribe_webhook(bot.client, full_url)
            except Exception as e:
                logger.warning("Отписка webhook не удалась: %s", e)


async def main() -> None:
    token = os.environ.get("MAX_BOT_TOKEN")
    if not token:
        logger.error("MAX_BOT_TOKEN not found in environment!")
        return
    webhook_url = (os.environ.get("WEBHOOK_URL") or "").strip()
    if not webhook_url:
        logger.error("Укажите WEBHOOK_URL (публичный https://... для POST /subscriptions).")
        return
    webhook_secret_raw = (os.environ.get("WEBHOOK_SECRET") or "").strip()
    webhook_secret: Optional[str] = None
    if webhook_secret_raw:
        if not WEBHOOK_SECRET_RE.match(webhook_secret_raw):
            logger.error(
                "WEBHOOK_SECRET должен быть 5–256 символов [a-zA-Z0-9_-] (см. документацию MAX API)."
            )
            return
        webhook_secret = webhook_secret_raw

    try:
        listen_host, listen_port = parse_listen_host_port(os.environ.get("WEBHOOK_LISTEN", "0.0.0.0:8000"))
    except ValueError as e:
        logger.error("%s", e)
        return

    bot = MaxBot(token, Config())
    try:
        await run_webhook_server(bot, webhook_url, webhook_secret, listen_host, listen_port)
    finally:
        await bot.client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
