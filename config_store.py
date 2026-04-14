"""
SQLite: настройки, пользователи (делегаты), привязки каналов, трек постов.
Миграция v1: таблица app_config(payload) → нормализованные таблицы.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

BACKUP_RETENTION_SEC = 7 * 24 * 3600


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def _parent_dir(path: str) -> str:
    return os.path.dirname(os.path.abspath(path))


def _connect(db_path: str) -> sqlite3.Connection:
    parent = _parent_dir(db_path)
    if parent:
        ensure_dir(parent)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            ad_text TEXT NOT NULL,
            ad_url TEXT NOT NULL,
            comments_chat_text TEXT NOT NULL,
            comments_message_button_text TEXT NOT NULL,
            promoted_master_ids TEXT NOT NULL,
            instruction_text TEXT NOT NULL DEFAULT '',
            instruction_button_text TEXT NOT NULL DEFAULT 'Инструкция',
            instruction_enabled INTEGER NOT NULL DEFAULT 1,
            instruction_text_format TEXT,
            instruction_markup TEXT
        );

        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY NOT NULL,
            delegate_parent_id INTEGER NULL,
            FOREIGN KEY (delegate_parent_id) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS channel_bindings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_root_id INTEGER NOT NULL,
            created_by INTEGER NOT NULL,
            channel_id INTEGER NOT NULL UNIQUE,
            comments_chat_id INTEGER NOT NULL,
            comments_chat_link TEXT NOT NULL,
            channel_title TEXT,
            comments_chat_title TEXT,
            chat_mute_enabled INTEGER NOT NULL DEFAULT 0,
            quiet_hours TEXT NOT NULL DEFAULT '',
            FOREIGN KEY (account_root_id) REFERENCES users(user_id),
            FOREIGN KEY (created_by) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS tracked_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            message_id TEXT NOT NULL,
            text TEXT NOT NULL DEFAULT '',
            message_link TEXT NOT NULL DEFAULT '',
            saved_at REAL NOT NULL,
            chat_message_id TEXT NOT NULL DEFAULT '',
            media_attachments TEXT NOT NULL DEFAULT '[]',
            text_format TEXT,
            markup TEXT,
            UNIQUE(channel_id, message_id)
        );

        CREATE INDEX IF NOT EXISTS idx_tracked_posts_channel ON tracked_posts(channel_id);
        """
    )
    conn.execute(
        "INSERT OR IGNORE INTO schema_meta (key, value) VALUES ('version', '2')"
    )
    settings_cols = {
        str(r["name"])
        for r in conn.execute("PRAGMA table_info(settings)")
    }
    if "instruction_text" not in settings_cols:
        conn.execute(
            "ALTER TABLE settings ADD COLUMN instruction_text TEXT NOT NULL DEFAULT ''"
        )
    if "instruction_button_text" not in settings_cols:
        conn.execute(
            "ALTER TABLE settings ADD COLUMN instruction_button_text TEXT NOT NULL DEFAULT 'Инструкция'"
        )
    if "instruction_enabled" not in settings_cols:
        conn.execute(
            "ALTER TABLE settings ADD COLUMN instruction_enabled INTEGER NOT NULL DEFAULT 1"
        )
    if "instruction_text_format" not in settings_cols:
        conn.execute("ALTER TABLE settings ADD COLUMN instruction_text_format TEXT")
    if "instruction_markup" not in settings_cols:
        conn.execute("ALTER TABLE settings ADD COLUMN instruction_markup TEXT")


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _migrate_app_config_payload_if_present(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "app_config"):
        return
    row = None
    try:
        row = conn.execute("SELECT payload FROM app_config WHERE id = 1").fetchone()
    except sqlite3.OperationalError:
        return
    if not row or not row[0]:
        conn.execute("DROP TABLE IF EXISTS app_config")
        return
    try:
        data = json.loads(row[0])
    except json.JSONDecodeError as e:
        logger.error("Миграция v1: повреждённый JSON в app_config: %s", e)
        return
    try:
        _write_payload_to_tables(conn, data)
        conn.execute("DROP TABLE app_config")
        logger.info("Миграция v1: app_config перенесена в таблицы users/channel_bindings/…")
    except Exception as e:
        logger.exception("Миграция v1 не удалась: %s", e)
        raise


def _collect_user_ids(data: Dict[str, Any]) -> Set[int]:
    ids: Set[int] = set()
    raw_pm = data.get("promoted_master_ids")
    if raw_pm is not None:
        if isinstance(raw_pm, list):
            for x in raw_pm:
                try:
                    ids.add(int(x))
                except (TypeError, ValueError):
                    pass
    raw_dp = data.get("delegate_parent") or {}
    if isinstance(raw_dp, dict):
        for k, v in raw_dp.items():
            try:
                ids.add(int(k))
                ids.add(int(v))
            except (TypeError, ValueError):
                pass
    for b in data.get("channel_bindings") or []:
        if not isinstance(b, dict):
            continue
        for key in ("account_root_id", "created_by"):
            if key in b and b[key] is not None:
                try:
                    ids.add(int(b[key]))
                except (TypeError, ValueError):
                    pass
    return ids


def _upsert_users(
    conn: sqlite3.Connection,
    delegate_parent: Dict[int, int],
    user_ids: Set[int],
) -> None:
    for uid in user_ids:
        conn.execute(
            "INSERT OR IGNORE INTO users (user_id, delegate_parent_id) VALUES (?, NULL)",
            (uid,),
        )
    for child, parent in sorted(delegate_parent.items()):
        conn.execute(
            "UPDATE users SET delegate_parent_id = ? WHERE user_id = ?",
            (parent, child),
        )


def _write_payload_to_tables(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    init_schema(conn)
    promoted = data.get("promoted_master_ids")
    if not isinstance(promoted, list):
        promoted = []
    dp_raw = data.get("delegate_parent") or {}
    delegate_parent: Dict[int, int] = {}
    if isinstance(dp_raw, dict):
        for k, v in dp_raw.items():
            try:
                delegate_parent[int(k)] = int(v)
            except (TypeError, ValueError):
                pass

    uids = _collect_user_ids(data)
    uids |= set(delegate_parent.keys()) | set(delegate_parent.values())
    _upsert_users(conn, delegate_parent, uids)

    conn.execute(
        """INSERT OR REPLACE INTO settings (
            id, ad_text, ad_url, comments_chat_text, comments_message_button_text, promoted_master_ids,
            instruction_text, instruction_button_text, instruction_enabled, instruction_text_format, instruction_markup
        ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            str(data.get("ad_text", "")),
            str(data.get("ad_url", "")),
            str(data.get("comments_chat_text", "")),
            str(data.get("comments_message_button_text", "")),
            json.dumps(promoted, ensure_ascii=False),
            str(data.get("instruction_text", "")),
            str(data.get("instruction_button_text", "Инструкция")),
            1 if bool(data.get("instruction_enabled", True)) else 0,
            (
                str(data.get("instruction_text_format"))
                if data.get("instruction_text_format") in ("markdown", "html")
                else None
            ),
            (
                json.dumps(data.get("instruction_markup"), ensure_ascii=False)
                if isinstance(data.get("instruction_markup"), list)
                else None
            ),
        ),
    )

    conn.execute("DELETE FROM tracked_posts")
    conn.execute("DELETE FROM channel_bindings")

    for b in data.get("channel_bindings") or []:
        if not isinstance(b, dict):
            continue
        try:
            cid = int(b["channel_id"])
            ccid = int(b["comments_chat_id"])
            link = str(b.get("comments_chat_link", "")).strip()
            ar = int(b["account_root_id"])
            cb = int(b["created_by"])
        except (KeyError, TypeError, ValueError):
            continue
        if not link:
            continue
        conn.execute(
            """INSERT INTO channel_bindings (
                account_root_id, created_by, channel_id, comments_chat_id, comments_chat_link,
                channel_title, comments_chat_title, chat_mute_enabled, quiet_hours
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                ar,
                cb,
                cid,
                ccid,
                link,
                b.get("channel_title"),
                b.get("comments_chat_title"),
                1 if b.get("chat_mute_enabled") else 0,
                str(b.get("quiet_hours") or ""),
            ),
        )

    for p in data.get("tracked_posts") or []:
        if not isinstance(p, dict):
            continue
        try:
            ch = int(p["channel_id"])
            mid = str(p["message_id"])
        except (KeyError, TypeError, ValueError):
            continue
        ma = p.get("media_attachments")
        if not isinstance(ma, list):
            ma = []
        mk = p.get("markup")
        markup_json = json.dumps(mk, ensure_ascii=False) if isinstance(mk, list) else None
        tf = p.get("text_format")
        if tf is not None and tf not in ("markdown", "html"):
            tf = None
        conn.execute(
            """INSERT OR REPLACE INTO tracked_posts (
                channel_id, message_id, text, message_link, saved_at, chat_message_id,
                media_attachments, text_format, markup
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                ch,
                mid,
                str(p.get("text", "")),
                str(p.get("message_link", "")),
                float(p.get("saved_at", 0)),
                str(p.get("chat_message_id") or ""),
                json.dumps(ma, ensure_ascii=False),
                tf,
                markup_json,
            ),
        )


def save_config(db_path: str, data: Dict[str, Any]) -> None:
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        init_schema(conn)
        _migrate_app_config_payload_if_present(conn)
        _write_payload_to_tables(conn, data)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _row_to_tracked_post(row: sqlite3.Row) -> Dict[str, Any]:
    raw_ma = row["media_attachments"] or "[]"
    try:
        ma = json.loads(raw_ma)
    except json.JSONDecodeError:
        ma = []
    if not isinstance(ma, list):
        ma = []
    out: Dict[str, Any] = {
        "channel_id": int(row["channel_id"]),
        "message_id": str(row["message_id"]),
        "text": str(row["text"] or ""),
        "message_link": str(row["message_link"] or ""),
        "saved_at": float(row["saved_at"] or 0),
        "chat_message_id": str(row["chat_message_id"] or ""),
        "media_attachments": ma,
    }
    tf = row["text_format"]
    if tf in ("markdown", "html"):
        out["text_format"] = tf
    raw_mk = row["markup"]
    if raw_mk:
        try:
            mk = json.loads(raw_mk)
            if isinstance(mk, list):
                out["markup"] = [dict(x) for x in mk if isinstance(x, dict)]
        except json.JSONDecodeError:
            pass
    return out


def load_config(db_path: str) -> Optional[Dict[str, Any]]:
    if not os.path.isfile(db_path):
        return None
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        init_schema(conn)
        _migrate_app_config_payload_if_present(conn)
        conn.commit()

        srow = conn.execute("SELECT * FROM settings WHERE id = 1").fetchone()
        if srow is None:
            return None

        try:
            promoted = json.loads(srow["promoted_master_ids"] or "[]")
        except json.JSONDecodeError:
            promoted = []
        if not isinstance(promoted, list):
            promoted = []

        delegate_parent: Dict[str, int] = {}
        for u in conn.execute("SELECT user_id, delegate_parent_id FROM users"):
            pid = u["delegate_parent_id"]
            if pid is not None:
                delegate_parent[str(int(u["user_id"]))] = int(pid)

        bindings: List[Dict[str, Any]] = []
        for r in conn.execute(
            """SELECT account_root_id, created_by, channel_id, comments_chat_id, comments_chat_link,
                channel_title, comments_chat_title, chat_mute_enabled, quiet_hours
               FROM channel_bindings ORDER BY id"""
        ):
            bindings.append(
                {
                    "channel_id": int(r["channel_id"]),
                    "comments_chat_id": int(r["comments_chat_id"]),
                    "comments_chat_link": str(r["comments_chat_link"] or ""),
                    "channel_title": r["channel_title"],
                    "comments_chat_title": r["comments_chat_title"],
                    "chat_mute_enabled": bool(r["chat_mute_enabled"]),
                    "quiet_hours": str(r["quiet_hours"] or ""),
                    "account_root_id": int(r["account_root_id"]),
                    "created_by": int(r["created_by"]),
                }
            )

        tracked: List[Dict[str, Any]] = []
        for r in conn.execute(
            "SELECT * FROM tracked_posts ORDER BY saved_at DESC"
        ):
            tracked.append(_row_to_tracked_post(r))
        raw_instruction_markup = srow["instruction_markup"]
        instruction_markup: List[Dict[str, Any]] = []
        if raw_instruction_markup:
            try:
                parsed_markup = json.loads(raw_instruction_markup)
                if isinstance(parsed_markup, list):
                    instruction_markup = [dict(x) for x in parsed_markup if isinstance(x, dict)]
            except json.JSONDecodeError:
                instruction_markup = []

        return {
            "ad_text": str(srow["ad_text"]),
            "ad_url": str(srow["ad_url"]),
            "comments_chat_text": str(srow["comments_chat_text"]),
            "comments_message_button_text": str(srow["comments_message_button_text"]),
            "instruction_text": str(srow["instruction_text"] or ""),
            "instruction_button_text": str(srow["instruction_button_text"] or "Инструкция"),
            "instruction_enabled": bool(srow["instruction_enabled"]),
            "instruction_text_format": (
                str(srow["instruction_text_format"])
                if srow["instruction_text_format"] in ("markdown", "html")
                else None
            ),
            "instruction_markup": instruction_markup,
            "promoted_master_ids": promoted,
            "delegate_parent": delegate_parent,
            "channel_bindings": bindings,
            "tracked_posts": tracked,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def dump_database_to_sql(db_path: str, out_sql: str) -> None:
    if not os.path.isfile(db_path):
        logger.warning("Дамп: файл БД не найден: %s", db_path)
        return
    parent = _parent_dir(out_sql)
    if parent:
        ensure_dir(parent)
    conn = sqlite3.connect(db_path)
    try:
        with open(out_sql, "w", encoding="utf-8") as f:
            for line in conn.iterdump():
                f.write(line)
                if not line.endswith("\n"):
                    f.write("\n")
    finally:
        conn.close()


def prune_old_backups(backup_dir: str, max_age_sec: float = BACKUP_RETENTION_SEC) -> None:
    if not os.path.isdir(backup_dir):
        return
    cutoff = time.time() - max_age_sec
    for name in os.listdir(backup_dir):
        path = os.path.join(backup_dir, name)
        try:
            if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                os.remove(path)
                logger.info("Удалён старый дамп: %s", path)
        except OSError as e:
            logger.warning("Не удалось удалить %s: %s", path, e)


def backup_now(db_path: str, backup_dir: str) -> Optional[str]:
    ensure_dir(backup_dir)
    prune_old_backups(backup_dir)
    ts = time.strftime("%Y%m%d-%H%M%S")
    out_path = os.path.join(backup_dir, f"app-{ts}.sql")
    dump_database_to_sql(db_path, out_path)
    logger.info("Дамп SQLite: %s", out_path)
    return out_path


# Обратная совместимость имён
read_state = load_config
write_state = save_config
