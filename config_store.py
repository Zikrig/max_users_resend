"""
Хранение снимка конфигурации в SQLite и периодический SQL-дамп на диск.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

BACKUP_RETENTION_SEC = 7 * 24 * 3600


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def _parent_dir(path: str) -> str:
    return os.path.dirname(os.path.abspath(path))


def read_state(db_path: str) -> Optional[Dict[str, Any]]:
    """Читает JSON-состояние из SQLite или None, если базы/строки ещё нет."""
    if not os.path.isfile(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.Error as e:
        logger.error("SQLite: не удалось открыть %s: %s", db_path, e)
        return None
    try:
        try:
            row = conn.execute("SELECT payload FROM app_config WHERE id = 1").fetchone()
        except sqlite3.OperationalError:
            return None
    finally:
        conn.close()
    if not row or not row[0]:
        return None
    try:
        return json.loads(row[0])
    except json.JSONDecodeError as e:
        logger.error("SQLite: повреждённый JSON в payload: %s", e)
        return None


def write_state(db_path: str, data: Dict[str, Any]) -> None:
    parent = _parent_dir(db_path)
    if parent:
        ensure_dir(parent)
    payload = json.dumps(data, ensure_ascii=False)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS app_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                payload TEXT NOT NULL
            )"""
        )
        conn.execute(
            "INSERT OR REPLACE INTO app_config (id, payload) VALUES (1, ?)",
            (payload,),
        )
        conn.commit()
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
    """Пишет app-YYYYMMDD-HHMMSS.sql и удаляет дампы старше недели."""
    ensure_dir(backup_dir)
    prune_old_backups(backup_dir)
    ts = time.strftime("%Y%m%d-%H%M%S")
    out_path = os.path.join(backup_dir, f"app-{ts}.sql")
    dump_database_to_sql(db_path, out_path)
    logger.info("Дамп SQLite: %s", out_path)
    return out_path
