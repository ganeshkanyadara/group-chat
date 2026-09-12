"""
Database package for persistent encrypted chat storage.
"""
from .database import (
    init_db,
    store_message,
    load_history_raw,
    save_public_key,
    load_public_key,
    get_message_by_id,
    tamper_message,
    check_db_health,
    get_db_connection,
    get_db_path,
    register_online_user,
    remove_online_user,
    clear_backend_users,
    get_all_online_users,
    record_cluster_event,
    get_cluster_events,
    DB_PATH,
)

__all__ = [
    "init_db",
    "store_message",
    "load_history_raw",
    "save_public_key",
    "load_public_key",
    "get_message_by_id",
    "tamper_message",
    "check_db_health",
    "get_db_connection",
    "get_db_path",
    "register_online_user",
    "remove_online_user",
    "clear_backend_users",
    "get_all_online_users",
    "record_cluster_event",
    "get_cluster_events",
    "DB_PATH",
]

