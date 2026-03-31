from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class DatabaseType(StrEnum):
    FIREBIRD = "firebird"
    MYSQL = "mysql"
    POSTGRES = "postgres"


class SchemaSource(StrEnum):
    DATABASE = "database"
    DDL = "ddl"


@dataclass
class ConnectionConfig:
    db_type: DatabaseType
    database: str
    user: str = "SYSDBA"
    password: str = "masterkey"
    host: str = "localhost"
    charset: str = "WIN1252"


SchemaTable = dict[str, Any]
