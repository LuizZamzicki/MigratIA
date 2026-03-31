from __future__ import annotations

from app.core import ConnectionConfig, DatabaseType
from app.db.connectors.base import BaseSchemaReader


class SchemaReaderFactory:
    @staticmethod
    def create(config: ConnectionConfig) -> BaseSchemaReader:
        if config.db_type == DatabaseType.FIREBIRD:
            from app.db.connectors.firebird import FirebirdSchemaReader
            return FirebirdSchemaReader(config)
        raise ValueError(f"Banco não suportado ainda: {config.db_type}")
