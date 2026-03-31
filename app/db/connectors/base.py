from __future__ import annotations

from abc import ABC, abstractmethod


class BaseSchemaReader(ABC):
    @abstractmethod
    def list_tables(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def read_table_structure(self, table: str) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def read_foreign_keys(self, table: str) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def read_related_schemas(self, base_table: str) -> tuple[list[dict], list[dict]]:
        raise NotImplementedError
