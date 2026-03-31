from __future__ import annotations

import os
from pathlib import Path

from app.core import ConnectionConfig
from app.db.connectors.base import BaseSchemaReader

try:
    import fdb
except Exception as exc:  # pragma: no cover
    fdb = None
    FDB_IMPORT_ERROR = exc
else:
    FDB_IMPORT_ERROR = None


class FirebirdSchemaReader(BaseSchemaReader):
    def __init__(self, config: ConnectionConfig) -> None:
        self.config = config
        self._load_client_if_present()

    def _load_client_if_present(self) -> None:
        if fdb is None:
            raise RuntimeError(f"Driver fdb indisponível: {FDB_IMPORT_ERROR}")

        local_dll = Path(__file__).resolve().parents[3] / "fbclient.dll"
        env_dll = os.getenv("FIREBIRD_CLIENT_DLL")
        dll_path = Path(env_dll) if env_dll else local_dll
        if dll_path.exists():
            try:
                fdb.load_api(str(dll_path))
            except Exception:
                pass

    def _connect(self):
        if fdb is None:
            raise RuntimeError(f"Driver fdb indisponível: {FDB_IMPORT_ERROR}")
        return fdb.connect(
            dsn=f"{self.config.host}:{self.config.database}",
            user=self.config.user,
            password=self.config.password,
            charset=self.config.charset,
        )

    def list_tables(self) -> list[str]:
        con = self._connect()
        cur = con.cursor()
        cur.execute(
            """
            SELECT TRIM(RDB$RELATION_NAME)
            FROM RDB$RELATIONS
            WHERE RDB$VIEW_BLR IS NULL
              AND COALESCE(RDB$SYSTEM_FLAG, 0) = 0
            ORDER BY 1
            """
        )
        rows = [r[0] for r in cur.fetchall()]
        con.close()
        return rows

    def read_table_structure(self, table: str) -> list[dict]:
        con = self._connect()
        cur = con.cursor()
        cur.execute(
            """
            SELECT
                TRIM(RF.RDB$FIELD_NAME) AS FIELD_NAME,
                F.RDB$FIELD_TYPE       AS FIELD_TYPE,
                F.RDB$FIELD_SUB_TYPE   AS FIELD_SUB_TYPE,
                F.RDB$FIELD_LENGTH     AS FIELD_LENGTH,
                F.RDB$FIELD_PRECISION  AS FIELD_PRECISION,
                F.RDB$FIELD_SCALE      AS FIELD_SCALE,
                F.RDB$CHARACTER_LENGTH AS CHARLENGTH
            FROM RDB$RELATION_FIELDS RF
            JOIN RDB$FIELDS F
              ON RF.RDB$FIELD_SOURCE = F.RDB$FIELD_NAME
            WHERE RF.RDB$RELATION_NAME = ?
            ORDER BY RF.RDB$FIELD_POSITION
            """,
            (table,),
        )
        cols: list[dict] = []
        for r in cur.fetchall():
            tamanho = r[6] if r[6] else r[3]
            cols.append(
                {
                    "campo": r[0],
                    "tipo_code": r[1],
                    "sub_type": r[2],
                    "tamanho": tamanho,
                    "precision": r[4],
                    "scale": r[5],
                }
            )
        con.close()
        return cols

    def read_foreign_keys(self, table: str) -> list[dict]:
        con = self._connect()
        cur = con.cursor()
        cur.execute(
            """
            SELECT
                TRIM(ISG.RDB$FIELD_NAME)         AS FK_FIELD,
                TRIM(REL_DEST.RDB$RELATION_NAME) AS PK_TABLE,
                TRIM(ISG_DEST.RDB$FIELD_NAME)    AS PK_FIELD
            FROM RDB$RELATION_CONSTRAINTS RC
            JOIN RDB$INDEX_SEGMENTS ISG
              ON RC.RDB$INDEX_NAME = ISG.RDB$INDEX_NAME
            JOIN RDB$REF_CONSTRAINTS REF
              ON RC.RDB$CONSTRAINT_NAME = REF.RDB$CONSTRAINT_NAME
            JOIN RDB$RELATION_CONSTRAINTS RC_DEST
              ON REF.RDB$CONST_NAME_UQ = RC_DEST.RDB$CONSTRAINT_NAME
            JOIN RDB$INDEX_SEGMENTS ISG_DEST
              ON RC_DEST.RDB$INDEX_NAME = ISG_DEST.RDB$INDEX_NAME
            JOIN RDB$RELATIONS REL_DEST
              ON RC_DEST.RDB$RELATION_NAME = REL_DEST.RDB$RELATION_NAME
            WHERE RC.RDB$RELATION_NAME = ?
              AND RC.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
            """,
            (table,),
        )
        fks = [
            {
                "tabela_origem": table,
                "campo_origem": r[0],
                "tabela_destino": r[1],
                "campo_destino": r[2],
            }
            for r in cur.fetchall()
        ]
        con.close()
        return fks

    def read_related_schemas(self, base_table: str) -> tuple[list[dict], list[dict]]:
        schemas: list[dict] = []
        seen: set[str] = set()

        def add_schema(table_name: str) -> None:
            if table_name in seen:
                return
            seen.add(table_name)
            schemas.append({"tabela": table_name, "campos": self.read_table_structure(table_name)})

        add_schema(base_table)
        fks = self.read_foreign_keys(base_table)
        for fk in fks:
            add_schema(fk["tabela_destino"])
        return schemas, fks
