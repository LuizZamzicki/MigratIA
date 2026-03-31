from __future__ import annotations

from app.core import ConnectionConfig, SchemaSource
from app.db.connectors.factory import SchemaReaderFactory
from app.services.ddl_parser import parse_ddl_tables


class MetadataService:
    def load_tables(self, source: SchemaSource, ddl_text: str = "", connection: ConnectionConfig | None = None) -> list[dict]:
        if source == SchemaSource.DDL:
            return parse_ddl_tables(ddl_text)
        if connection is None:
            raise ValueError("Configuração de conexão não informada.")
        reader = SchemaReaderFactory.create(connection)
        tables = []
        for table in reader.list_tables():
            tables.append({"tabela": table, "campos": reader.read_table_structure(table)})
        return tables

    def load_related_origin_context(
        self,
        source: SchemaSource,
        base_table: str,
        ddl_text: str = "",
        connection: ConnectionConfig | None = None,
    ) -> tuple[list[dict], list[dict]]:
        if source == SchemaSource.DDL:
            tables = parse_ddl_tables(ddl_text)
            base = next((t for t in tables if t["tabela"] == base_table.upper()), None)
            if base is None:
                raise ValueError("Tabela base da origem não encontrada no DDL informado.")
            inferred = infer_possible_relationships(tables, base_table.upper())
            return [base, *[t for t in tables if t["tabela"] in {fk['tabela_destino'] for fk in inferred}]], inferred
        if connection is None:
            raise ValueError("Configuração de conexão não informada.")
        reader = SchemaReaderFactory.create(connection)
        schemas, fks = reader.read_related_schemas(base_table.upper())
        if not fks:
            all_tables = []
            for table in reader.list_tables():
                all_tables.append({"tabela": table, "campos": reader.read_table_structure(table)})
            return schemas, infer_possible_relationships(all_tables, base_table.upper())
        return schemas, fks


def infer_possible_relationships(tables: list[dict], base_table: str) -> list[dict]:
    base = next((t for t in tables if t["tabela"] == base_table), None)
    if not base:
        return []
    base_fields = {c["campo"].upper() for c in base["campos"]}
    relations: list[dict] = []
    for table in tables:
        if table["tabela"] == base_table:
            continue
        for col in table["campos"]:
            target_field = col["campo"].upper()
            candidates = {target_field, f"ID{table['tabela']}", f"{table['tabela']}_ID"}
            common = base_fields.intersection(candidates)
            if common:
                relations.append(
                    {
                        "tabela_origem": base_table,
                        "campo_origem": sorted(common)[0],
                        "tabela_destino": table["tabela"],
                        "campo_destino": target_field,
                        "inferido": True,
                    }
                )
                break
    return relations
