from __future__ import annotations

import re
from typing import Any

TYPE_CODE_MAP = {
    "SMALLINT": 7,
    "INTEGER": 8,
    "BIGINT": 16,
    "INT128": 26,
    "FLOAT": 10,
    "DOUBLE": 27,
    "DATE": 12,
    "TIME": 13,
    "TIMESTAMP": 35,
    "BLOB": 261,
    "CHAR": 14,
    "VARCHAR": 37,
    "DECIMAL": 16,
    "NUMERIC": 16,
}


def parse_ddl_tables(ddl_text: str) -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []
    create_pattern = re.compile(
        r"CREATE\s+TABLE\s+([\w\$\"]+)\s*\((.*?)\);",
        re.IGNORECASE | re.DOTALL,
    )
    for match in create_pattern.finditer(ddl_text):
        raw_table = match.group(1).strip().strip('"')
        body = match.group(2)
        fields: list[dict[str, Any]] = []
        for raw_line in split_columns(body):
            line = raw_line.strip().rstrip(",")
            upper = line.upper()
            if not line or upper.startswith(("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CONSTRAINT", "CHECK")):
                continue
            col_match = re.match(r'"?([\w\$]+)"?\s+(.+)$', line, re.IGNORECASE)
            if not col_match:
                continue
            field_name = col_match.group(1).strip().upper()
            type_part = col_match.group(2).strip()
            field_info = parse_type_part(field_name, type_part)
            fields.append(field_info)
        tables.append({"tabela": raw_table.upper(), "campos": fields})
    return tables


def split_columns(body: str) -> list[str]:
    result: list[str] = []
    current: list[str] = []
    level = 0
    for char in body:
        if char == '(':
            level += 1
        elif char == ')':
            level -= 1
        if char == ',' and level == 0:
            result.append(''.join(current))
            current = []
            continue
        current.append(char)
    if current:
        result.append(''.join(current))
    return result


def parse_type_part(field_name: str, type_part: str) -> dict[str, Any]:
    upper = type_part.upper()
    type_name = upper.split()[0]
    size = None
    precision = None
    scale = None

    size_match = re.search(r'\((\d+)(?:\s*,\s*(\d+))?\)', upper)
    if size_match:
        precision = int(size_match.group(1))
        size = precision
        if size_match.group(2):
            scale = -int(size_match.group(2))

    if type_name == 'CHARACTER':
        if 'VARYING' in upper:
            type_name = 'VARCHAR'
        else:
            type_name = 'CHAR'

    if type_name == 'TIMESTAMP':
        size = 19
    elif type_name == 'DATE':
        size = 10
    elif type_name == 'TIME':
        size = 8
    elif type_name in {'INTEGER', 'SMALLINT', 'BIGINT', 'INT128', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE'} and size is None:
        size = 20
    elif type_name == 'BLOB' and size is None:
        size = 500

    return {
        "campo": field_name,
        "tipo": type_name,
        "tipo_code": TYPE_CODE_MAP.get(type_name, 37),
        "sub_type": 0,
        "tamanho": size or 20,
        "precision": precision,
        "scale": scale,
    }
