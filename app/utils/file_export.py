from __future__ import annotations

from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile


def build_sql_zip(export_sql: str, import_sql: str, ddl_externo: str) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, mode="w", compression=ZIP_DEFLATED) as zip_file:
        zip_file.writestr("01_exportacao.sql", export_sql)
        zip_file.writestr("02_importacao.sql", import_sql)
    return buffer.getvalue()
