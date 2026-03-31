from __future__ import annotations

import json
import os

import streamlit as st
from dotenv import load_dotenv

from app.core import ConnectionConfig, DatabaseType, SchemaSource
from app.services.metadata_service import MetadataService
from app.services.rules_service import load_rules
from app.services.sql_generation_service import SQLGenerationService
from app.utils.file_export import build_sql_zip

load_dotenv()

metadata_service = MetadataService()


def _init_state() -> None:
    defaults = {
        "origem_tables": [],
        "destino_tables": [],
        "schema_destino_json": "",
        "schema_origem_contexto": [],
        "relacionamentos": [],
        "export_sql": "",
        "import_sql": "",
        "ddl_externo": "",
        "rules_json": json.dumps(
            {
                "regras_gerais": [],
                "regras_por_tabela": {
                   
                },
                "sinonimos": {
                    
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def _build_connection(prefix: str) -> ConnectionConfig:
    return ConnectionConfig(
        db_type=DatabaseType.FIREBIRD,
        database=st.session_state[f"{prefix}_database"],
        user=st.session_state.get(f"{prefix}_user", os.getenv("FIREBIRD_USER", "SYSDBA")),
        password=st.session_state.get(f"{prefix}_password", os.getenv("FIREBIRD_PASSWORD", "masterkey")),
        host=st.session_state.get(f"{prefix}_host", os.getenv("FIREBIRD_HOST", "localhost")),
        charset=st.session_state.get(f"{prefix}_charset", os.getenv("FIREBIRD_CHARSET", "WIN1252")),
    )





def load_metadata() -> None:
    origem_source = SchemaSource(st.session_state["origem_source"])
    destino_source = SchemaSource(st.session_state["destino_source"])

    origem_connection = _build_connection("origem") if origem_source == SchemaSource.DATABASE else None
    destino_connection = _build_connection("destino") if destino_source == SchemaSource.DATABASE else None

    origem_tables = metadata_service.load_tables(
        source=origem_source,
        ddl_text=st.session_state.get("origem_ddl", ""),
        connection=origem_connection,
    )
    destino_tables = metadata_service.load_tables(
        source=destino_source,
        ddl_text=st.session_state.get("destino_ddl", ""),
        connection=destino_connection,
    )

    st.session_state["origem_tables"] = origem_tables
    st.session_state["destino_tables"] = destino_tables


def generate_sql() -> None:
    origem_table = st.session_state["origem_table"]
    destino_table = st.session_state["destino_table"]

    destino_schema = next((t for t in st.session_state["destino_tables"] if t["tabela"] == destino_table), None)
    if destino_schema is None:
        raise ValueError("Tabela destino não encontrada.")

    origem_source = SchemaSource(st.session_state["origem_source"])
    origem_connection = _build_connection("origem") if origem_source == SchemaSource.DATABASE else None

    schemas_origem, relacionamentos = metadata_service.load_related_origin_context(
        source=origem_source,
        base_table=origem_table,
        ddl_text=st.session_state.get("origem_ddl", ""),
        connection=origem_connection,
    )

    st.session_state["schema_origem_contexto"] = schemas_origem
    st.session_state["relacionamentos"] = relacionamentos

    rules = load_rules(raw_json=st.session_state["rules_json"])
    service = SQLGenerationService(
        api_key=st.session_state.get("openai_api_key") or os.getenv("OPENAI_API_KEY"),
        model=st.session_state.get("openai_model") or os.getenv("OPENAI_MODEL", "gpt-5.4-mini"),
    )
    result = service.gerar_sql(
        schema_destino=destino_schema,
        schemas_origem=schemas_origem,
        relacionamentos=relacionamentos,
        rules=rules,
        pasta_export=st.session_state.get("pasta_export", r"C:\DADOS"),
    )
    st.session_state["schema_destino_json"] = json.dumps(destino_schema, ensure_ascii=False, indent=2)
    st.session_state["ddl_externo"] = result["ddl_externo"]
    st.session_state["export_sql"] = result["export_sql"]
    st.session_state["import_sql"] = result["import_sql"]


def main() -> None:
    _init_state()
    st.set_page_config(page_title="Migrador SQL com IA", layout="wide")
    st.title("Migrador SQL com IA")

    with st.sidebar:
        st.header("OpenAI")
        st.text_input("Modelo", value=os.getenv("OPENAI_MODEL", "gpt-5.4-mini"), key="openai_model")
        st.divider()
        st.header("Configuração de exportação")
        st.text_input("Pasta TXT (EXTERNAL)", value=r"C:\DADOS", key="pasta_export")

    origem_col, destino_col = st.columns(2)

    with origem_col:
        st.subheader("Origem")
        st.selectbox("Tipo", [DatabaseType.FIREBIRD], format_func=lambda x: x.value.upper(), key="origem_dbtype")
        st.radio(
            "Fonte do schema",
            [SchemaSource.DDL.value, SchemaSource.DATABASE.value],
            format_func=lambda x: "DDL colado" if x == "ddl" else "Conexão com banco",
            key="origem_source",
        )
        if st.session_state["origem_source"] == SchemaSource.DATABASE.value:
            uploaded = st.file_uploader("Banco origem (.fdb)", type=["fdb"], key="origem_database_file")
            if uploaded:
                import tempfile
                temp_dir = tempfile.gettempdir()
                temp_path = os.path.join(temp_dir, uploaded.name)
                with open(temp_path, 'wb') as f:
                    f.write(uploaded.getvalue())
                st.session_state["origem_database"] = temp_path
            st.text_input("Banco origem (.fdb)", key="origem_database")
            st.text_input("Host", value=os.getenv("FIREBIRD_HOST", "localhost"), key="origem_host")
            st.text_input("Usuário", value=os.getenv("FIREBIRD_USER", "SYSDBA"), key="origem_user")
            st.text_input("Senha", value=os.getenv("FIREBIRD_PASSWORD", "masterkey"), type="password", key="origem_password")
            st.text_input("Charset", value=os.getenv("FIREBIRD_CHARSET", "WIN1252"), key="origem_charset")
        else:
            st.text_area("DDL origem", height=250, key="origem_ddl")

    with destino_col:
        st.subheader("Destino")
        st.selectbox("Tipo ", [DatabaseType.FIREBIRD], format_func=lambda x: x.value.upper(), key="destino_dbtype")
        st.radio(
            "Fonte do schema ",
            [SchemaSource.DDL.value, SchemaSource.DATABASE.value],
            format_func=lambda x: "DDL colado" if x == "ddl" else "Conexão com banco",
            key="destino_source",
        )
        if st.session_state["destino_source"] == SchemaSource.DATABASE.value:
            uploaded = st.file_uploader("Banco destino (.fdb)", type=["fdb"], key="destino_database_file")
            if uploaded:
                import tempfile
                temp_dir = tempfile.gettempdir()
                temp_path = os.path.join(temp_dir, uploaded.name)
                with open(temp_path, 'wb') as f:
                    f.write(uploaded.getvalue())
                st.session_state["destino_database"] = temp_path
            st.text_input("Banco destino (.fdb)", key="destino_database")
            st.text_input("Host ", value=os.getenv("FIREBIRD_HOST", "localhost"), key="destino_host")
            st.text_input("Usuário ", value=os.getenv("FIREBIRD_USER", "SYSDBA"), key="destino_user")
            st.text_input("Senha ", value=os.getenv("FIREBIRD_PASSWORD", "masterkey"), type="password", key="destino_password")
            st.text_input("Charset ", value=os.getenv("FIREBIRD_CHARSET", "WIN1252"), key="destino_charset")
        else:
            st.text_area("DDL destino", height=250, key="destino_ddl")

    st.subheader("Regras JSON")
    st.text_area("Regras e sinônimos", height=250, key="rules_json")

    if st.button("Carregar tabelas"):
        load_metadata()
        st.success("Metadados carregados.")

    if st.session_state["origem_tables"] and st.session_state["destino_tables"]:
        pick1, pick2 = st.columns(2)
        with pick1:
            st.selectbox("Tabela origem", [t["tabela"] for t in st.session_state["origem_tables"]], key="origem_table")
        with pick2:
            st.selectbox("Tabela destino", [t["tabela"] for t in st.session_state["destino_tables"]], key="destino_table")

        if st.button("Gerar SQL"):
            generate_sql()
            st.success("SQL gerado.")

    if st.session_state["schema_destino_json"]:
        with st.expander("Schema destino"):
            st.code(st.session_state["schema_destino_json"], language="json")
    if st.session_state["schema_origem_contexto"]:
        with st.expander("Contexto da origem"):
            st.json(st.session_state["schema_origem_contexto"])
    if st.session_state["relacionamentos"]:
        with st.expander("Relacionamentos detectados / inferidos"):
            st.json(st.session_state["relacionamentos"])

    result1, result2 = st.columns(2)
    with result1:
        st.subheader("Resultado para exportação")
        st.code(st.session_state.get("export_sql", ""), language="sql")
    with result2:
        st.subheader("Resultado para importação")
        st.code(st.session_state.get("import_sql", ""), language="sql")

    if st.session_state.get("export_sql") and st.session_state.get("import_sql"):
        zip_bytes = build_sql_zip(
            export_sql=st.session_state["export_sql"],
            import_sql=st.session_state["import_sql"],
            ddl_externo=st.session_state["ddl_externo"],
        )
        st.download_button(
            "Salvar SQLs (.zip)",
            data=zip_bytes,
            file_name="sql_migracao.zip",
            mime="application/zip",
        )


if __name__ == "__main__":
    main()
