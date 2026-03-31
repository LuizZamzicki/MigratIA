from __future__ import annotations

import json
import os
import re
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

from app.services.rules_service import render_rules_for_prompt

load_dotenv()


def extract_sql_pure(text: str) -> str:
    text = re.sub(r"```(?:sql)?", "", text or "", flags=re.IGNORECASE)
    return text.strip()


def tamanho_char_firebird(campo: dict[str, Any]) -> int:
    tipo = campo.get("tipo_code")
    tamanho = campo.get("tamanho") or 20
    if tipo == 12:
        return 10
    if tipo == 13:
        return 8
    if tipo == 35:
        return 19
    if tipo == 261:
        return 500
    if tipo in (7, 8, 16, 27, 26, 10):
        return 20
    return min(int(tamanho), 4000)


def gerar_ddl_externo(schema_json: dict[str, Any], pasta: str) -> str:
    nome = f"{schema_json['tabela']}_EXPORTA"
    caminho = os.path.join(pasta or r"C:\\DADOS", f"{schema_json['tabela']}.txt").replace("\\", "/")
    colunas = [f"    {c['campo']} CHAR({tamanho_char_firebird(c)})" for c in schema_json["campos"]]
    colunas.append("    LF CHAR(2)")
    return f"CREATE TABLE {nome} EXTERNAL FILE '{caminho}' (\n" + ",\n".join(colunas) + "\n);"


class SQLGenerationService:
    def __init__(self, api_key: str | None = None, model: str | None = None) -> None:
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-5.4-mini")

    def gerar_sql(
        self,
        schema_destino: dict,
        schemas_origem: list[dict],
        relacionamentos: list[dict],
        rules: dict,
        pasta_export: str,
    ) -> dict[str, str]:
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY não configurada.")

        ddl_externo = gerar_ddl_externo(schema_destino, pasta_export)
        regras_txt, sinonimos_txt = render_rules_for_prompt(rules, schema_destino["tabela"])
        relacionamentos_txt = "\n".join(
            f"- {fk['tabela_origem']}.{fk['campo_origem']} -> {fk['tabela_destino']}.{fk['campo_destino']}"
            + (" -- INFERIDO" if fk.get("inferido") else "")
            for fk in relacionamentos
        ) or "Nenhum relacionamento detectado."

        prompt = f"""
Você é um especialista em migração de dados Firebird/SQL ANSI.

Retorne SOMENTE SQL em dois blocos, nesta ordem exata:
EXPORT:
<sql exportação>
IMPORT:
<sql importação>

REGRAS OBRIGATÓRIAS:
- Gerar exportação para tabela EXTERNAL FILE com layout fixo baseado no DDL abaixo.
- Gerar importação a partir da tabela externa.
- Na importação, usar TRIM() em todos os campos de texto.
- Se destino for menor que origem, usar SUBSTRING(... FROM 1 FOR <tam>) e comentar na mesma linha: -- VERIFICAR MANUALMENTE. A virgula de final do campo deve vir antes do comentário.
- Se origem for TIMESTAMP e destino DATE, usar SUBSTRING(campo FROM 1 FOR 10)
- Se um campo não for encontrado, preencher com valor compatível e comentar -- NENHUM CAMPO ENCONTRADO
- Se existir relacionamento útil, usar LEFT JOIN.
- Se não houver FK explícita, você pode usar relacionamentos inferidos, mas preserve comentário -- VERIFICAR MANUALMENTE quando necessário.
- Nunca omitir colunas do destino.
- Nunca escreva explicações fora do SQL.
- No bloco EXPORT, incluir APENAS INSERT INTO tabela externa e DROP TABLE da externa (NÃO incluir CREATE TABLE, pois será adicionado separadamente).
- No bloco IMPORT, incluir INSERT INTO destino e DROP TABLE da externa (NÃO incluir CREATE TABLE, pois será adicionado separadamente).
- Um campo por linha no SELECT.
- Sempre usar alias AS no SELECT de exportação.
- Quebra de linha deve ser feita usando ASCII_CHAR(10) e ASCII_CHAR(13) dependendo do banco, verificar qual é o caso e usar a função correta.
                

REGRAS ESPECÍFICAS:
{regras_txt}

SINÔNIMOS:
{sinonimos_txt}

RELACIONAMENTOS:
{relacionamentos_txt}

DDL EXTERNO:
{ddl_externo}

DESTINO:
{json.dumps(schema_destino, ensure_ascii=False, indent=2)}

ORIGEM:
{json.dumps(schemas_origem, ensure_ascii=False, indent=2)}
""".strip()

        client = OpenAI(api_key=self.api_key)
        response = client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": "Você gera somente SQL Firebird válido."},
                {"role": "user", "content": prompt},
            ],
        )
        raw = extract_sql_pure(response.output_text)
        if "IMPORT:" not in raw:
            raise RuntimeError("A resposta da IA não veio no formato esperado com EXPORT:/IMPORT:.")
        export_sql, import_sql = raw.split("IMPORT:", 1)
        export_sql = export_sql.replace("EXPORT:", "").strip()
        import_sql = import_sql.strip()
        return {
            "ddl_externo": ddl_externo,
            "export_sql": ddl_externo + "\n\n" + export_sql,
            "import_sql": ddl_externo + "\n\n" + import_sql,
            "raw": raw,
        }
