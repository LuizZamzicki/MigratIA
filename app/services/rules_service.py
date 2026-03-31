from __future__ import annotations

import json
from pathlib import Path


def load_rules(raw_json: str | None = None, file_path: str | None = None) -> dict:
    if raw_json and raw_json.strip():
        return json.loads(raw_json)
    if file_path:
        return json.loads(Path(file_path).read_text(encoding="utf-8"))
    return {"regras_gerais": [], "regras_por_tabela": {}, "sinonimos": {}}


def render_rules_for_prompt(rules: dict, target_table: str) -> tuple[str, str]:
    gerais = rules.get("regras_gerais", [])
    por_tabela = rules.get("regras_por_tabela", {}).get(target_table.upper(), [])
    sinonimos = rules.get("sinonimos", {})

    regras_txt = "\n".join(f"- {item}" for item in [*gerais, *por_tabela]) or "Nenhuma regra extra."
    sinonimos_txt = json.dumps(sinonimos, ensure_ascii=False, indent=2) if sinonimos else "Nenhum sinônimo informado."
    return regras_txt, sinonimos_txt
