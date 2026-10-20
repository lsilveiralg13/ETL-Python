import json
from datetime import datetime, timezone
from typing import Any, Optional


def criar_registro_feedback(
    *,
    exchange_id: str,
    prompt: str,
    resposta: str,
    avaliacao: str,
    resposta_corrigida: str,
    dialeto_sql: str,
    modelo: Optional[str],
    anexos: list[str],
) -> dict[str, Any]:
    avaliacao_normalizada = avaliacao.strip().lower()
    if avaliacao_normalizada not in {"positive", "negative"}:
        raise ValueError("A avaliação deve ser positive ou negative.")
    resposta_corrigida = resposta_corrigida.strip()
    if avaliacao_normalizada == "negative" and not resposta_corrigida:
        raise ValueError(
            "Inclua uma resposta corrigida para aproveitar uma avaliação negativa."
        )
    resposta_alvo = (
        resposta_corrigida
        if avaliacao_normalizada == "negative"
        else resposta.strip()
    )
    return {
        "schema_version": 1,
        "exchange_id": exchange_id,
        "prompt": prompt.strip(),
        "resposta_original": resposta.strip(),
        "resposta_alvo": resposta_alvo,
        "avaliacao": avaliacao_normalizada,
        "dialeto_sql": dialeto_sql,
        "modelo": modelo,
        "anexos": anexos,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "elegivel_para_treinamento_supervisionado": bool(resposta_alvo),
    }


def exportar_jsonl(registros: list[dict[str, Any]]) -> str:
    return "\n".join(
        json.dumps(registro, ensure_ascii=False, default=str)
        for registro in registros
    )
