from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, List

import requests


# =========================
#   Utilidades de CNPJ
# =========================
def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def is_valid_cnpj(cnpj: str) -> bool:
    cnpj = only_digits(cnpj)
    if len(cnpj) != 14:
        return False
    if cnpj == cnpj[0] * 14:
        return False

    def calc_dv(base: str) -> str:
        weights = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        if len(base) == 12:
            w = weights[1:]
        else:
            w = weights
        total = sum(int(d) * w[i] for i, d in enumerate(base))
        mod = total % 11
        dv = 0 if mod < 2 else 11 - mod
        return str(dv)

    base12 = cnpj[:12]
    dv1 = calc_dv(base12)
    dv2 = calc_dv(base12 + dv1)
    return cnpj[-2:] == dv1 + dv2


def format_cnpj(cnpj: str) -> str:
    c = only_digits(cnpj)
    if len(c) != 14:
        return cnpj
    return f"{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:]}"


# =========================
#   Cliente HTTP robusto
# =========================
@dataclass
class FetchResult:
    source: str
    status_code: int
    data: Optional[Dict[str, Any]]
    error: Optional[str]


class CNPJClient:
    def __init__(
        self,
        timeout: int = 20,
        sleep_seconds: float = 0.35,
        max_retries: int = 4,
        backoff_base: float = 0.8,
        user_agent: str = "BIA-CNPJ-Streamlit/1.0",
    ):
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": user_agent})

    def _request_json(self, url: str, source: str) -> FetchResult:
        last_err = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = self.sess.get(url, timeout=self.timeout)

                # Rate limit / instabilidade
                if resp.status_code in (429, 502, 503, 504):
                    wait = self.backoff_base * (2 ** attempt)
                    time.sleep(wait)
                    continue

                if resp.status_code == 404:
                    return FetchResult(source, 404, None, "CNPJ não encontrado")

                resp.raise_for_status()
                return FetchResult(source, resp.status_code, resp.json(), None)

            except requests.RequestException as e:
                last_err = str(e)
                wait = self.backoff_base * (2 ** attempt)
                time.sleep(wait)

        return FetchResult(source, -1, None, last_err or "Falha desconhecida")

    def fetch(self, cnpj: str, prefer: str = "brasilapi") -> FetchResult:
        cnpj_d = only_digits(cnpj)

        if prefer.lower() == "minhareceita":
            order = ["minhareceita", "brasilapi"]
        else:
            order = ["brasilapi", "minhareceita"]

        last = FetchResult("N/A", -1, None, "Falha desconhecida")

        for src in order:
            if src == "brasilapi":
                url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_d}"
                last = self._request_json(url, "BrasilAPI")
            else:
                url = f"https://minhareceita.org/{cnpj_d}"
                last = self._request_json(url, "MinhaReceita")

            if last.data is not None:
                time.sleep(self.sleep_seconds)
                return last

            time.sleep(self.sleep_seconds)

        return last


# =========================
#   Normalização
# =========================
def pick(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def extract_socios(data: Dict[str, Any]) -> List[Dict[str, str]]:
    socios: List[Dict[str, str]] = []
    qsa = data.get("qsa") or data.get("socios") or data.get("quadro_societario")

    if isinstance(qsa, list):
        for item in qsa:
            if not isinstance(item, dict):
                continue
            nome = (
                item.get("nome_socio")
                or item.get("nome")
                or item.get("razao_social")
                or item.get("nome_representante")
            )
            qual = (
                item.get("qualificacao_socio")
                or item.get("qualificacao")
                or item.get("descricao_qualificacao_socio")
                or item.get("qualificacao_representante")
            )
            if nome:
                socios.append(
                    {
                        "nome": str(nome).strip(),
                        "qualificacao": str(qual).strip() if qual else "",
                    }
                )
    return socios


def normalize_company(data: Dict[str, Any], cnpj_digits: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    out["cnpj"] = pick(data, "cnpj") or format_cnpj(cnpj_digits)
    out["razao_social"] = pick(data, "razao_social", "nome")
    out["nome_fantasia"] = pick(data, "nome_fantasia", "fantasia")
    out["situacao_cadastral"] = pick(data, "descricao_situacao_cadastral", "situacao", "status")
    out["data_abertura"] = pick(data, "data_inicio_atividade", "abertura")

    out["capital_social"] = pick(data, "capital_social", "capital")

    # Endereço
    logradouro = pick(data, "logradouro")
    numero = pick(data, "numero")
    complemento = pick(data, "complemento")
    bairro = pick(data, "bairro")
    municipio = pick(data, "municipio", "cidade")
    uf = pick(data, "uf")
    cep = pick(data, "cep")

    endereco_partes = [p for p in [logradouro, numero, complemento] if p]
    out["endereco"] = " ".join(str(p) for p in endereco_partes).strip() or None
    out["bairro"] = bairro
    out["municipio"] = municipio
    out["uf"] = uf
    out["cep"] = cep

    out["cnae_principal"] = pick(data, "cnae_fiscal", "cnae_principal")
    out["descricao_cnae"] = pick(data, "cnae_fiscal_descricao", "descricao_cnae_fiscal")

    out["socios"] = extract_socios(data)

    return out


def consultar_cnpj(
    cnpj_input: str,
    prefer: str = "brasilapi",
    client: Optional[CNPJClient] = None,
) -> Dict[str, Any]:
    """
    Retorna um dict padrão:
    {
      "ok": bool,
      "cnpj": "...",
      "source": "BrasilAPI|MinhaReceita",
      "summary": {...},
      "error": "..."
    }
    """
    cnpj_digits = only_digits(cnpj_input)

    if not is_valid_cnpj(cnpj_digits):
        return {
            "ok": False,
            "cnpj": cnpj_input,
            "source": None,
            "summary": None,
            "error": "CNPJ inválido (verifique dígitos).",
        }

    if client is None:
        client = CNPJClient()

    res = client.fetch(cnpj_digits, prefer=prefer)
    if res.data is None:
        return {
            "ok": False,
            "cnpj": format_cnpj(cnpj_digits),
            "source": res.source,
            "summary": None,
            "error": res.error or "Falha ao consultar.",
        }

    summary = normalize_company(res.data, cnpj_digits)
    return {
        "ok": True,
        "cnpj": summary.get("cnpj") or format_cnpj(cnpj_digits),
        "source": res.source,
        "summary": summary,
        "error": None,
        "raw": res.data,  # útil para debug (você pode esconder no app)
    }

