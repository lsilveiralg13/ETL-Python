from __future__ import annotations

import csv
import os
import socket
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import streamlit as st
from cnpj_core import CNPJClient, consultar_cnpj, only_digits, format_cnpj, fetch_ie_cnpja_open

# =========================
#   Log local (CSV)
# =========================
def get_log_path() -> Path:
    appdata = os.getenv("APPDATA") or str(Path.home())
    base = Path(appdata) / "BIA-CNPJ"
    base.mkdir(parents=True, exist_ok=True)
    return base / "consultas.csv"


def append_log(row: Dict[str, Any]) -> None:
    path = get_log_path()
    file_exists = path.exists()

    headers = [
        "timestamp",
        "usuario_windows",
        "computador",
        "cnpj",
        "fonte",
        "sucesso",
        "erro",
    ]

    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, delimiter=";")
        if not file_exists:
            w.writeheader()
        w.writerow({h: row.get(h, "") for h in headers})


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_user() -> str:
    return os.getenv("USERNAME") or os.getenv("USER") or "desconhecido"


def get_host() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "desconhecido"


# =========================
#   Streamlit UI
# =========================
st.set_page_config(page_title="BIA – Consulta CNPJ", layout="centered")

st.title("🔎 BIA – Consulta de CNPJ (SDR)")
st.caption("Consulta via BrasilAPI com fallback para MinhaReceita. Log local habilitado (metadados).")

with st.expander("⚙️ Configurações", expanded=False):
    prefer = st.selectbox("Fonte preferida", ["brasilapi", "minhareceita"], index=0)
    sleep = st.slider("Pausa entre consultas (segundos)", min_value=0.1, max_value=1.5, value=0.35, step=0.05)
    retries = st.slider("Re-tentativas (rede/429)", min_value=0, max_value=6, value=4, step=1)
    timeout = st.slider("Timeout HTTP (segundos)", min_value=5, max_value=60, value=20, step=5)
    mostrar_raw = st.checkbox("Mostrar JSON bruto (debug)", value=False)

st.write("Cole um ou mais CNPJs (um por linha):")

cnpjs_txt = st.text_area(
    "CNPJs",
    placeholder="Ex:\n33.683.111/0001-07\n00.000.000/0001-91",
    height=160,
)

col1, col2 = st.columns(2)
with col1:
    btn = st.button("Consultar", type="primary")
with col2:
    limpar = st.button("Limpar")

if limpar:
    st.session_state["cnpjs_txt"] = ""
    st.rerun()

# Mantém valor do text_area ao limpar de forma previsível
if "cnpjs_txt" not in st.session_state:
    st.session_state["cnpjs_txt"] = cnpjs_txt
else:
    st.session_state["cnpjs_txt"] = cnpjs_txt


def render_result(result: Dict[str, Any]) -> None:
    if not result["ok"]:
        st.error(f"❌ {result.get('cnpj')}: {result.get('error')}")
        return

    s = result["summary"]
    st.success(f"{s.get('razao_social') or 'Razão social não informada'}")
    st.write(f"**CNPJ:** {s.get('cnpj')}")
    st.write(f"**Fonte:** {result.get('source')}")

    # Cards “limpos”
    st.markdown("### 📌 Cadastro")
    c1, c2 = st.columns(2)
    with c1:
        st.write("**Situação:**", s.get("situacao_cadastral") or "-")
        st.write("**Abertura:**", s.get("data_abertura") or "-")
    with c2:
        st.write("**Capital social:**", s.get("capital_social") or "-")
        st.write("**Nome fantasia:**", s.get("nome_fantasia") or "-")

    st.markdown("### 🗺️ Endereço")
    endereco = s.get("endereco") or "-"
    st.write("**Endereço:**", endereco)
    st.write("**Bairro:**", s.get("bairro") or "-")
    st.write("**Cidade/UF:**", f"{s.get('municipio') or '-'} / {s.get('uf') or '-'}")
    st.write("**CEP:**", s.get("cep") or "-")

    # =========================
    #   NOVO: Inscrição Estadual (IE) - CNPJá (open)
    # =========================
    st.markdown("### 🧾 Inscrição Estadual (IE)")
    try:
        ie_info = fetch_ie_cnpja_open(s.get("cnpj") or "", uf_preferida=s.get("uf"))
        ie_num = ie_info.get("ie")
        if ie_num:
            st.write("**IE:**", ie_num)
            st.write("**UF da IE:**", ie_info.get("uf_ie") or (s.get("uf") or "-"))
            if ie_info.get("situacao_ie"):
                st.write("**Situação da IE:**", ie_info.get("situacao_ie"))
            st.caption(f"Fonte IE: {ie_info.get('fonte')}")
        else:
            st.info("IE não localizada automaticamente para este CNPJ (pode não existir, ser isento, ou não estar disponível na fonte).")
            if ie_info.get("erro"):
                st.caption(f"Detalhe: {ie_info.get('erro')}")
            st.caption(f"Fonte IE: {ie_info.get('fonte')}")
    except Exception as e:
        st.warning("Não foi possível consultar IE neste momento.")
        st.caption(f"Detalhe técnico: {e}")

    st.markdown("### 🏷️ Atividade (CNAE)")
    st.write("**CNAE principal:**", s.get("cnae_principal") or "-")
    st.write("**Descrição CNAE:**", s.get("descricao_cnae") or "-")

    st.markdown("### 👥 Sócios (QSA)")
    socios: List[Dict[str, str]] = s.get("socios") or []
    if not socios:
        st.info("QSA não disponível nesta fonte para este CNPJ.")
    else:
        for item in socios:
            nome = item.get("nome", "")
            qual = item.get("qualificacao", "")
            st.write(f"- {nome}" + (f" ({qual})" if qual else ""))

    if mostrar_raw:
        st.markdown("### 🧪 JSON bruto (debug)")
        st.json(result.get("raw", {}))


if btn:
    # Normaliza linhas
    raw_lines = [ln.strip() for ln in (cnpjs_txt or "").splitlines() if ln.strip()]
    if not raw_lines:
        st.warning("Informe ao menos um CNPJ.")
        st.stop()

    # Remove duplicados preservando ordem
    seen = set()
    cnpjs = []
    for ln in raw_lines:
        d = only_digits(ln)
        key = d or ln
        if key not in seen:
            seen.add(key)
            cnpjs.append(ln)

    st.write(f"📦 Total para consultar: **{len(cnpjs)}**")

    client = CNPJClient(timeout=timeout, sleep_seconds=sleep, max_retries=retries)

    for idx, cnpj in enumerate(cnpjs, start=1):
        with st.spinner(f"[{idx}/{len(cnpjs)}] Consultando {format_cnpj(only_digits(cnpj)) or cnpj}..."):
            result = consultar_cnpj(cnpj, prefer=prefer, client=client)

        # Log (metadados)
        append_log(
            {
                "timestamp": now_str(),
                "usuario_windows": get_user(),
                "computador": get_host(),
                "cnpj": result.get("cnpj") or cnpj,
                "fonte": result.get("source") or "",
                "sucesso": "SIM" if result.get("ok") else "NAO",
                "erro": "" if result.get("ok") else (result.get("error") or ""),
            }
        )

        # UI
        st.divider()
        render_result(result)

    st.divider()
    st.success("Concluído ✅")
    st.caption(f"Log salvo em: {get_log_path()}")
