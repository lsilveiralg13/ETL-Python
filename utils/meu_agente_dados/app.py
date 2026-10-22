import json
import logging
import os
import re
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Optional

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from utils.meu_agente_dados.attachments import (
    CHAT_ONLY_EXTENSIONS,
    MAX_UPLOAD_BYTES,
    SUPPORTED_UPLOAD_EXTENSIONS,
    extrair_texto_upload,
    preparar_anexo,
)
from utils.meu_agente_dados.feedback import criar_registro_feedback, exportar_jsonl

# =============================================================================
# CONFIGURAÇÃO DA PÁGINA
# =============================================================================
st.set_page_config(
    page_title="Vetra — Agente Consultivo de Dados",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="collapsed",
)

RAW_API_URL = os.environ.get("VETRA_API_URL", "http://127.0.0.1:8000").strip()
RAW_API_URL = RAW_API_URL.replace("0.0.0.0", "127.0.0.1")
API_BASE_URL = re.sub(r"/(chat|chat/stream)/?$", "", RAW_API_URL).rstrip("/")

MAX_TEXTO_RAG = min(
    int(os.environ.get("VETRA_MAX_TEXTO_RAG", 90_000)),
    90_000,
)
MAX_TOTAL_UPLOAD_BYTES = int(
    os.environ.get("VETRA_MAX_TOTAL_ATTACHMENT_BYTES", 25 * 1024 * 1024)
)
MAX_ATTACHMENTS = int(os.environ.get("VETRA_MAX_ATTACHMENTS", "10"))
MAX_FEEDBACK_EXAMPLES = 200
if min(MAX_TOTAL_UPLOAD_BYTES, MAX_ATTACHMENTS) < 1:
    raise ValueError("Os limites de upload da Vetra devem ser maiores que zero.")
HTTP_TIMEOUT = (5, 60)
logger = logging.getLogger("vetra.streamlit")

CATALOGO_FERRAMENTAS = [
    {"nome": "validar_e_executar_sql", "descricao": "Valida via AST e schema; não executa no banco"},
    {"nome": "buscar_conhecimento_rag", "descricao": "Busca vetorial/semântica no Qdrant Cloud com Cache"},
    {"nome": "indexar_documento_com_chunking", "descricao": "Chunking + Ingestão no Qdrant Cloud (PII Masked)"},
    {"nome": "listar_esquemas_e_tabelas", "descricao": "Lista tabelas e visões do ambiente"},
    {"nome": "descrever_estrutura_tabela", "descricao": "Traz DDL, colunas e tipos de uma tabela"},
    {"nome": "calcular_indicador_otif", "descricao": "Métrica de entregas logísticas On-Time In-Full"},
    {"nome": "calcular_lead_time_producao", "descricao": "Métrica de tempo de ciclo de produção/OEE"},
    {"nome": "consultar_ibge_sidra", "descricao": "Consulta indicadores econômicos (IPCA) e população no IBGE/SIDRA"},
    {"nome": "buscar_dados_municipio_ibge", "descricao": "Obtém código IBGE, UF e região de um município"},
    {"nome": "consultar_indicadores_bcb", "descricao": "Séries do Banco Central (Selic, IPCA, Dólar)"},
    {"nome": "consultar_cnpj_brasilapi", "descricao": "Consulta cadastral de empresas na Receita Federal"},
    {"nome": "consultar_cep_brasilapi", "descricao": "Endereçamento e geolocalização por CEP"},
    {"nome": "consultar_cotacao_moeda", "descricao": "Cotação de moedas em tempo real (AwesomeAPI)"},
    {"nome": "geocodificar_endereco", "descricao": "Converte nomes de lugares em Latitude/Longitude"},
    {"nome": "consultar_clima_open_meteo", "descricao": "Temperatura e condições do tempo via Open-Meteo"},
    {"nome": "consultar_futebol_liga", "descricao": "Tabela e classificação do Campeonato Brasileiro"},
]

SUGESTOES_INICIAIS = [
    "Quais tabelas eu tenho disponíveis?",
    "Explique a estrutura da tb_producao",
    "Qual a fórmula do OTIF % acumulado?",
    "Monte uma query de produção do mês atual",
]


if "preferencias_usuario" not in st.session_state:
    st.session_state.preferencias_usuario = {
        "provider": "copilot",
        "dialeto_sql": "PostgreSQL",
        "departamento": "Engenharia de Dados",
        "temperatura": 0.2,
        "mascarar_pii": True,
        "perfil": "Engenheiro de Dados (Preciso)",
    }
if "messages" not in st.session_state:
    st.session_state.messages = [{
        "role": "model",
        "content": "Olá! Sou o **Vetra**, seu agente consultivo de engenharia de dados e BI. Posso explorar esquemas, montar queries SQL, validar regras de negócio e fornecer insights proativos sobre seus indicadores. Como posso ajudar?",
    }]
for chave, valor in {
    "pending_prompt": None,
    "feedback_notice": None,
    "ultimo_modelo": None,
    "total_chamadas_mcp": 0,
    "feedback_fine_tuning": [],
    "total_tokens_input": 0,
    "total_tokens_output": 0,
    "custo_acumulado_usd": 0.0,
    "total_copilot_ai_credits": 0.0,
    "copilot_last_ai_credits": 0.0,
}.items():
    if chave not in st.session_state:
        st.session_state[chave] = valor


def aplicar_mascaramento_pii(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    df_mascarado = df.copy()
    colunas_mascaradas = []
    padroes_pii = [r"cpf", r"email", r"e-mail", r"telefone", r"celular", r"nome", r"sobrenome", r"cartao", r"credito", r"senha", r"usuario"]

    def mascarar(valor: Any) -> str:
        if pd.isna(valor):
            return ""
        texto = str(valor)
        return texto[0] + "***" + texto[-1] if len(texto) > 2 else "***"

    for col in df_mascarado.columns:
        if any(re.search(padrao, str(col).lower()) for padrao in padroes_pii):
            colunas_mascaradas.append(str(col))
            df_mascarado[col] = df_mascarado[col].apply(mascarar)
    return df_mascarado, colunas_mascaradas


def executar_sanity_check_df(df: pd.DataFrame) -> list[str]:
    alertas = []
    nulos = df.isnull().sum()
    cols_com_nulos = nulos[nulos > 0]
    if not cols_com_nulos.empty:
        detalhes = ", ".join(f"`{c}` ({v} nulos)" for c, v in cols_com_nulos.items())
        alertas.append(f"⚠️ **Valores Nulos Detectados**: {detalhes}.")
    for col in df.select_dtypes(include=["number"]).columns:
        if any(kw in str(col).lower() for kw in ["otif", "qtd", "quantidade", "valor", "total", "lead_time", "preco"]):
            negativos = int((df[col] < 0).sum())
            if negativos:
                alertas.append(f"⚠️ **Inconsistência Numérica**: `{col}` possui {negativos} valores negativos inesperados.")
    return alertas


def analisar_explain_plan_sql(sql_query: str) -> list[str]:
    alertas = []
    query_upper = sql_query.upper()
    num_joins = len(re.findall(r"\bJOIN\b", query_upper))
    if num_joins >= 3:
        alertas.append(f"⚡ **Query Complexa ({num_joins} JOINs)**: EXPLAIN estimado de alto custo. Recomendado aplicar filtros de partição/data na cláusula WHERE.")
    if re.search(r"\bSELECT\s+\*", query_upper):
        alertas.append("⚡ **Varredura Ampla (`SELECT *`)**: Pode aumentar o I/O de rede e latência.")
    return alertas


def chamar_api_backend(
    prompt: str,
    historico: list,
    dialeto_sql: str,
    temperatura: float,
    provider: str,
    access_token: str,
    anexos: Optional[list] = None,
    on_event: Optional[Callable[[dict[str, Any]], None]] = None,
):
    historico_recente = historico[-10:]
    historico_formatado = []
    for mensagem in historico_recente:
        if isinstance(mensagem, dict):
            role = mensagem.get("role", "user")
            if role == "model":
                role = "assistant"
            historico_formatado.append({"role": role, "content": str(mensagem.get("content", ""))})

    # O prompt atual é enviado separadamente; não o duplica no histórico.
    if historico_formatado and historico_formatado[-1].get("role") == "user" and historico_formatado[-1].get("content") == prompt:
        historico_formatado.pop()

    payload = {
        "prompt": prompt,
        "historico": historico_formatado,
        "anexos": anexos or [],
        "provider": provider,
        "dialeto_sql": dialeto_sql,
        "temperatura": max(0.0, min(2.0, float(temperatura))),
    }
    erros_http = []
    stream_iniciado = False
    stream_concluido = False
    headers = {
        "Accept": "text/event-stream",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    try:
        with requests.post(
            f"{API_BASE_URL}/chat/stream",
            json=payload,
            headers=headers,
            stream=True,
            timeout=HTTP_TIMEOUT,
        ) as response:
            if response.ok:
                stream_iniciado = True
                resposta_texto = ""
                modelo_usado = "FastAPI-Backend"
                mcp_chamado = False
                dados_mcp_raw = None
                for linha in response.iter_lines():
                    if isinstance(linha, bytes):
                        linha = linha.decode("utf-8", errors="replace")
                    if not linha or not linha.strip().startswith("data:"):
                        continue
                    texto_evento = linha.strip()[5:].strip()
                    if texto_evento == "[DONE]":
                        break
                    try:
                        conteudo = json.loads(texto_evento)
                    except json.JSONDecodeError:
                        continue
                    if conteudo.get("type") == "error" or conteudo.get("error"):
                        return (
                            "❌ Erro retornado pelo backend: "
                            f"{conteudo.get('message') or conteudo.get('error')}",
                            None,
                            False,
                            None,
                        )
                    if on_event is not None and conteudo.get("type") not in {
                        "done",
                        None,
                    }:
                        on_event(conteudo)
                    if conteudo.get("type") == "done" or (
                        conteudo.get("type") is None
                        and conteudo.get("resposta") is not None
                    ):
                        stream_concluido = True
                    trecho = conteudo.get("resposta")
                    if trecho is not None:
                        # Compatível com backends que enviam resposta acumulada ou chunks.
                        resposta_texto = str(trecho) if str(trecho).startswith(resposta_texto) else resposta_texto + str(trecho)
                    modelo_usado = conteudo.get("modelo_usado", modelo_usado)
                    mcp_chamado = bool(conteudo.get("mcp_chamado", mcp_chamado))
                    dados_mcp_raw = conteudo.get("dados_mcp_raw", dados_mcp_raw)
                if stream_concluido and resposta_texto.strip():
                    return resposta_texto, modelo_usado, mcp_chamado, dados_mcp_raw
                if stream_iniciado:
                    return (
                        "❌ O stream do backend terminou antes de enviar uma "
                        "resposta completa. A chamada não será repetida para "
                        "evitar executar novamente as ferramentas.",
                        None,
                        False,
                        None,
                    )
            else:
                erros_http.append(f"stream HTTP {response.status_code}")
                if response.status_code not in {404, 405, 501}:
                    return (
                        f"❌ O backend recusou o stream (HTTP {response.status_code}).",
                        None,
                        False,
                        None,
                    )
    except requests.RequestException as erro:
        erros_http.append(f"stream: {erro}")
        if stream_iniciado:
            return (
                "❌ A conexão do stream foi interrompida; a consulta não será "
                "repetida para evitar duplicar chamadas de ferramentas.",
                None,
                False,
                None,
            )

    try:
        with requests.post(
            f"{API_BASE_URL}/chat",
            json=payload,
            headers={**headers, "Accept": "application/json"},
            timeout=HTTP_TIMEOUT,
        ) as response:
            if response.ok:
                dados = response.json()
                if isinstance(dados, dict):
                    resposta = str(dados.get("resposta") or "").strip()
                    if resposta:
                        return resposta, dados.get("modelo_usado", "FastAPI-Backend"), bool(dados.get("mcp_chamado", False)), dados.get("dados_mcp_raw")
                erros_http.append("resposta síncrona vazia ou inválida")
            else:
                erros_http.append(f"chat HTTP {response.status_code}")
    except (requests.RequestException, ValueError) as erro:
        erros_http.append(f"chat: {erro}")

    detalhe = "; ".join(erros_http[-2:])
    sufixo = f" Detalhes: {detalhe}" if detalhe else ""
    return f"❌ Não foi possível conectar ao servidor backend da Vetra ({API_BASE_URL}).{sufixo}", None, False, None


st.markdown(
    """
<style>
[data-testid="stAppViewContainer"] {
    background:
        radial-gradient(ellipse at 16% 0%, rgba(69, 196, 176, .08), transparent 34rem),
        #090D18;
}
[data-testid="stMainBlockContainer"] {
    max-width: 1160px;
}
[data-testid="stHeader"] {
    background: transparent;
}
[data-testid="stSidebar"] {
    border-right: 1px solid #202B3D;
}
[data-testid="stChatMessage"] {
    border: 1px solid rgba(125, 150, 185, .13);
    border-radius: 18px;
    background: rgba(17, 24, 39, .68);
}
[data-testid="stChatInput"] {
    border: 1px solid rgba(83, 214, 181, .3);
    border-radius: 18px;
    background: rgba(17, 24, 39, .92);
    box-shadow: 0 12px 36px rgba(0, 0, 0, .2);
}
.vt-brand {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: .4rem 0 1.6rem;
}
.vt-mark {
    display: grid;
    width: 48px;
    height: 48px;
    place-items: center;
    border: 1px solid rgba(83, 214, 181, .32);
    border-radius: 15px;
    background: linear-gradient(145deg, rgba(83, 214, 181, .2), rgba(83, 214, 181, .04));
    color: #67E3C2;
    font: 700 1.35rem 'Space Grotesk', sans-serif;
}
.vt-eyebrow {
    color: #67E3C2;
    font-size: .68rem;
    font-weight: 600;
    letter-spacing: .16em;
}
.vt-brand h1 {
    margin: .12rem 0;
    font: 600 1.55rem 'Space Grotesk', sans-serif;
}
.vt-brand p {
    margin: 0;
    color: #8998AE;
    font-size: .88rem;
}
.vt-kicker {
    color: #8C9AB0;
    font-size: .8rem;
    letter-spacing: .12em;
    text-transform: uppercase;
}
.vt-welcome {
    max-width: 52rem;
    padding: 1.2rem 0 1.6rem;
}
.vt-welcome h2 {
    margin: .4rem 0;
    font: 600 clamp(1.7rem, 3vw, 2.5rem) 'Space Grotesk', sans-serif;
    letter-spacing: -.035em;
}
.vt-welcome p {
    max-width: 42rem;
    color: #97A5BA;
    font-size: 1rem;
    line-height: 1.7;
}
.vt-section-label {
    margin: 1.5rem 0 .6rem;
    color: #8C9AB0;
    font-size: .74rem;
    font-weight: 600;
    letter-spacing: .1em;
    text-transform: uppercase;
}
.vt-tool-name {
    color: #67E3C2;
    font-family: 'JetBrains Mono', monospace;
    font-size: .76rem;
}
</style>
""",
    unsafe_allow_html=True,
)


if "copilot_session_token" not in st.session_state:
    st.session_state.copilot_session_token = None

if "code" in st.query_params or "error" in st.query_params:
    oauth_code = str(st.query_params.get("code", ""))
    oauth_state = str(st.query_params.get("state", ""))
    oauth_error = str(st.query_params.get("error_description") or st.query_params.get("error") or "")
    st.query_params.clear()
    if oauth_error:
        st.session_state.github_auth_error = "O login do GitHub foi cancelado ou recusado."
    elif oauth_code and oauth_state:
        try:
            callback_response = requests.post(
                f"{API_BASE_URL}/auth/github/callback",
                json={"code": oauth_code, "state": oauth_state},
                timeout=(5, 20),
            )
            callback_response.raise_for_status()
            callback_data = callback_response.json()
            st.session_state.copilot_session_token = callback_data["session_token"]
            st.session_state.github_login = callback_data["login"]
            st.session_state.pop("github_auth_error", None)
        except (requests.RequestException, ValueError, KeyError) as erro:
            logger.warning("Falha ao finalizar o login GitHub: %s", erro)
            detalhe = ""
            resposta_erro = getattr(erro, "response", None)
            if resposta_erro is not None:
                try:
                    detalhe = str(resposta_erro.json().get("detail") or "")
                except ValueError:
                    detalhe = ""
            st.session_state.github_auth_error = (
                "Não foi possível concluir o login do GitHub. Tente entrar novamente."
                + (f" Motivo: {detalhe}" if detalhe else f" Detalhe técnico: {type(erro).__name__}: {erro}")
            )

auth_status = st.session_state.get("github_auth_status")
if auth_status is None:
    try:
        resposta_status = requests.get(
            f"{API_BASE_URL}/auth/github/status",
            timeout=HTTP_TIMEOUT,
        )
        resposta_status.raise_for_status()
        auth_status = resposta_status.json()
        st.session_state.github_auth_status = auth_status
    except (requests.RequestException, ValueError) as erro:
        logger.warning(
            "Não foi possível consultar a configuração de autenticação: %s",
            erro,
        )
        st.error(
            "Não foi possível conectar ao backend da Vetra para verificar o login. "
            "Recarregue a página ou tente novamente mais tarde.\n\n"
            f"Detalhe técnico: `{type(erro).__name__}: {erro}`"
        )
        st.stop()

copilot_session_token = st.session_state.copilot_session_token
github_login = st.session_state.get("github_login")
if copilot_session_token:
    try:
        resposta_usuario = requests.get(
            f"{API_BASE_URL}/auth/github/me",
            headers={"Authorization": f"Bearer {copilot_session_token}"},
            timeout=HTTP_TIMEOUT,
        )
        if resposta_usuario.status_code == 401:
            st.session_state.pop("copilot_session_token", None)
            st.session_state.pop("github_login", None)
            copilot_session_token = None
        else:
            resposta_usuario.raise_for_status()
            github_login = resposta_usuario.json()["login"]
            st.session_state.github_login = github_login
    except (requests.RequestException, ValueError, KeyError) as erro:
        logger.warning("Não foi possível validar a sessão GitHub da Vetra: %s", erro)
        st.error("Não foi possível validar sua sessão. Recarregue a página.")
        st.stop()

if not copilot_session_token:
    st.markdown(
        '<div class="vt-welcome"><div class="vt-kicker">'
        '◆ &nbsp; ACESSO PRIVADO</div>'
        '<h2>Entre para continuar.</h2>'
        '<p>A Vetra está configurada para uso pessoal e aceita somente a conta '
        'GitHub autorizada.</p></div>',
        unsafe_allow_html=True,
    )
    if st.session_state.get("github_auth_error"):
        st.error(st.session_state.github_auth_error)
    if not auth_status.get("configured"):
        st.error(
            "O login ainda não foi configurado no backend. Configure "
            "`GITHUB_CLIENT_ID`, `GITHUB_CLIENT_SECRET`, "
            "`VETRA_GITHUB_REDIRECT_URI` e `VETRA_GITHUB_ALLOWED_USER` "
            "como secrets/variáveis no Fly.io."
        )
    else:
        try:
            resposta_login = requests.get(
                f"{API_BASE_URL}/auth/github/start",
                timeout=HTTP_TIMEOUT,
            )
            resposta_login.raise_for_status()
            st.link_button(
                "Entrar com GitHub",
                resposta_login.json()["authorization_url"],
                icon=":material/login:",
                width="stretch",
            )
        except (requests.RequestException, ValueError, KeyError) as erro:
            logger.warning("Não foi possível iniciar o login GitHub: %s", erro)
            st.error("Não foi possível iniciar o login. Tente novamente.")
    st.stop()

if st.session_state.get("provider_model_cache") is None:
    st.session_state.provider_model_cache = {
        "copilot_model": auth_status.get("copilot_model", "gpt-5.4"),
        "copilot_max_ai_credits_per_request": auth_status.get(
            "copilot_max_ai_credits_per_request", 5
        ),
    }


with st.sidebar:
    st.markdown(
        '<div class="vt-brand"><div class="vt-mark">V</div><div>'
        '<span class="vt-eyebrow">DATA INTELLIGENCE</span>'
        '<h1>Vetra</h1><p>Seu espaço de análise</p></div></div>',
        unsafe_allow_html=True,
    )
    st.badge(
        f"GitHub conectado · {github_login}",
        icon=":material/circle:",
        color="green",
    )
    if st.button(
        "Sair da conta GitHub",
        icon=":material/logout:",
        type="tertiary",
        width="stretch",
    ):
        try:
            requests.post(
                f"{API_BASE_URL}/auth/github/logout",
                headers={"Authorization": f"Bearer {copilot_session_token}"},
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException:
            logger.warning("Não foi possível encerrar a sessão no backend.")
        st.session_state.pop("copilot_session_token", None)
        st.session_state.pop("github_login", None)
        st.rerun()

    registros_feedback = st.session_state.feedback_fine_tuning
    if st.session_state.feedback_notice:
        st.toast(st.session_state.feedback_notice, icon=":material/check_circle:")
        st.session_state.feedback_notice = None

    with st.expander("Ações rápidas", icon=":material/bolt:", expanded=False):
        for rotulo, texto in [
            ("Consultar OTIF", "Calcular indicador OTIF acumulado da empresa"),
            ("Explorar tb_producao", "Descrever estrutura da tabela tb_producao"),
            ("Ver cotação de moedas", "Consultar cotação do Dólar e Euro atual"),
        ]:
            if st.button(rotulo, width="stretch"):
                st.session_state.pending_prompt = texto
                st.rerun()

    with st.expander("Adicionar conhecimento", icon=":material/database:", expanded=False):
        extensoes_rag = sorted(
            extension.lstrip(".")
            for extension in SUPPORTED_UPLOAD_EXTENSIONS
            if extension not in CHAT_ONLY_EXTENSIONS
        )
        arquivo_uploaded = st.file_uploader(
            "Documento para indexar",
            type=extensoes_rag,
            key="uploader_rag",
            help="Textos, PDF, Office, dados tabulares e imagens com texto.",
        )
        st.caption(
            "O texto extraído será enviado ao Qdrant. Evite dados sem autorização."
        )
        cat_input = st.text_input("Categoria", value="Documentação técnica")
        if st.button(
            "Indexar documento",
            icon=":material/upload:",
            width="stretch",
        ) and arquivo_uploaded:
            try:
                if arquivo_uploaded.size > MAX_UPLOAD_BYTES:
                    raise ValueError(
                        "O arquivo excede o limite de "
                        f"{MAX_UPLOAD_BYTES // (1024 * 1024)} MB."
                    )
                conteudo_texto = extrair_texto_upload(arquivo_uploaded).strip()
                if not conteudo_texto:
                    st.warning("O arquivo não contém texto extraível.")
                else:
                    if len(conteudo_texto) > MAX_TEXTO_RAG:
                        st.warning(
                            "O conteúdo foi limitado aos primeiros "
                            f"{MAX_TEXTO_RAG:,} caracteres."
                        )
                        conteudo_texto = conteudo_texto[:MAX_TEXTO_RAG]
                    fonte_segura = os.path.basename(arquivo_uploaded.name).replace(
                        "'", ""
                    )
                    categoria_segura = cat_input.strip().replace("'", "") or "Geral"
                    st.session_state.pending_prompt = (
                        "Por favor, use a ferramenta "
                        "'indexar_documento_com_chunking' para salvar o seguinte "
                        f"texto com a fonte '{fonte_segura}' e categoria "
                        f"'{categoria_segura}':\n\n{conteudo_texto}"
                    )
                    st.rerun()
            except Exception as erro:
                st.error(f"Não foi possível ler o arquivo: {erro}")

    with st.expander("Preferências", icon=":material/tune:", expanded=False):
        provedores = (
            ["GitHub Copilot", "Gemini"]
            if auth_status.get("gemini_configured")
            else ["GitHub Copilot"]
        )
        provider_labels = {
            "GitHub Copilot": "copilot",
            "Gemini": "gemini",
        }
        provider_atual = st.session_state.preferencias_usuario.get(
            "provider", "copilot"
        )
        provider_padrao = next(
            (
                rotulo
                for rotulo, valor in provider_labels.items()
                if valor == provider_atual and rotulo in provedores
            ),
            provedores[0],
        )
        st.session_state.preferencias_usuario["provider"] = provider_labels[
            st.selectbox(
                "Provedor de IA",
                provedores,
                index=provedores.index(provider_padrao),
            )
        ]
        if st.session_state.preferencias_usuario["provider"] == "copilot":
            st.caption(
                f"Modelo: `{st.session_state.provider_model_cache['copilot_model']}` · "
                f"limite suave de "
                f"{st.session_state.provider_model_cache['copilot_max_ai_credits_per_request']} "
                "AI Credits por consulta."
            )
        dialetos = ["PostgreSQL", "Databricks SQL", "MySQL", "BigQuery", "Oracle"]
        atual = st.session_state.preferencias_usuario.get(
            "dialeto_sql", "PostgreSQL"
        )
        st.session_state.preferencias_usuario["dialeto_sql"] = st.selectbox(
            "Dialeto SQL",
            dialetos,
            index=dialetos.index(atual) if atual in dialetos else 0,
        )
        st.session_state.preferencias_usuario["mascarar_pii"] = st.toggle(
            "Mascarar dados pessoais",
            value=bool(
                st.session_state.preferencias_usuario.get("mascarar_pii", True)
            ),
            help="Mascara CPFs, e-mails, telefones e nomes na visualização.",
        )
        perfis = [
            "Engenheiro de Dados (Preciso)",
            "Analista de BI (Consultivo)",
            "Auditor de Governança (Estrito)",
        ]
        perfil_atual = st.session_state.preferencias_usuario.get("perfil", perfis[0])
        perfil = st.selectbox(
            "Perfil de resposta",
            perfis,
            index=perfis.index(perfil_atual) if perfil_atual in perfis else 0,
        )
        st.session_state.preferencias_usuario["perfil"] = perfil
        st.session_state.preferencias_usuario["temperatura"] = {
            perfis[0]: 0.1,
            perfis[1]: 0.3,
            perfis[2]: 0.0,
        }[perfil]

    with st.expander("Ferramentas", icon=":material/build:", expanded=False):
        st.caption("Ferramentas disponíveis para consultas e análises.")
        for ferramenta in CATALOGO_FERRAMENTAS:
            st.markdown(
                f"**`{ferramenta['nome']}`**  \n{ferramenta['descricao']}"
            )

    painel_feedback = st.expander(
        "Feedback curado",
        icon=":material/rate_review:",
        expanded=False,
        on_change="rerun",
    )
    if painel_feedback.open:
        with painel_feedback:
            st.caption(
                f"{len(registros_feedback)} exemplo(s). "
                "Nenhum treinamento é iniciado automaticamente."
            )
            if registros_feedback:
                st.warning(
                    "Revise dados pessoais ou confidenciais antes de compartilhar."
                )
                st.download_button(
                    "Baixar dataset JSONL",
                    data=exportar_jsonl(registros_feedback),
                    file_name=f"vetra_feedback_{datetime.now():%Y%m%d_%H%M%S}.jsonl",
                    mime="application/x-ndjson",
                    key="download_feedback_jsonl",
                    width="stretch",
                )

    quantidade_mensagens = len(st.session_state.messages)
    st.caption(
        f"{quantidade_mensagens} "
        f"{'mensagem' if quantidade_mensagens == 1 else 'mensagens'} · "
        f"{st.session_state.total_chamadas_mcp} chamadas de ferramenta · "
        f"último modelo: {st.session_state.ultimo_modelo or '—'}"
    )
    if st.button(
        "Limpar conversa",
        icon=":material/delete_outline:",
        type="tertiary",
        width="stretch",
    ):
        st.session_state.messages = [{"role": "model", "content": "Conversa reiniciada. Em que posso ajudar agora?"}]
        st.session_state.ultimo_modelo = None
        st.session_state.total_chamadas_mcp = 0
        st.session_state.pending_prompt = None
        st.session_state.pop("vetra_suggestion", None)
        st.rerun()


st.markdown(
    '<div class="vt-welcome"><div class="vt-kicker">'
    '◆ &nbsp; CONSULTORIA DE DADOS COM IA</div>'
    '<h2>Dados mais claros.<br>Decisões melhores.</h2>'
    '<p>Explore esquemas, valide regras de negócio e transforme perguntas '
    "em análises e SQL com apoio da Vetra.</p></div>",
    unsafe_allow_html=True,
)

sugestao_selecionada = None
if len(st.session_state.messages) == 1:
    st.markdown(
        '<div class="vt-section-label">Comece por aqui</div>',
        unsafe_allow_html=True,
    )
    sugestao_selecionada = st.pills(
        "Sugestões de consulta",
        SUGESTOES_INICIAIS,
        selection_mode="single",
        label_visibility="collapsed",
        width="stretch",
        key="vetra_suggestion",
    )
    if sugestao_selecionada:
        prompt_sugerido = sugestao_selecionada
    else:
        prompt_sugerido = None
else:
    prompt_sugerido = None

AVATAR_USUARIO, AVATAR_MODELO = ":material/person:", ":material/auto_awesome:"
feedback_por_exchange = {
    registro["exchange_id"]: registro
    for registro in st.session_state.feedback_fine_tuning
}
for mensagem in st.session_state.messages:
    role = "assistant" if mensagem.get("role") == "model" else mensagem.get("role", "user")
    with st.chat_message(role, avatar=AVATAR_MODELO if role == "assistant" else AVATAR_USUARIO):
        st.markdown(str(mensagem.get("content", "")))
        exchange_id = mensagem.get("exchange_id") if role == "assistant" else None
        if exchange_id:
            registro_existente = feedback_por_exchange.get(exchange_id)
            if registro_existente:
                st.caption(
                    "Feedback salvo: "
                    + ("👍 útil" if registro_existente["avaliacao"] == "positive" else "👎 corrigido")
                )
            else:
                with st.expander("Ajudar a melhorar a Vetra"):
                    avaliacao_ui = st.feedback(
                        "thumbs",
                        key=f"rating_{exchange_id}",
                    )
                    resposta_corrigida = st.text_area(
                        "Resposta ideal (obrigatória para avaliação negativa)",
                        key=f"correcao_{exchange_id}",
                        max_chars=100_000,
                    )
                    if st.button(
                        "Salvar feedback para curadoria",
                        key=f"salvar_feedback_{exchange_id}",
                        disabled=(
                            len(st.session_state.feedback_fine_tuning)
                            >= MAX_FEEDBACK_EXAMPLES
                        ),
                    ):
                        if avaliacao_ui is None:
                            st.warning("Selecione 👍 ou 👎 antes de salvar.")
                        else:
                            try:
                                registro = criar_registro_feedback(
                                    exchange_id=exchange_id,
                                    prompt=str(mensagem.get("prompt", "")),
                                    resposta=str(mensagem.get("content", "")),
                                    avaliacao="positive" if avaliacao_ui == 1 else "negative",
                                    resposta_corrigida=resposta_corrigida,
                                    dialeto_sql=str(mensagem.get("dialeto_sql", "")),
                                    modelo=mensagem.get("modelo_usado"),
                                    anexos=list(mensagem.get("anexos", [])),
                                )
                                st.session_state.feedback_fine_tuning.append(registro)
                                st.session_state.feedback_notice = (
                                    "Feedback incluído no dataset desta sessão."
                                )
                                st.rerun()
                            except ValueError as erro:
                                st.warning(str(erro))
                if (
                    len(st.session_state.feedback_fine_tuning)
                    >= MAX_FEEDBACK_EXAMPLES
                ):
                    st.caption(
                        f"Limite de {MAX_FEEDBACK_EXAMPLES} exemplos nesta sessão."
                    )

envio_chat = st.chat_input(
    "Pergunte sobre seus dados ou anexe arquivos…",
    accept_file="multiple",
    file_type=sorted(
        extension.lstrip(".") for extension in SUPPORTED_UPLOAD_EXTENSIONS
    ),
    max_upload_size=max(1, MAX_UPLOAD_BYTES // (1024 * 1024)),
    submit_mode="disable",
    key="vetra_chat_input",
)
if envio_chat is None:
    prompt = None
    arquivos_anexados = []
elif isinstance(envio_chat, str):
    prompt = envio_chat
    arquivos_anexados = []
else:
    prompt = str(getattr(envio_chat, "text", "") or "")
    arquivos_anexados = list(getattr(envio_chat, "files", []) or [])

if st.session_state.pending_prompt and not prompt:
    prompt = st.session_state.pending_prompt
    st.session_state.pending_prompt = None
elif not prompt and prompt_sugerido:
    prompt = prompt_sugerido

anexos_payload = []
erros_anexos = []
if arquivos_anexados:
    if len(arquivos_anexados) > MAX_ATTACHMENTS:
        erros_anexos.append(
            f"Envie no máximo {MAX_ATTACHMENTS} anexos por consulta."
        )
    tamanho_total = sum(arquivo.size for arquivo in arquivos_anexados)
    if tamanho_total > MAX_TOTAL_UPLOAD_BYTES:
        erros_anexos.append(
            "O tamanho total dos anexos excede "
            f"{MAX_TOTAL_UPLOAD_BYTES // (1024 * 1024)} MB."
        )
    if not erros_anexos:
        for arquivo in arquivos_anexados:
            try:
                anexos_payload.append(preparar_anexo(arquivo))
            except Exception as erro:
                erros_anexos.append(str(erro))
for erro in erros_anexos:
    st.error(erro)

if (prompt or arquivos_anexados) and not erros_anexos:
    prompt_texto = prompt or "Analise o(s) arquivo(s) em anexo."
    if arquivos_anexados:
        prompt_texto += "\n\n📎 *Anexo(s): " + ", ".join(os.path.basename(a.name) for a in arquivos_anexados) + "*"

    st.session_state.messages.append({"role": "user", "content": prompt_texto})
    with st.chat_message("user", avatar=AVATAR_USUARIO):
        st.markdown(prompt_texto)

    historico_copia = list(st.session_state.messages)
    dialeto_atual = st.session_state.preferencias_usuario.get("dialeto_sql", "PostgreSQL")
    temp_atual = st.session_state.preferencias_usuario.get("temperatura", 0.2)

    with st.chat_message("assistant", avatar=AVATAR_MODELO):
        resposta_stream = st.empty()
        estado_stream = {
            "texto": "",
            "caracteres_pendentes": 0,
            "ultima_atualizacao": time.monotonic(),
            "houve_renderizacao": False,
            "uso": None,
        }
        with st.status(
            ":material/auto_awesome: Consultando seus dados",
            type="compact",
            expanded=False,
        ) as status_box:
            status_box.write("Conectando às ferramentas e ao modelo…")

            def atualizar_progresso(evento: dict[str, Any]) -> None:
                tipo_evento = evento.get("type")
                if tipo_evento == "status":
                    status_box.write(str(evento.get("message", "Processando...")))
                elif tipo_evento == "model":
                    status_box.write(
                        f":material/psychology: Consultando `{evento.get('name')}`…"
                    )
                elif tipo_evento == "tool_start":
                    status_box.write(
                        f":material/build: Executando `{evento.get('name')}` "
                        f"(etapa {evento.get('step')}/{evento.get('total_steps')})…"
                    )
                elif tipo_evento == "tool_done":
                    icone = (
                        ":material/warning:"
                        if evento.get("error")
                        else ":material/check_circle:"
                    )
                    status_box.write(
                        f"{icone} Ferramenta `{evento.get('name')}` concluída."
                    )
                elif tipo_evento == "reset":
                    estado_stream["texto"] = ""
                    estado_stream["caracteres_pendentes"] = 0
                    estado_stream["ultima_atualizacao"] = time.monotonic()
                    estado_stream["houve_renderizacao"] = False
                    resposta_stream.empty()
                elif tipo_evento == "token":
                    trecho = str(evento.get("text", ""))
                    estado_stream["texto"] += trecho
                    estado_stream["caracteres_pendentes"] += len(trecho)
                    agora = time.monotonic()
                    if (
                        not estado_stream["houve_renderizacao"]
                        or estado_stream["caracteres_pendentes"] >= 64
                        or agora - estado_stream["ultima_atualizacao"] >= 0.15
                    ):
                        resposta_stream.markdown(estado_stream["texto"])
                        estado_stream["caracteres_pendentes"] = 0
                        estado_stream["ultima_atualizacao"] = agora
                        estado_stream["houve_renderizacao"] = True
                elif tipo_evento == "usage":
                    creditos = float(evento.get("ai_credits", 0) or 0)
                    estado_stream["uso"] = {
                        "ai_credits": creditos,
                        "input_tokens": int(evento.get("input_tokens", 0) or 0),
                        "output_tokens": int(evento.get("output_tokens", 0) or 0),
                    }
                    st.session_state.copilot_last_ai_credits = creditos
                    st.session_state.total_copilot_ai_credits += creditos

            resposta, modelo_usado, mcp_chamado, retorno_mcp = chamar_api_backend(
                prompt_texto,
                historico_copia,
                dialeto_atual,
                temp_atual,
                st.session_state.preferencias_usuario.get("provider", "copilot"),
                copilot_session_token,
                anexos_payload,
                on_event=atualizar_progresso,
            )
            resposta = str(resposta or "Sem resposta do backend.")
            estado_erro = resposta.startswith(("❌", "⚠️ Erro:"))
            status_box.update(
                label=(
                    "Falha ao processar a consulta"
                    if estado_erro
                    else "Resposta pronta"
                ),
                state="error" if estado_erro else "complete",
                expanded=estado_erro,
            )
        if estado_stream["uso"] is not None:
            st.caption(
                f"Esta consulta: {estado_stream['uso']['ai_credits']:.6f} "
                "GitHub AI Credits · "
                f"{estado_stream['uso']['input_tokens']:,} tokens de entrada · "
                f"{estado_stream['uso']['output_tokens']:,} tokens de saída · "
                f"total nesta sessão: "
                f"{st.session_state.total_copilot_ai_credits:.6f} credits"
            )

        if resposta.startswith("❌"):
            resposta_stream.error(resposta, icon=":material/error:")
        elif resposta.startswith("⚠️ Erro:"):
            resposta_stream.warning(
                resposta.removeprefix("⚠️ Erro:").strip(),
                title="Configuração necessária",
                icon=":material/warning:",
            )
        else:
            resposta_stream.markdown(resposta)
        bloco_sql = re.search(r"```sql\s*(.*?)```", resposta, flags=re.IGNORECASE | re.DOTALL)
        if bloco_sql:
            sql_code = bloco_sql.group(1).strip()
            for alerta in analisar_explain_plan_sql(sql_code):
                st.info(alerta)
            with st.expander("📋 Ver Query SQL em destaque para copiar/baixar"):
                st.code(sql_code, language="sql")
                st.download_button("💾 Baixar Query (.sql)", sql_code, file_name=f"query_vetra_{datetime.now():%H%M%S}.sql", mime="text/plain", key=f"dl_sql_{len(st.session_state.messages)}")

        if modelo_usado:
            st.caption(f"Respondido por `{modelo_usado}` · {datetime.now():%H:%M}")

        if retorno_mcp:
            retorno_texto = retorno_mcp if isinstance(retorno_mcp, str) else json.dumps(retorno_mcp, ensure_ascii=False, default=str)
            bloco_json = re.search(r"```(?:json)?\s*(.*?)```", retorno_texto, flags=re.IGNORECASE | re.DOTALL)
            json_str = bloco_json.group(1).strip() if bloco_json else retorno_texto.strip()
            try:
                dados = json.loads(json_str)
                if isinstance(dados, dict) and dados.get("status") == "sucesso":
                    st.markdown("#### 🤖 Painel de Inteligência de Machine Learning")
                    if dados.get("previsoes") is not None and dados.get("historico_recente") is not None:
                        df_hist = pd.DataFrame(dados["historico_recente"]).rename(columns={"valor_historico": "Valor"})
                        df_pred = pd.DataFrame(dados["previsoes"]).rename(columns={"valor_previsto": "Valor"})
                        if {"data", "Valor"}.issubset(df_hist.columns):
                            df_hist["Tipo"] = "Histórico"
                        if {"data", "Valor"}.issubset(df_pred.columns):
                            df_pred["Tipo"] = "Projeção ML"
                        frames = [df for df in (df_hist, df_pred) if {"data", "Valor", "Tipo"}.issubset(df.columns)]
                        if frames:
                            combinado = pd.concat(frames, ignore_index=True)
                            combinado["Valor"] = pd.to_numeric(combinado["Valor"], errors="coerce")
                            combinado = combinado.dropna(subset=["Valor"])
                            if not combinado.empty:
                                figura = px.line(combinado, x="data", y="Valor", color="Tipo", markers=True, title="📈 Projeção Tendencial e Séries Temporais", color_discrete_map={"Histórico": "#45C4B0", "Projeção ML": "#E8A945"})
                                st.plotly_chart(figura, width="stretch")
                    elif "total_anomalias_encontradas" in dados:
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Linhas Analisadas", dados.get("total_linhas_analisadas", 0))
                        c2.metric("Anomalias Detectadas", dados.get("total_anomalias_encontradas", 0))
                        c3.metric("Taxa de Contaminação", f"{dados.get('percentual_anomalias', 0)}%")
                        if dados.get("amostra_anomalias_detectadas"):
                            st.dataframe(
                                pd.DataFrame(dados["amostra_anomalias_detectadas"]),
                                width="stretch",
                            )
                elif isinstance(dados, dict) and isinstance(dados.get("linhas"), list) and isinstance(dados.get("colunas"), list):
                    df_bruto = pd.DataFrame(dados["linhas"], columns=dados["colunas"])
                    alertas = executar_sanity_check_df(df_bruto)
                    if alertas:
                        with st.expander("🛡️ Relatório de Qualidade de Dados", expanded=True):
                            for alerta in alertas:
                                st.warning(alerta)
                    df_exibicao, mascaradas = aplicar_mascaramento_pii(df_bruto) if st.session_state.preferencias_usuario.get("mascarar_pii", True) else (df_bruto, [])
                    if mascaradas:
                        st.caption("🔒 LGPD / PII Masking Ativo: " + ", ".join(mascaradas))
                    st.markdown("#### 📊 Painel de Análise e Visualização de Dados")
                    col_df, col_chart = st.columns([1, 1])
                    with col_df:
                        st.dataframe(df_exibicao, width="stretch")
                        st.download_button("📥 Baixar Dados da Tabela (.csv)", df_exibicao.to_csv(index=False).encode("utf-8-sig"), file_name=f"dados_vetra_{datetime.now():%H%M%S}.csv", mime="text/csv", key=f"dl_csv_{len(st.session_state.messages)}")
                    with col_chart:
                        numericas = list(df_exibicao.select_dtypes(include="number").columns)
                        if not df_exibicao.empty and numericas:
                            eixo_x = st.selectbox("Eixo X", list(df_exibicao.columns), key=f"chart_x_{len(st.session_state.messages)}")
                            eixo_y = st.selectbox("Eixo Y", numericas, key=f"chart_y_{len(st.session_state.messages)}")
                            st.plotly_chart(
                                px.bar(
                                    df_exibicao,
                                    x=eixo_x,
                                    y=eixo_y,
                                    title=f"{eixo_y} por {eixo_x}",
                                ),
                                width="stretch",
                            )
            except (json.JSONDecodeError, ValueError, TypeError) as erro:
                st.warning(f"Não foi possível renderizar a visualização tabular/gráfica: {erro}")

    if mcp_chamado:
        st.session_state.total_chamadas_mcp += 1
    if modelo_usado:
        st.session_state.ultimo_modelo = modelo_usado
    st.session_state.messages.append(
        {
            "role": "model",
            "content": resposta,
            "exchange_id": str(uuid.uuid4()),
            "prompt": prompt_texto,
            "dialeto_sql": dialeto_atual,
            "modelo_usado": modelo_usado,
            "anexos": [
                anexo.get("nome", "")
                for anexo in anexos_payload
            ],
        }
    )
