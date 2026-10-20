import asyncio
import json
import logging
import os
import re
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
    initial_sidebar_state="expanded",
)

RAW_API_URL = os.environ.get("VETRA_API_URL", "http://127.0.0.1:8000").strip()
RAW_API_URL = RAW_API_URL.replace("0.0.0.0", "127.0.0.1")
API_BASE_URL = re.sub(r"/(chat|chat/stream)/?$", "", RAW_API_URL).rstrip("/")

PRECO_INPUT_1M = 0.075
PRECO_OUTPUT_1M = 0.30
TAXA_CAMBIO_USD_BRL = 5.60
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

MODELOS_PREFERENCIA = [
    "gemini-3.8-flash",
    "gemini-3.1-pro-preview",
    "gemini-3-flash-preview",
]

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


def obter_chave_gemini_segura() -> str:
    valor_env = os.environ.get("GEMINI_API_KEY")
    if valor_env:
        return valor_env
    try:
        return str(st.secrets.get("GEMINI_API_KEY", ""))
    except Exception:
        return ""


if "preferencias_usuario" not in st.session_state:
    st.session_state.preferencias_usuario = {
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


try:
    from utils.meu_agente_dados.main import processar_mcp_e_llm
    HAS_LOCAL_ORCHESTRATOR = True
except Exception:
    processar_mcp_e_llm = None
    HAS_LOCAL_ORCHESTRATOR = False
    logger.exception("Não foi possível carregar o orquestrador local da Vetra.")


def executar_corrotina(corrotina):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(corrotina)
    raise RuntimeError("Já existe um loop assíncrono ativo; use o backend HTTP para esta execução.")


def chamar_api_backend(
    prompt: str,
    historico: list,
    dialeto_sql: str,
    temperatura: float,
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
        "dialeto_sql": dialeto_sql,
        "temperatura": max(0.0, min(2.0, float(temperatura))),
    }
    erros_http = []
    stream_iniciado = False
    stream_concluido = False

    try:
        with requests.post(
            f"{API_BASE_URL}/chat/stream",
            json=payload,
            headers={"Accept": "text/event-stream", "Content-Type": "application/json"},
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
            headers={"Content-Type": "application/json"},
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

    if HAS_LOCAL_ORCHESTRATOR and processar_mcp_e_llm is not None:
        try:
            async def publicar_evento_local(evento: dict[str, Any]) -> None:
                if on_event is not None:
                    on_event(evento)

            resposta, modelo_usado, mcp_chamado, retorno_mcp = executar_corrotina(
                processar_mcp_e_llm(
                    prompt,
                    historico_formatado,
                    dialeto_sql,
                    temperatura,
                    anexos or [],
                    on_event=publicar_evento_local,
                )
            )
            return str(resposta), f"{modelo_usado} (In-Memory)", bool(mcp_chamado), retorno_mcp
        except Exception as erro:
            return f"❌ Erro na execução nativa do agente: {erro}", None, False, None

    detalhe = "; ".join(erros_http[-2:])
    sufixo = f" Detalhes: {detalhe}" if detalhe else ""
    return f"❌ Não foi possível conectar ao servidor backend da Vetra ({API_BASE_URL}).{sufixo}", None, False, None


st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');
:root { --bg-deep:#0A0F1C; --bg-panel:#0F1626; --bg-surface:#141D33; --border-subtle:#223050; --border-strong:#2E4270; --accent:#45C4B0; --accent-soft:rgba(69,196,176,.14); --text-primary:#E9EEF7; --text-muted:#8496B8; --text-faint:#5A6C8C; }
html,body,[class*="css"]{font-family:'Inter',-apple-system,sans-serif}
[data-testid="stAppViewContainer"]{background:radial-gradient(circle at 15% 0%,rgba(69,196,176,.06),transparent 40%),radial-gradient(circle at 85% 100%,rgba(232,169,69,.05),transparent 40%),var(--bg-deep)}
[data-testid="stHeader"]{background:transparent} footer,#MainMenu{visibility:hidden}
[data-testid="stSidebar"]{background:var(--bg-panel);border-right:1px solid var(--border-subtle)} [data-testid="stSidebar"] *{color:var(--text-primary)}
div[data-baseweb="input"]>div,div[data-baseweb="select"]>div,input,textarea,[data-testid="stChatInput"] textarea{background-color:var(--bg-surface)!important;color:var(--text-primary)!important;border-color:var(--border-strong)!important}
.vt-card{background:var(--bg-surface);border:1px solid var(--border-subtle);border-radius:10px;padding:14px 16px;margin-bottom:10px}.vt-card-title{font-size:.72rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--text-faint);margin-bottom:6px}.vt-tool-row{display:flex;gap:8px;padding:6px 0;border-bottom:1px solid var(--border-subtle);font-size:.82rem}.vt-tool-name{font-family:'JetBrains Mono',monospace;color:var(--accent);font-size:.76rem}.vt-tool-desc{color:var(--text-muted)}
.vt-pill{display:inline-flex;align-items:center;gap:6px;background:var(--accent-soft);border:1px solid rgba(69,196,176,.35);color:var(--accent);font-size:.78rem;padding:4px 12px;border-radius:999px}.vt-dot{width:6px;height:6px;border-radius:50%;background:var(--accent)}.vt-hero{display:flex;align-items:center;gap:14px;padding:4px 0 2px}.vt-mark{width:42px;height:42px;border-radius:11px;background:linear-gradient(135deg,var(--accent),#2E8C7E);display:flex;align-items:center;justify-content:center;font-family:'Space Grotesk',sans-serif;font-weight:700;color:#06120F;font-size:1.15rem}[data-testid="stChatMessage"]{background:var(--bg-surface);border:1px solid var(--border-subtle);border-radius:12px}.vt-sep{border-top:1px solid var(--border-subtle);margin:14px 0}
</style>
""", unsafe_allow_html=True)


with st.sidebar:
    st.markdown('<div class="vt-hero"><div class="vt-mark">V</div><div><h1 style="font-size:1.15rem;">Vetra</h1><p style="font-size:0.78rem;">Agente Consultivo de Dados</p></div></div>', unsafe_allow_html=True)
    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)
    status_texto = "Chave detectada" if obter_chave_gemini_segura() else "API Backend configurada"
    st.markdown(f'<span class="vt-pill"><span class="vt-dot"></span>{status_texto}</span>', unsafe_allow_html=True)
    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)

    st.markdown('<div class="vt-card-title">Feedback e dataset de fine-tuning</div>', unsafe_allow_html=True)
    registros_feedback = st.session_state.feedback_fine_tuning
    st.caption(
        f"{len(registros_feedback)} exemplo(s) revisado(s). "
        "Nenhum treinamento é iniciado automaticamente."
    )
    if registros_feedback:
        st.warning(
            "O JSONL contém prompts e respostas. Revise dados pessoais ou "
            "confidenciais antes de compartilhar."
        )
        st.download_button(
            "Baixar dataset JSONL",
            data=exportar_jsonl(registros_feedback),
            file_name=f"vetra_feedback_{datetime.now():%Y%m%d_%H%M%S}.jsonl",
            mime="application/x-ndjson",
            key="download_feedback_jsonl",
        )
    if st.session_state.feedback_notice:
        st.success(st.session_state.feedback_notice)
        st.session_state.feedback_notice = None

    with st.expander("⚡ Prompts Rápidos", expanded=False):
        for rotulo, texto in [
            ("📊 Consultar OTIF Geral", "Calcular indicador OTIF acumulado da empresa"),
            ("🗄️ Detalhar tb_producao", "Descrever estrutura da tabela tb_producao"),
            ("💲 Cotação Moedas Hoje", "Consultar cotação do Dólar e Euro atual"),
        ]:
            if st.button(rotulo, use_container_width=True):
                st.session_state.pending_prompt = texto
                st.rerun()

    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)
    st.markdown('<div class="vt-card-title">Ingestão RAG (Qdrant)</div>', unsafe_allow_html=True)
    extensoes_rag = sorted(
        extension.lstrip(".")
        for extension in SUPPORTED_UPLOAD_EXTENSIONS
        if extension not in CHAT_ONLY_EXTENSIONS
    )
    arquivo_uploaded = st.file_uploader(
        "Carregar documento ou imagem para indexação",
        type=extensoes_rag,
        key="uploader_rag",
        help="PDF, Office, código, dados tabulares e imagens com texto (OCR disponível no servidor).",
    )
    st.caption(
        "A indexação envia o texto extraído ao Qdrant configurado. "
        "Não inclua dados sem autorização."
    )
    cat_input = st.text_input("Categoria/Tag", value="Documentação Técnica")
    if st.button("Indexar Arquivo no RAG", use_container_width=True) and arquivo_uploaded:
        try:
            if arquivo_uploaded.size > MAX_UPLOAD_BYTES:
                raise ValueError(f"O arquivo excede o limite de {MAX_UPLOAD_BYTES // (1024 * 1024)} MB.")
            conteudo_texto = extrair_texto_upload(arquivo_uploaded).strip()
            if not conteudo_texto:
                st.warning("O arquivo não contém texto extraível.")
            else:
                if len(conteudo_texto) > MAX_TEXTO_RAG:
                    st.warning(f"O conteúdo foi limitado aos primeiros {MAX_TEXTO_RAG:,} caracteres.")
                    conteudo_texto = conteudo_texto[:MAX_TEXTO_RAG]
                fonte_segura = os.path.basename(arquivo_uploaded.name).replace("'", "")
                categoria_segura = cat_input.strip().replace("'", "") or "Geral"
                st.session_state.pending_prompt = f"Por favor, use a ferramenta 'indexar_documento_com_chunking' para salvar o seguinte texto com a fonte '{fonte_segura}' e categoria '{categoria_segura}':\n\n{conteudo_texto}"
                st.rerun()
        except Exception as erro:
            st.error(f"Não foi possível ler o arquivo: {erro}")

    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)
    st.markdown('<div class="vt-card-title">Preferências de Memória</div>', unsafe_allow_html=True)
    dialetos = ["PostgreSQL", "Databricks SQL", "MySQL", "BigQuery", "Oracle"]
    atual = st.session_state.preferencias_usuario.get("dialeto_sql", "PostgreSQL")
    st.session_state.preferencias_usuario["dialeto_sql"] = st.selectbox("Dialeto SQL Alvo", dialetos, index=dialetos.index(atual) if atual in dialetos else 0)
    st.session_state.preferencias_usuario["mascarar_pii"] = st.toggle("Mascaramento de PII", value=bool(st.session_state.preferencias_usuario.get("mascarar_pii", True)), help="Mascara automaticamente CPFs, E-mails e Nomes na exibição.")
    perfis = ["Engenheiro de Dados (Preciso)", "Analista de BI (Consultivo)", "Auditor de Governança (Estrito)"]
    perfil_atual = st.session_state.preferencias_usuario.get("perfil", perfis[0])
    perfil = st.selectbox("Modo de Operação", perfis, index=perfis.index(perfil_atual) if perfil_atual in perfis else 0)
    st.session_state.preferencias_usuario["perfil"] = perfil
    st.session_state.preferencias_usuario["temperatura"] = {perfis[0]: 0.1, perfis[1]: 0.3, perfis[2]: 0.0}[perfil]

    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)
    st.markdown('<div class="vt-card-title">Ferramentas MCP</div>', unsafe_allow_html=True)
    linhas = "".join(f'<div class="vt-tool-row"><span class="vt-tool-name">{f["nome"]}</span><span class="vt-tool-desc">— {f["descricao"]}</span></div>' for f in CATALOGO_FERRAMENTAS)
    st.markdown(f'<div class="vt-card">{linhas}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="vt-card"><div class="vt-tool-row">Mensagens trocadas</div><div>{len(st.session_state.messages)}</div><div class="vt-tool-row">Último modelo usado</div><div class="vt-tool-name">{st.session_state.ultimo_modelo or "—"}</div><div class="vt-tool-row">Chamadas MCP</div><div>{st.session_state.total_chamadas_mcp}</div></div>', unsafe_allow_html=True)
    if st.button("🗑️ Limpar conversa", use_container_width=True):
        st.session_state.messages = [{"role": "model", "content": "Conversa reiniciada. Em que posso ajudar agora?"}]
        st.session_state.ultimo_modelo = None
        st.session_state.total_chamadas_mcp = 0
        st.session_state.pending_prompt = None
        st.rerun()
    st.caption("Vetra Client — Powered by FastAPI + MCP Backend.")


st.markdown('<div class="vt-hero"><div class="vt-mark">V</div><div><h1>Vetra — Agente Consultivo de Dados e Engenharia</h1><p>Conectado à API FastAPI · Servidor MCP · Qdrant Vector Database</p></div></div>', unsafe_allow_html=True)
st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)

if len(st.session_state.messages) == 1:
    st.markdown('<div class="vt-card-title">Sugestões para começar</div>', unsafe_allow_html=True)
    for coluna, sugestao in zip(st.columns(len(SUGESTOES_INICIAIS)), SUGESTOES_INICIAIS):
        with coluna:
            if st.button(sugestao, use_container_width=True, key=f"sug_{sugestao}"):
                st.session_state.pending_prompt = sugestao
                st.rerun()

AVATAR_USUARIO, AVATAR_MODELO = "👤", "🤖"
for mensagem in st.session_state.messages:
    role = "assistant" if mensagem.get("role") == "model" else mensagem.get("role", "user")
    with st.chat_message(role, avatar=AVATAR_MODELO if role == "assistant" else AVATAR_USUARIO):
        st.markdown(str(mensagem.get("content", "")))
        exchange_id = mensagem.get("exchange_id") if role == "assistant" else None
        if exchange_id:
            registro_existente = next(
                (
                    registro
                    for registro in st.session_state.feedback_fine_tuning
                    if registro["exchange_id"] == exchange_id
                ),
                None,
            )
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

arquivos_anexados = st.file_uploader(
    "📎 Anexar documentos, imagens, código, áudio ou vídeo",
    type=sorted(extension.lstrip(".") for extension in SUPPORTED_UPLOAD_EXTENSIONS),
    accept_multiple_files=True,
    key=f"uploader_{len(st.session_state.messages)}",
    help=(
        "PDF, Office, texto/código, CSV/TSV/Parquet, PNG/JPG/WEBP, "
        "áudio e vídeo compatíveis. Limite por arquivo: "
        f"{MAX_UPLOAD_BYTES // (1024 * 1024)} MB."
    ),
)
st.caption(
    "Anexos de conversa são enviados ao provedor Gemini configurado. "
    "Evite segredos, credenciais e dados pessoais sem autorização."
)
prompt = st.chat_input("Digite sua pergunta sobre os dados, envie um print ou solicite um SQL...")
if st.session_state.pending_prompt and not prompt:
    prompt = st.session_state.pending_prompt
    st.session_state.pending_prompt = None

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
        estado_stream = {"texto": ""}
        with st.status("🔍 *Processando consulta com inteligência de dados...*", expanded=True) as status_box:
            status_box.write("1. Conectando ao backend FastAPI e orquestrador MCP...")

            def atualizar_progresso(evento: dict[str, Any]) -> None:
                tipo_evento = evento.get("type")
                if tipo_evento == "status":
                    status_box.write(str(evento.get("message", "Processando...")))
                elif tipo_evento == "model":
                    status_box.write(f"🤖 Consultando o modelo `{evento.get('name')}`...")
                elif tipo_evento == "tool_start":
                    status_box.write(
                        f"🛠️ Executando `{evento.get('name')}` "
                        f"(etapa {evento.get('step')}/{evento.get('total_steps')})..."
                    )
                elif tipo_evento == "tool_done":
                    prefixo = "⚠️" if evento.get("error") else "✅"
                    status_box.write(f"{prefixo} Ferramenta `{evento.get('name')}` concluída.")
                elif tipo_evento == "reset":
                    estado_stream["texto"] = ""
                    resposta_stream.empty()
                elif tipo_evento == "token":
                    estado_stream["texto"] += str(evento.get("text", ""))
                    resposta_stream.markdown(estado_stream["texto"])

            resposta, modelo_usado, mcp_chamado, retorno_mcp = chamar_api_backend(
                prompt_texto,
                historico_copia,
                dialeto_atual,
                temp_atual,
                anexos_payload,
                on_event=atualizar_progresso,
            )
            resposta = str(resposta or "Sem resposta do backend.")
            status_box.write("2. Sintetizando resposta final e validando regras...")
            estado_erro = resposta.startswith("❌")
            status_box.update(label="❌ Falha no processamento" if estado_erro else "✅ Processamento concluído com sucesso!", state="error" if estado_erro else "complete", expanded=estado_erro)

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
                                st.plotly_chart(figura, use_container_width=True)
                    elif "total_anomalias_encontradas" in dados:
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Linhas Analisadas", dados.get("total_linhas_analisadas", 0))
                        c2.metric("Anomalias Detectadas", dados.get("total_anomalias_encontradas", 0))
                        c3.metric("Taxa de Contaminação", f"{dados.get('percentual_anomalias', 0)}%")
                        if dados.get("amostra_anomalias_detectadas"):
                            st.dataframe(pd.DataFrame(dados["amostra_anomalias_detectadas"]), use_container_width=True)
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
                        st.dataframe(df_exibicao, use_container_width=True)
                        st.download_button("📥 Baixar Dados da Tabela (.csv)", df_exibicao.to_csv(index=False).encode("utf-8-sig"), file_name=f"dados_vetra_{datetime.now():%H%M%S}.csv", mime="text/csv", key=f"dl_csv_{len(st.session_state.messages)}")
                    with col_chart:
                        numericas = list(df_exibicao.select_dtypes(include="number").columns)
                        if not df_exibicao.empty and numericas:
                            eixo_x = st.selectbox("Eixo X", list(df_exibicao.columns), key=f"chart_x_{len(st.session_state.messages)}")
                            eixo_y = st.selectbox("Eixo Y", numericas, key=f"chart_y_{len(st.session_state.messages)}")
                            st.plotly_chart(px.bar(df_exibicao, x=eixo_x, y=eixo_y, title=f"{eixo_y} por {eixo_x}"), use_container_width=True)
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
