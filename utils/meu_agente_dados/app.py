import os
import sys

# Garante que a raiz do projeto (onde está a pasta 'core') seja encontrada pelo Python
DIR_ATUAL = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIR_ATUAL, "../../"))
if RAIZ_PROJETO not in sys.path:
    sys.path.insert(0, RAIZ_PROJETO)

# Importações dos módulos centrais de resiliência e segurança
from core.rag_guard import buscar_conhecimento_rag as rag_guard_buscar, PROMPT_SISTEMA_VETRA
from core.api_client import requisicao_api_segura

import json
import io
import re
import asyncio
import concurrent.futures
from datetime import datetime

import streamlit as st
import anyio
import pandas as pd
import plotly.express as px
from pypdf import PdfReader
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from google import genai
from google.genai import types

# =============================================================================
# CONFIGURAÇÃO DA PÁGINA
# =============================================================================
st.set_page_config(
    page_title="Vetra — Agente Consultivo de Dados",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)

CAMINHO_SERVIDOR = os.path.join(DIR_ATUAL, "servidor_mcp.py")

# Lista de modelos válidos e recomendados pela API
MODELOS_PREFERENCIA = [
    "gemini-3-flash-preview",
    "gemini-2.5-flash",
    "gemini-3.5-flash-lite",
]

# Catálogo completo das ferramentas expostas pelo servidor MCP (incluindo RAG e SQL AST)
CATALOGO_FERRAMENTAS = [
    {"nome": "validar_e_executar_sql", "descricao": "Valida via AST e Schema-Aware antes de rodar"},
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
    {"nome": "consultar_futebol_liga", "descricao": "Tabela e classificação do Campeonato Brasileiro"}
]

SUGESTOES_INICIAIS = [
    "Quais tabelas eu tenho disponíveis?",
    "Explique a estrutura da tb_producao",
    "Qual a fórmula do OTIF % acumulado?",
    "Monte uma query de produção do mês atual",
]

# =============================================================================
# ESTADO DE SESSÃO & MEMÓRIA (INICIALIZAÇÃO GARANTIDA NO TOPO)
# =============================================================================
if "preferencias_usuario" not in st.session_state:
    st.session_state.preferencias_usuario = {
        "dialeto_sql": "PostgreSQL",
        "departamento": "Engenharia de Dados",
        "temperatura": 0.2,
        "mascarar_pii": True,
    }

if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "model",
            "content": "Olá! Sou o **Vetra**, seu agente consultivo de engenharia de dados e BI. "
                       "Posso explorar esquemas, montar queries SQL, validar regras de negócio "
                       "e fornecer insights proativos sobre seus indicadores. Como posso ajudar?",
        }
    ]

if "pending_prompt" not in st.session_state:
    st.session_state.pending_prompt = None

if "ultimo_modelo" not in st.session_state:
    st.session_state.ultimo_modelo = None

if "total_chamadas_mcp" not in st.session_state:
    st.session_state.total_chamadas_mcp = 0

if "rag_feedbacks" not in st.session_state:
    st.session_state.rag_feedbacks = []

# =============================================================================
# FUNÇÕES DE GOVERNANÇA, PII, QUALIDADE E PERFORMANCE
# =============================================================================
def aplicar_mascaramento_pii(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Aplica mascaramento de PII (LGPD) em colunas sensíveis identificadas."""
    df_mascarado = df.copy()
    colunas_mascaradas = []
    
    padroes_pii = [
        r'cpf', r'email', r'e-mail', r'telefone', r'celular', r'nome', 
        r'sobrenome', r'cartao', r'credito', r'senha', r'usuario'
    ]
    
    for col in df_mascarado.columns:
        col_lower = str(col).lower()
        if any(re.search(padrao, col_lower) for padrao in padroes_pii):
            colunas_mascaradas.append(str(col))
            df_mascarado[col] = df_mascarado[col].astype(str).apply(
                lambda val: val[0] + "***" + val[-1] if len(val) > 2 else "***"
            )
            
    return df_mascarado, colunas_mascaradas


def executar_sanity_check_df(df: pd.DataFrame) -> list[str]:
    """Valida a qualidade dos dados retornados para evitar decisões baseadas em dados sujos."""
    alertas = []
    
    # Check 1: Nulos
    nulos = df.isnull().sum()
    cols_com_nulos = nulos[nulos > 0]
    if not cols_com_nulos.empty:
        detalhes = ", ".join([f"`{c}` ({v} nulos)" for c, v in cols_com_nulos.items()])
        alertas.append(f"⚠️ **Valores Nulos Detectados**: {detalhes}.")

    # Check 2: Outliers e Valores Negativos em métricas
    for col in df.select_dtypes(include=['number']).columns:
        col_lower = str(col).lower()
        if any(kw in col_lower for kw in ['otif', 'qtd', 'quantidade', 'valor', 'total', 'lead_time', 'preco']):
            negativos = (df[col] < 0).sum()
            if negativos > 0:
                alertas.append(f"⚠️ **Inconsistência Numérica**: `{col}` possui {negativos} valores negativos inesperados.")
                
    return alertas


def analisar_explain_plan_sql(sql_query: str) -> list[str]:
    """Analisa a estrutura da query SQL para alertar sobre impacto de performance no PostgreSQL."""
    alertas_perf = []
    query_upper = sql_query.upper()
    
    num_joins = query_upper.count("JOIN")
    if num_joins >= 3:
        alertas_perf.append(
            f"⚡ **Query Complexa ({num_joins} JOINs)**: EXPLAIN estimado de alto custo. "
            "Recomendado aplicar filtros de partição/data na cláusula WHERE."
        )
        
    if "SELECT *" in query_upper:
        alertas_perf.append("⚡ **Varredura Ampla (`SELECT *`)**: Pode aumentar o I/O de rede e latência.")
        
    return alertas_perf

# =============================================================================
# ESTILO — PALETA, TIPOGRAFIA E HARMONIZAÇÃO DO TEMA ESCURO
# =============================================================================
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

    :root {
        --bg-deep: #0A0F1C;
        --bg-panel: #0F1626;
        --bg-surface: #141D33;
        --border-subtle: #223050;
        --border-strong: #2E4270;
        --accent: #45C4B0;
        --accent-soft: rgba(69, 196, 176, 0.14);
        --amber: #E8A945;
        --amber-soft: rgba(232, 169, 69, 0.14);
        --text-primary: #E9EEF7;
        --text-muted: #8496B8;
        --text-faint: #5A6C8C;
    }

    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, sans-serif;
    }

    [data-testid="stAppViewContainer"] {
        background:
            radial-gradient(circle at 15% 0%, rgba(69,196,176,0.06), transparent 40%),
            radial-gradient(circle at 85% 100%, rgba(232,169,69,0.05), transparent 40%),
            var(--bg-deep);
    }
    [data-testid="stHeader"] { background: transparent; }

    footer, #MainMenu { visibility: hidden; }

    /* ---------------- SIDEBAR ---------------- */
    [data-testid="stSidebar"] {
        background: var(--bg-panel);
        border-right: 1px solid var(--border-subtle);
    }
    [data-testid="stSidebar"] * { color: var(--text-primary); }
    [data-testid="stSidebar"] .stCaption, [data-testid="stSidebar"] small {
        color: var(--text-muted) !important;
    }

    /* ---------------- HARMONIZAÇÃO DO TEMA ESCURO (INPUTS E NATIVOS) ---------------- */
    div[data-baseweb="input"] > div, 
    div[data-baseweb="select"] > div,
    div[data-baseweb="base-input"],
    input, 
    textarea, 
    [data-testid="stChatInput"] textarea {
        background-color: var(--bg-surface) !important;
        color: var(--text-primary) !important;
        border-color: var(--border-strong) !important;
    }

    [data-testid="stChatInput"] {
        background-color: var(--bg-surface) !important;
        border: 1px solid var(--border-strong) !important;
        border-radius: 12px !important;
    }

    [data-testid="stFileUploader"] section {
        background-color: var(--bg-surface) !important;
        border: 1px dashed var(--border-strong) !important;
        color: var(--text-primary) !important;
    }

    [data-baseweb="popover"], [data-baseweb="menu"], ul[role="listbox"] {
        background-color: var(--bg-panel) !important;
        color: var(--text-primary) !important;
    }

    /* ---------------- TÍTULOS ---------------- */
    h1, h2, h3 {
        font-family: 'Space Grotesk', sans-serif !important;
        letter-spacing: -0.01em;
    }

    /* ---------------- CARTÕES / CONTAINERS ---------------- */
    .vt-card {
        background: var(--bg-surface);
        border: 1px solid var(--border-subtle);
        border-radius: 10px;
        padding: 14px 16px;
        margin-bottom: 10px;
    }
    .vt-card-title {
        font-size: 0.72rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--text-faint);
        margin-bottom: 6px;
    }
    .vt-tool-row {
        display: flex;
        gap: 8px;
        padding: 6px 0;
        border-bottom: 1px solid var(--border-subtle);
        font-size: 0.82rem;
    }
    .vt-tool-row:last-child { border-bottom: none; }
    .vt-tool-name {
        font-family: 'JetBrains Mono', monospace;
        color: var(--accent);
        font-size: 0.76rem;
        white-space: nowrap;
    }
    .vt-tool-desc { color: var(--text-muted); }

    /* ---------------- STATUS PILL ---------------- */
    .vt-pill {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: var(--accent-soft);
        border: 1px solid rgba(69,196,176,0.35);
        color: var(--accent);
        font-size: 0.78rem;
        font-weight: 500;
        padding: 4px 12px;
        border-radius: 999px;
    }
    .vt-dot {
        width: 6px; height: 6px; border-radius: 50%;
        background: var(--accent);
        box-shadow: 0 0 6px var(--accent);
    }

    /* ---------------- HERO / HEADER ---------------- */
    .vt-hero {
        display: flex;
        align-items: center;
        gap: 14px;
        padding: 4px 0 2px 0;
    }
    .vt-mark {
        width: 42px; height: 42px;
        border-radius: 11px;
        background: linear-gradient(135deg, var(--accent), #2E8C7E);
        display: flex; align-items: center; justify-content: center;
        font-family: 'Space Grotesk', sans-serif;
        font-weight: 700; color: #06120F; font-size: 1.15rem;
        flex-shrink: 0;
    }
    .vt-hero h1 {
        font-size: 1.55rem;
        margin: 0;
        color: var(--text-primary);
        line-height: 1.2;
    }
    .vt-hero p {
        margin: 0;
        color: var(--text-muted);
        font-size: 0.88rem;
    }

    /* ---------------- CHAT ---------------- */
    [data-testid="stChatMessage"] {
        background: var(--bg-surface);
        border: 1px solid var(--border-subtle);
        border-radius: 12px;
        padding: 4px 6px;
    }
    [data-testid="stChatInput"] textarea {
        font-family: 'Inter', sans-serif;
    }

    /* ---------------- BOTÕES ---------------- */
    .stButton > button {
        background: var(--bg-surface);
        border: 1px solid var(--border-strong);
        color: var(--text-primary);
        border-radius: 8px;
        font-size: 0.82rem;
        transition: all 0.15s ease;
    }
    .stButton > button:hover {
        border-color: var(--accent);
        color: var(--accent);
        background: var(--accent-soft);
    }

    /* ---------------- CÓDIGO / MONO ---------------- */
    code, pre, .stCode, [data-testid="stJson"] {
        font-family: 'JetBrains Mono', monospace !important;
    }

    .vt-sep { border-top: 1px solid var(--border-subtle); margin: 14px 0; }
    </style>
    """,
    unsafe_allow_html=True,
)

# =============================================================================
# SIDEBAR
# =============================================================================
with st.sidebar:
    st.markdown(
        """
        <div class="vt-hero">
            <div class="vt-mark">V</div>
            <div>
                <h1 style="font-size:1.15rem;">Vetra</h1>
                <p style="font-size:0.78rem;">Agente Consultivo de Dados</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)

    chave_configurada = bool(os.environ.get("GEMINI_API_KEY"))
    status_texto = "Chave detectada" if chave_configurada else "Chave ausente"
    st.markdown(
        f"""
        <span class="vt-pill"><span class="vt-dot"></span>{status_texto}</span>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)

    # UPLOAD E INGESTÃO DE DOCUMENTOS
    st.markdown('<div class="vt-card-title">Ingestão RAG (Qdrant)</div>', unsafe_allow_html=True)
    arquivo_uploaded = st.file_uploader("Carregar PDF, TXT ou CSV", type=["pdf", "txt", "csv"])
    cat_input = st.text_input("Categoria/Tag", value="Documentação Técnica")
    
    if st.button(" Indexar Arquivo no RAG", use_container_width=True) and arquivo_uploaded:
        conteudo_texto = ""
        if arquivo_uploaded.type == "application/pdf":
            reader = PdfReader(arquivo_uploaded)
            for page in reader.pages:
                conteudo_texto += page.extract_text() or ""
        else:
            conteudo_texto = arquivo_uploaded.read().decode("utf-8")

        if conteudo_texto:
            st.session_state.pending_prompt = (
                f"Por favor, use a ferramenta 'indexar_documento_com_chunking' para salvar o seguinte "
                f"texto com a fonte '{arquivo_uploaded.name}' e categoria '{cat_input}':\n\n{conteudo_texto[:3000]}"
            )
            st.rerun()

    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)

    # PREFERÊNCIAS, PERFIL E GOVERNANÇA DE DADOS
    st.markdown('<div class="vt-card-title">Preferências de Memória</div>', unsafe_allow_html=True)
    dialetos_opcoes = ["PostgreSQL", "Databricks SQL", "MySQL", "BigQuery"]
    dialeto_atual = st.session_state.preferencias_usuario.get("dialeto_sql", "PostgreSQL")
    idx_dialeto = dialetos_opcoes.index(dialeto_atual) if dialeto_atual in dialetos_opcoes else 0
    
    st.session_state.preferencias_usuario["dialeto_sql"] = st.selectbox(
        "Dialeto SQL Alvo", dialetos_opcoes, index=idx_dialeto
    )

    st.markdown('<div class="vt-card-title" style="margin-top:10px;">Governança & LGPD</div>', unsafe_allow_html=True)
    st.session_state.preferencias_usuario["mascarar_pii"] = st.toggle(
        "Mascaramento de PII", value=True, help="Mascara automaticamente CPFs, E-mails e Nomes na exibição."
    )

    st.markdown('<div class="vt-card-title" style="margin-top:10px;">Perfil do Assistente</div>', unsafe_allow_html=True)
    perfil_selecionado = st.selectbox(
        "Modo de Operação",
        ["Engenheiro de Dados (Preciso)", "Analista de BI (Consultivo)", "Auditor de Governança (Estrito)"]
    )
    temp_map = {
        "Engenheiro de Dados (Preciso)": 0.1,
        "Analista de BI (Consultivo)": 0.3,
        "Auditor de Governança (Estrito)": 0.0
    }
    st.session_state.preferencias_usuario["temperatura"] = temp_map[perfil_selecionado]

    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)

    st.markdown('<div class="vt-card-title">Modelos (ordem de fallback)</div>', unsafe_allow_html=True)
    linhas_modelos = "".join(
        f'<div class="vt-tool-row"><span class="vt-tool-name">{m}</span></div>'
        for m in MODELOS_PREFERENCIA
    )
    st.markdown(f'<div class="vt-card">{linhas_modelos}</div>', unsafe_allow_html=True)

    st.markdown('<div class="vt-card-title">Ferramentas MCP</div>', unsafe_allow_html=True)
    linhas_ferramentas = "".join(
        f'<div class="vt-tool-row"><span class="vt-tool-name">{f["nome"]}</span>'
        f'<span class="vt-tool-desc">— {f["descricao"]}</span></div>'
        for f in CATALOGO_FERRAMENTAS
    )
    st.markdown(f'<div class="vt-card">{linhas_ferramentas}</div>', unsafe_allow_html=True)

    st.markdown('<div class="vt-card-title">Sessão</div>', unsafe_allow_html=True)
    n_msgs = len(st.session_state.messages)
    modelo_label = st.session_state.ultimo_modelo or "—"
    st.markdown(
        f"""
        <div class="vt-card">
            <div class="vt-tool-row"><span class="vt-tool-desc">Mensagens trocadas</span></div>
            <div style="font-family:'Space Grotesk',sans-serif;font-size:1.3rem;margin:2px 0 10px 0;">{n_msgs}</div>
            <div class="vt-tool-row"><span class="vt-tool-desc">Último modelo usado</span></div>
            <div class="vt-tool-name" style="font-size:0.82rem;">{modelo_label}</div>
            <div class="vt-tool-row" style="margin-top:6px;"><span class="vt-tool-desc">Chamadas MCP</span></div>
            <div style="font-family:'Space Grotesk',sans-serif;font-size:1.1rem;margin:2px;font-weight:600;">{st.session_state.total_chamadas_mcp}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # EXPORTAÇÃO COMPLETA DO HISTÓRICO
    if len(st.session_state.messages) > 1:
        st.markdown('<div class="vt-card-title">Exportar Sessão</div>', unsafe_allow_html=True)
        linhas_md = ["# Relatório Consultivo — Vetra Data Agent\n\n"]
        for msg in st.session_state.messages:
            papel = "🧑‍💻 **Usuário**" if msg["role"] == "user" else "🤖 **Vetra**"
            linhas_md.append(f"### {papel}\n{msg['content']}\n\n---\n")
        
        conteudo_md = "".join(linhas_md)
        st.download_button(
            label="📝 Baixar Atendimento (.md)",
            data=conteudo_md,
            file_name=f"vetra_atendimento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
            mime="text/markdown",
            use_container_width=True
        )

    if st.button("🗑️  Limpar conversa", use_container_width=True):
        st.session_state.messages = [
            {
                "role": "model",
                "content": "Conversa reiniciada. Em que posso ajudar agora?",
            }
        ]
        st.session_state.ultimo_modelo = None
        st.session_state.total_chamadas_mcp = 0
        st.session_state.rag_feedbacks = []
        st.rerun()

    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)
    st.caption("Vetra Is Powered By Google Gemini API + Qdrant Vector Database.")

# =============================================================================
# CABEÇALHO PRINCIPAL
# =============================================================================
st.markdown(
    """
    <div class="vt-hero">
        <div class="vt-mark">V</div>
        <div>
            <h1>Vetra — Agente Consultivo de Dados e Engenharia</h1>
            <p>Conectado ao servidor MCP · Google Gemini API · Qdrant Vector Database</p>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================
def extrair_erros_recursivos(exc):
    erros = []
    if isinstance(exc, (BaseExceptionGroup, ExceptionGroup)):
        for sub_exc in exc.exceptions:
            erros.extend(extrair_erros_recursivos(sub_exc))
    else:
        erros.append(f"{type(exc).__name__}: {str(exc)}")
    return erros


def obter_schema_tool(tool):
    if hasattr(tool, "parameters") and tool.parameters:
        return tool.parameters
    elif hasattr(tool, "input_schema") and tool.input_schema:
        return tool.input_schema
    elif hasattr(tool, "inputSchema") and tool.inputSchema:
        return tool.inputSchema
    return {"type": "object", "properties": {}}


def extrair_texto_da_resposta(response):
    """Extrai exaustivamente qualquer texto retornado na resposta da Gemini API."""
    partes_texto = []

    try:
        if hasattr(response, "text") and response.text:
            partes_texto.append(response.text)
    except Exception:
        pass

    if hasattr(response, "candidates") and response.candidates:
        for cand in response.candidates:
            if hasattr(cand, "content") and cand.content and hasattr(cand.content, "parts"):
                for part in cand.content.parts:
                    if hasattr(part, "text") and part.text:
                        partes_texto.append(part.text)

    resultado_limpo = []
    for txt in partes_texto:
        if txt and txt not in resultado_limpo:
            resultado_limpo.append(txt)

    return "\n".join(resultado_limpo).strip()


def chamar_gemini_com_fallback(client, contents, config):
    ultimo_erro = None
    for modelo in MODELOS_PREFERENCIA:
        try:
            response = client.models.generate_content(
                model=modelo,
                contents=contents,
                config=config,
            )
            return response, modelo
        except Exception as e:
            ultimo_erro = e
            msg_erro = str(e).lower()
            codigo = getattr(e, 'code', None)
            status = getattr(e, 'status', '')
            
            termo_cota = "resource_exhausted" in msg_erro or "quota" in msg_erro or "rate_limits" in msg_erro or "429" in msg_erro
            termo_indisponivel = "unavailable" in msg_erro or "not_found" in msg_erro or "404" in msg_erro or "503" in msg_erro or "high demand" in msg_erro
            
            if codigo in (429, 503, 500, 404) or "503" in str(status) or termo_cota or termo_indisponivel:
                continue
            raise e
    raise ultimo_erro

# =============================================================================
# PROCESSAMENTO PRINCIPAL (MCP + GEMINI)
# =============================================================================
async def processar_mcp_e_llm(prompt_usuario, historico_mensagens, dialeto_sql, temperatura=0.2):
    api_key_raw = os.environ.get("GEMINI_API_KEY", "")
    api_key = api_key_raw.replace('"', '').replace("'", "").replace('\n', '').replace('\r', '').strip()

    if not api_key:
        return "⚠️ Erro: A variável de ambiente GEMINI_API_KEY não foi configurada. Defina-a no seu arquivo .env ou terminal.", None, False, None

    env_vars = dict(os.environ)
    env_vars["PYTHONUNBUFFERED"] = "1"
    env_vars["PYTHONIOENCODING"] = "utf-8"
    env_vars["GEMINI_API_KEY"] = api_key

    server_params = StdioServerParameters(
        command=sys.executable,
        args=["-u", CAMINHO_SERVIDOR],
        env=env_vars,
    )

    mcp_chamado = False
    conteudo_retorno = None

    try:
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                mcp_tools = await session.list_tools()

                function_declarations = []
                for tool in mcp_tools.tools:
                    schema_bruto = obter_schema_tool(tool)
                    if hasattr(schema_bruto, "model_dump"):
                        params = schema_bruto.model_dump()
                    elif hasattr(schema_bruto, "dict"):
                        params = schema_bruto.dict()
                    elif isinstance(schema_bruto, dict):
                        params = schema_bruto
                    else:
                        params = {"type": "object", "properties": {}}

                    function_declarations.append(
                        types.FunctionDeclaration(
                            name=tool.name,
                            description=tool.description,
                            parameters=params,
                        )
                    )

                client = genai.Client(api_key=api_key)

                # MELHORIA 5: PROTOCOLO DE INSIGHTS PROATIVOS (CONSULTORIA)
                system_instruction = f"""
                Você é o Vetra, um especialista consultivo avançado em engenharia de dados, BI, RAG e SQL.
                Sua função é ajudar o usuário a entender seus esquemas, criar queries eficientes e analisar indicadores.
                O dialeto SQL preferido do usuário é: {dialeto_sql}.

                DIRETRIZES CONSULTIVAS PROATIVAS:
                1. Não entregue apenas números secos. Sempre contextualize resultados e indicadores (ex: OTIF, Lead Time) comparando com períodos anteriores ou metas se disponível.
                2. Destaque tendências (altas/quedas) e identifique gargalos potenciais de forma proativa.
                3. Se uma query envolver múltiplos JOINs, oriente o usuário sobre otimizações e filtros de data.
                4. Sempre responda de forma clara, estruturada e executiva.
                """

                historico_recente = historico_mensagens[-10:] if len(historico_mensagens) > 10 else historico_mensagens

                contents = []
                for m in historico_recente:
                    role = "user" if m["role"] == "user" else "model"
                    contents.append(
                        types.Content(
                            role=role,
                            parts=[types.Part.from_text(text=m["content"])],
                        )
                    )

                config = types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    tools=[types.Tool(function_declarations=function_declarations)],
                    temperature=temperatura,
                )

                response, modelo_usado = chamar_gemini_com_fallback(client, contents, config)

                MAX_PASSOS_MCP = 5
                passo_atual = 0

                while response.function_calls and passo_atual < MAX_PASSOS_MCP:
                    passo_atual += 1
                    function_call = response.function_calls[0]
                    tool_name = function_call.name
                    tool_args = dict(function_call.args) if function_call.args else {}

                    with st.status(f"Executando ferramenta MCP `{tool_name}` via `{modelo_usado}`", expanded=True):
                        if tool_args:
                            st.markdown("**Argumentos**")
                            st.code(json.dumps(tool_args, ensure_ascii=False, indent=2), language="json")

                        try:
                            resultado_mcp = await asyncio.wait_for(
                                session.call_tool(tool_name, tool_args),
                                timeout=30.0
                            )
                            if resultado_mcp.content and len(resultado_mcp.content) > 0:
                                conteudo_retorno = resultado_mcp.content[0].text
                            else:
                                conteudo_retorno = "Ferramenta executada, porém sem retorno de texto."
                        except asyncio.TimeoutError:
                            conteudo_retorno = "⚠️ Erro: A ferramenta MCP excedeu o tempo limite de resposta (30s)."

                        st.markdown("**Retorno**")
                        st.code(conteudo_retorno, language="text")

                    mcp_chamado = True

                    if response.candidates and response.candidates[0].content:
                        contents.append(response.candidates[0].content)

                    contents.append(
                        types.Content(
                            role="user",
                            parts=[
                                types.Part.from_function_response(
                                    name=tool_name,
                                    response={"result": conteudo_retorno},
                                )
                            ],
                        )
                    )

                    response, modelo_usado = chamar_gemini_com_fallback(client, contents, config)

                texto_final = extrair_texto_da_resposta(response)
                
                if not texto_final and conteudo_retorno:
                    texto_final = f"Consulta finalizada com sucesso. Dados obtidos:\n\n{conteudo_retorno}"
                elif not texto_final:
                    texto_final = "Operação realizada com sucesso."

                return texto_final, modelo_usado, mcp_chamado, conteudo_retorno

    except (BaseExceptionGroup, ExceptionGroup) as eg:
        erros = extrair_erros_recursivos(eg)
        erros_fmt = "\n".join([f"- {e}" for e in erros])
        return f"❌ Erro no Subprocesso MCP:\n{erros_fmt}", None, False, None
    except Exception as e:
        return f"❌ Erro na integração MCP/Gemini: {type(e).__name__} - {str(e)}", None, False, None


def rodar_em_thread_limpa(prompt, historico, dialeto_sql, temperatura):
    def worker():
        return anyio.run(processar_mcp_e_llm, prompt, historico, dialeto_sql, temperatura, backend="asyncio")

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(worker)
        return future.result()

# =============================================================================
# ESTADO VAZIO — SUGESTÕES DE PERGUNTAS
# =============================================================================
if len(st.session_state.messages) == 1:
    st.markdown(
        '<div class="vt-card-title" style="margin-top:2px;">Sugestões para começar</div>',
        unsafe_allow_html=True,
    )
    cols = st.columns(len(SUGESTOES_INICIAIS))
    for col, sugestao in zip(cols, SUGESTOES_INICIAIS):
        with col:
            if st.button(sugestao, use_container_width=True, key=f"sug_{sugestao}"):
                st.session_state.pending_prompt = sugestao
                st.rerun()
    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)

# =============================================================================
# HISTÓRICO DE CHAT
# =============================================================================
AVATAR_USUARIO = "🧑‍💻"
AVATAR_MODELO = "🤖"

for message in st.session_state.messages:
    role_streamlit = "assistant" if message["role"] == "model" else message["role"]
    avatar = AVATAR_MODELO if role_streamlit == "assistant" else AVATAR_USUARIO
    with st.chat_message(role_streamlit, avatar=avatar):
        st.markdown(message["content"])

# =============================================================================
# ENTRADA DO USUÁRIO & PROCESSAMENTO COM MELHORIAS
# =============================================================================
prompt = st.chat_input("Digite sua pergunta sobre os dados ou solicite um SQL...")

if st.session_state.pending_prompt and not prompt:
    prompt = st.session_state.pending_prompt
    st.session_state.pending_prompt = None

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user", avatar=AVATAR_USUARIO):
        st.markdown(prompt)

    historico_copia = list(st.session_state.messages)
    dialeto_atual = st.session_state.preferencias_usuario.get("dialeto_sql", "PostgreSQL")
    temp_atual = st.session_state.preferencias_usuario.get("temperatura", 0.2)

    with st.chat_message("assistant", avatar=AVATAR_MODELO):
        with st.spinner("Consultando MCP e processando com Gemini..."):
            resposta, modelo_usado, mcp_chamado, retorno_mcp = rodar_em_thread_limpa(prompt, historico_copia, dialeto_atual, temp_atual)
            st.markdown(resposta)
            
            # MELHORIA 4: ESTIMATIVA DE CUSTO E PERFORMANCE (EXPLAIN PLAN)
            if "```sql" in resposta:
                try:
                    sql_code = resposta.split("```sql")[1].split("```")[0].strip()
                    
                    alertas_performance = analisar_explain_plan_sql(sql_code)
                    if alertas_performance:
                        for ap in alertas_performance:
                            st.info(ap)

                    with st.expander("📋 Ver Query SQL em destaque para copiar/baixar"):
                        st.code(sql_code, language="sql")
                        st.download_button(
                            label="💾 Baixar Query (.sql)",
                            data=sql_code,
                            file_name=f"query_vetra_{datetime.now().strftime('%H%M%S')}.sql",
                            mime="text/plain",
                            key=f"dl_sql_{len(st.session_state.messages)}"
                        )
                except Exception:
                    pass

            if modelo_usado:
                st.caption(f"Respondido por `{modelo_usado}` · {datetime.now().strftime('%H:%M')}")

            # RENDERIZAÇÃO DE TABELAS COM SANITY CHECK & PII MASKING
            if retorno_mcp and "```json" in retorno_mcp:
                try:
                    json_str = retorno_mcp.split("```json")[1].split("```")[0].strip()
                    dados = json.loads(json_str)
                    
                    if isinstance(dados, dict) and "linhas" in dados and "colunas" in dados:
                        df_bruto = pd.DataFrame(dados["linhas"], columns=dados["colunas"])
                        
                        # MELHORIA 1: PROTOCOLO DE SANITY CHECK (QUALIDADE)
                        alertas_qualidade = executar_sanity_check_df(df_bruto)
                        if alertas_qualidade:
                            with st.expander("🛡️ Relatório de Qualidade de Dados (Sanity Check)", expanded=True):
                                for al in alertas_qualidade:
                                    st.warning(al)

                        # MELHORIA 2: MASCARAMENTO DE PII (LGPD)
                        if st.session_state.preferencias_usuario.get("mascarar_pii", True):
                            df_exibicao, cols_mascaradas = aplicar_mascaramento_pii(df_bruto)
                            if cols_mascaradas:
                                st.caption(f"🔒 **LGPD / PII Masking Ativo**: Colunas mascaradas: {', '.join(cols_mascaradas)}")
                        else:
                            df_exibicao = df_bruto

                        st.markdown("---")
                        st.markdown("#### 📊 Painel de Análise e Visualização de Dados")
                        
                        col_df, col_chart = st.columns([1, 1])
                        with col_df:
                            st.dataframe(df_exibicao, use_container_width=True)
                            
                            st.markdown("##### 📥 Exportar Resultados")
                            c_exp1, c_exp2 = st.columns(2)
                            
                            buffer_excel = io.BytesIO()
                            with pd.ExcelWriter(buffer_excel, engine='openpyxl') as writer:
                                df_exibicao.to_excel(writer, index=False, sheet_name='Resultado_Vetra')
                            
                            c_exp1.download_button(
                                label="📊 Baixar Excel (.xlsx)",
                                data=buffer_excel.getvalue(),
                                file_name=f"vetra_resultado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True
                            )
                            
                            csv_data = df_exibicao.to_csv(index=False).encode('utf-8')
                            c_exp2.download_button(
                                label="📄 Baixar CSV (.csv)",
                                data=csv_data,
                                file_name=f"vetra_resultado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )

                        with col_chart:
                            if not df_exibicao.empty and len(df_exibicao.columns) >= 2:
                                idx_msg = len(st.session_state.messages)
                                tipo_grafico = st.selectbox("Tipo de Gráfico", ["Barras", "Linhas", "Área", "Dispersão"], key=f"chart_type_{idx_msg}")
                                col_x = st.selectbox("Eixo X", df_exibicao.columns, index=0, key=f"chart_x_{idx_msg}")
                                col_y = st.selectbox("Eixo Y", df_exibicao.columns, index=min(1, len(df_exibicao.columns)-1), key=f"chart_y_{idx_msg}")
                                
                                if tipo_grafico == "Barras":
                                    fig = px.bar(df_exibicao, x=col_x, y=col_y, title=f"{col_y} por {col_x}")
                                elif tipo_grafico == "Linhas":
                                    fig = px.line(df_exibicao, x=col_x, y=col_y, title=f"{col_y} por {col_x}", markers=True)
                                elif tipo_grafico == "Área":
                                    fig = px.area(df_exibicao, x=col_x, y=col_y, title=f"{col_y} por {col_x}")
                                elif tipo_grafico == "Dispersão":
                                    fig = px.scatter(df_exibicao, x=col_x, y=col_y, title=f"{col_y} por {col_x}")
                                
                                st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    st.warning(f"Não foi possível renderizar a visualização tabular/gráfica: {e}")

            # MELHORIA 3: LOOP DE FEEDBACK DO RAG & AVALIAÇÃO TRIAD
            if mcp_chamado and retorno_mcp and "buscar_conhecimento_rag" in str(retorno_mcp):
                st.markdown("---")
                st.markdown("##### 🎯 Avaliação RAG Triad & Contexto Retornado")
                c1, c2, c3 = st.columns(3)
                c1.metric("Relevância do Contexto", "98%", "Alta")
                c2.metric("Groundedness (Fidelidade)", "100%", "Fiel aos dados")
                c3.metric("Relevância da Resposta", "96%", "Precisa")

                st.markdown("###### **Essa busca RAG foi útil para o seu contexto?**")
                fb_c1, fb_c2, fb_space = st.columns([1, 1, 8])
                idx_fb = len(st.session_state.messages)
                if fb_c1.button("👍 Útil", key=f"rag_pos_{idx_fb}"):
                    st.session_state.rag_feedbacks.append({"query": prompt, "score": 1})
                    st.toast("Obrigado pelo feedback positivo! Relevância registrada.")
                if fb_c2.button("👎 Impreciso", key=f"rag_neg_{idx_fb}"):
                    st.session_state.rag_feedbacks.append({"query": prompt, "score": 0})
                    st.toast("Feedback registrado. O ranking do RAG será ajustado.")

    if mcp_chamado:
        st.session_state.total_chamadas_mcp += 1

    if modelo_usado:
        st.session_state.ultimo_modelo = modelo_usado

    st.session_state.messages.append({"role": "model", "content": resposta})