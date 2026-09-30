import os
import sys
import json
import io
import re
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.express as px
from pypdf import PdfReader
import requests

# =============================================================================
# CONFIGURAÇÃO DA PÁGINA
# =============================================================================
st.set_page_config(
    page_title="Vetra — Agente Consultivo de Dados",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# URL da API Backend (Endereço padronizado para a rota de streaming da API)
API_URL = os.environ.get("VETRA_API_URL", "http://127.0.0.1:8000/chat/stream")

# Tabela de Preços Estimados (Gemini Flash Pay-as-you-go) por 1 Milhão de Tokens (USD)
PRECO_INPUT_1M = 0.075   # $0,075 por 1M tokens de entrada
PRECO_OUTPUT_1M = 0.30   # $0,30 por 1M tokens de saída
TAXA_CAMBIO_USD_BRL = 5.60 # Cotação média BRL/USD para estimativa

# Lista de modelos válidos e recomendados estritamente da Google Gemini API
MODELOS_PREFERENCIA = [
    "gemini-3.8-flash",         # Modelo principal recomendado pela Google
    "gemini-3.1-pro-preview",   # Modelo Pro recomendado
    "gemini-3-flash-preview",   # Modelo Preview ativo
]

# Catálogo completo das ferramentas expostas pelo servidor MCP
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
# FUNÇÃO DE BUSCA SEGURA DE LEITURA DE CHAVE (EVITA STREAMLITSECRETNOTFOUNDERROR)
# =============================================================================
def obter_chave_gemini_segura() -> str:
    """Busca primeiro em os.environ e trata st.secrets caso não exista secrets.toml."""
    valor_env = os.environ.get("GEMINI_API_KEY")
    if valor_env:
        return valor_env
    try:
        if "GEMINI_API_KEY" in st.secrets:
            return st.secrets["GEMINI_API_KEY"]
    except Exception:
        pass
    return ""

# =============================================================================
# ESTADO DE SESSÃO & MEMÓRIA
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

if "total_tokens_input" not in st.session_state:
    st.session_state.total_tokens_input = 0

if "total_tokens_output" not in st.session_state:
    st.session_state.total_tokens_output = 0

if "custo_acumulado_usd" not in st.session_state:
    st.session_state.custo_acumulado_usd = 0.0

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
    
    nulos = df.isnull().sum()
    cols_com_nulos = nulos[nulos > 0]
    if not cols_com_nulos.empty:
        detalhes = ", ".join([f"`{c}` ({v} nulos)" for c, v in cols_com_nulos.items()])
        alertas.append(f"⚠️ **Valores Nulos Detectados**: {detalhes}.")

    for col in df.select_dtypes(include=['number']).columns:
        col_lower = str(col).lower()
        if any(kw in col_lower for kw in ['otif', 'qtd', 'quantidade', 'valor', 'total', 'lead_time', 'preco']):
            negativos = (df[col] < 0).sum()
            if negativos > 0:
                alertas.append(f"⚠️️ **Inconsistência Numérica**: `{col}` possui {negativos} valores negativos inesperados.")
                
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


def chamar_api_backend(prompt: str, historico: list, dialeto_sql: str, temperatura: float):
    """Envia o payload e consome a rota SSE com streaming da API Backend."""
    # Transforma 'model' em 'assistant' para garantir compatibilidade se necessário
    historico_formatado = []
    for m in historico:
        historico_formatado.append({
            "role": m.get("role", "user"),
            "content": m.get("content", "")
        })

    payload = {
        "prompt": prompt,
        "historico": historico_formatado,
        "dialeto_sql": dialeto_sql,
        "temperatura": temperatura
    }

    try:
        url_alvo = API_URL if API_URL.endswith("/chat/stream") else API_URL.replace("/chat", "/chat/stream")
        
        response = requests.post(
            url_alvo, 
            json=payload, 
            headers={"Content-Type": "application/json"},
            stream=True,
            timeout=300
        )
        
        if response.status_code == 200:
            resposta_texto = ""
            modelo_usado = "FastAPI-Backend"
            mcp_chamado = False
            dados_mcp_raw = None

            for line in response.iter_lines():
                if line:
                    linha_str = line.decode("utf-8")
                    if linha_str.startswith("data: "):
                        conteudo = json.loads(linha_str[6:])
                        if "error" in conteudo:
                            return f"❌ Erro no backend: {conteudo['error']}", None, False, None
                        if "resposta" in conteudo:
                            resposta_texto = conteudo.get("resposta", "")
                            modelo_usado = conteudo.get("modelo_usado", modelo_usado)
                            mcp_chamado = conteudo.get("mcp_chamado", False)
                            dados_mcp_raw = conteudo.get("dados_mcp_raw", None)

            return resposta_texto, modelo_usado, mcp_chamado, dados_mcp_raw
        else:
            return f"❌ Erro na API Backend (Status {response.status_code}): {response.text}", None, False, None

    except requests.exceptions.ConnectionError:
        return f"❌ Não foi possível conectar ao servidor backend da Vetra ({API_URL}). Certifique-se de que a API FastAPI está ativa.", None, False, None
    except requests.exceptions.Timeout:
        return "❌ Tempo limite excedido (Timeout) aguardando resposta do backend.", None, False, None
    except Exception as e:
        return f"❌ Erro ao comunicar com o Backend: {str(e)}", None, False, None

# =============================================================================
# ESTILO — TEMA ESCURO HARMONIZADO
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

    [data-testid="stSidebar"] {
        background: var(--bg-panel);
        border-right: 1px solid var(--border-subtle);
    }
    [data-testid="stSidebar"] * { color: var(--text-primary); }

    div[data-baseweb="input"] > div, 
    div[data-baseweb="select"] > div,
    input, textarea, [data-testid="stChatInput"] textarea {
        background-color: var(--bg-surface) !important;
        color: var(--text-primary) !important;
        border-color: var(--border-strong) !important;
    }

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
    .vt-tool-name {
        font-family: 'JetBrains Mono', monospace;
        color: var(--accent);
        font-size: 0.76rem;
    }
    .vt-tool-desc { color: var(--text-muted); }

    .vt-pill {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: var(--accent-soft);
        border: 1px solid rgba(69,196,176,0.35);
        color: var(--accent);
        font-size: 0.78rem;
        padding: 4px 12px;
        border-radius: 999px;
    }
    .vt-dot {
        width: 6px; height: 6px; border-radius: 50%;
        background: var(--accent);
    }

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
    }

    [data-testid="stChatMessage"] {
        background: var(--bg-surface);
        border: 1px solid var(--border-subtle);
        border-radius: 12px;
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

    chave_configurada = bool(obter_chave_gemini_segura())
    status_texto = "Chave detectada" if chave_configurada else "API Backend Ativa"
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

    if st.button("🗑️  Limpar conversa", use_container_width=True):
        st.session_state.messages = [
            {
                "role": "model",
                "content": "Conversa reiniciada. Em que posso ajudar agora?",
            }
        ]
        st.session_state.ultimo_modelo = None
        st.session_state.total_chamadas_mcp = 0
        st.rerun()

    st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)
    st.caption("Vetra Client — Powered by FastAPI + MCP Backend.")

# =============================================================================
# CABEÇALHO PRINCIPAL
# =============================================================================
st.markdown(
    """
    <div class="vt-hero">
        <div class="vt-mark">V</div>
        <div>
            <h1>Vetra — Agente Consultivo de Dados e Engenharia</h1>
            <p>Conectado à API FastAPI · Servidor MCP · Qdrant Vector Database</p>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown('<div class="vt-sep"></div>', unsafe_allow_html=True)

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
AVATAR_USUARIO = "👤"
AVATAR_MODELO = "🤖"

for message in st.session_state.messages:
    role_streamlit = "assistant" if message["role"] == "model" else message["role"]
    avatar = AVATAR_MODELO if role_streamlit == "assistant" else AVATAR_USUARIO
    with st.chat_message(role_streamlit, avatar=avatar):
        st.markdown(message["content"])

# =============================================================================
# ENTRADA DO USUÁRIO & PROCESSAMENTO
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
        with st.spinner("Enviando requisição à API Backend da Vetra..."):
            resposta, modelo_usado, mcp_chamado, retorno_mcp = chamar_api_backend(
                prompt, historico_copia, dialeto_atual, temp_atual
            )
            st.markdown(resposta)
            
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
                st.caption(f"Respondido por `{modelo_usado}` via FastAPI Backend · {datetime.now().strftime('%H:%M')}")

            # RENDERIZAÇÃO VISUAL: TABELAS E MOTORES DE MACHINE LEARNING
            if retorno_mcp:
                json_str = retorno_mcp
                if "```json" in retorno_mcp:
                    json_str = retorno_mcp.split("```json")[1].split("```")[0].strip()
                elif "```" in retorno_mcp:
                    json_str = retorno_mcp.split("```")[1].split("```")[0].strip()

                try:
                    dados = json.loads(json_str)

                    if isinstance(dados, dict) and dados.get("status") == "sucesso":
                        st.markdown("---")
                        st.markdown("#### 🤖 Painel de Inteligência de Machine Learning")

                        if "previsoes" in dados and "historico_recente" in dados:
                            df_hist = pd.DataFrame(dados["historico_recente"])
                            df_pred = pd.DataFrame(dados["previsoes"])

                            if "valor_historico" in df_hist.columns:
                                df_hist = df_hist.rename(columns={"valor_historico": "Valor"})
                                df_hist["Tipo"] = "Histórico"
                            
                            if "valor_previsto" in df_pred.columns:
                                df_pred = df_pred.rename(columns={"valor_previsto": "Valor"})
                                df_pred["Tipo"] = "Projeção ML"

                            df_combinado = pd.concat([df_hist, df_pred], ignore_index=True)

                            fig_forecast = px.line(
                                df_combinado, x="data", y="Valor", color="Tipo",
                                title="📈 Projeção Tendencial e Séries Temporais (Forecasting ML)",
                                markers=True, color_discrete_map={"Histórico": "#45C4B0", "Projeção ML": "#E8A945"}
                            )
                            st.plotly_chart(fig_forecast, use_container_width=True)

                        elif "total_anomalias_encontradas" in dados:
                            c_kpi1, c_kpi2, c_kpi3 = st.columns(3)
                            c_kpi1.metric("Linhas Analisadas", dados.get("total_linhas_analisadas", 0))
                            c_kpi2.metric("Anomalias Detectadas", dados.get("total_anomalias_encontradas", 0))
                            c_kpi3.metric("Taxa de Contaminação", f"{dados.get('percentual_anomalias', 0)}%")

                            if "amostra_anomalias_detectadas" in dados and dados["amostra_anomalias_detectadas"]:
                                df_anom = pd.DataFrame(dados["amostra_anomalias_detectadas"])
                                st.dataframe(df_anom, use_container_width=True)

                    elif isinstance(dados, dict) and "linhas" in dados and "colunas" in dados:
                        df_bruto = pd.DataFrame(dados["linhas"], columns=dados["colunas"])
                        
                        alertas_qualidade = executar_sanity_check_df(df_bruto)
                        if alertas_qualidade:
                            with st.expander("🛡️ Relatório de Qualidade de Dados (Sanity Check)", expanded=True):
                                for al in alertas_qualidade:
                                    st.warning(al)

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

                        with col_chart:
                            if not df_exibicao.empty and len(df_exibicao.columns) >= 2:
                                idx_msg = len(st.session_state.messages)
                                col_x = st.selectbox("Eixo X", df_exibicao.columns, index=0, key=f"chart_x_{idx_msg}")
                                col_y = st.selectbox("Eixo Y", df_exibicao.columns, index=min(1, len(df_exibicao.columns)-1), key=f"chart_y_{idx_msg}")
                                fig = px.bar(df_exibicao, x=col_x, y=col_y, title=f"{col_y} por {col_x}")
                                st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    st.warning(f"Não foi possível renderizar a visualização tabular/gráfica: {e}")

    if mcp_chamado:
        st.session_state.total_chamadas_mcp += 1

    if modelo_usado:
        st.session_state.ultimo_modelo = modelo_usado

    st.session_state.messages.append({"role": "model", "content": resposta})