import os
import sys
import json
import asyncio
import concurrent.futures
from datetime import datetime

import streamlit as st
import anyio
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

DIR_ATUAL = os.path.dirname(os.path.abspath(__file__))
CAMINHO_SERVIDOR = os.path.join(DIR_ATUAL, "servidor_mcp.py")

# Lista de modelos válidos e recomendados pela API
MODELOS_PREFERENCIA = [
    "gemini-3-flash-preview",
    "gemini-2.5-flash",
    "gemini-3.5-flash-lite",
]

# Catálogo completo das ferramentas expostas pelo servidor MCP (incluindo RAG)
CATALOGO_FERRAMENTAS = [
    {"nome": "listar_esquemas_e_tabelas", "descricao": "Lista tabelas e visões do ambiente"},
    {"nome": "descrever_estrutura_tabela", "descricao": "Traz DDL, colunas e tipos de uma tabela"},
    {"nome": "executar_query_sql", "descricao": "Executa SELECTs no Databricks SQL"},
    {"nome": "buscar_conhecimento_rag", "descricao": "Busca vetorial/semântica no Qdrant Cloud"},
    {"nome": "indexar_documento_rag", "descricao": "Salva e indexa novos conhecimentos"},
]

SUGESTOES_INICIAIS = [
    "Quais tabelas eu tenho disponíveis?",
    "Explique a estrutura da tb_producao",
    "Qual a fórmula do OTIF % acumulado?",
    "Monte uma query de produção do mês atual",
]

# =============================================================================
# ESTILO — PALETA, TIPOGRAFIA E COMPONENTES CUSTOMIZADOS
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
    [data-testid="stChatInput"] {
        border: 1px solid var(--border-strong) !important;
        border-radius: 12px !important;
        background: var(--bg-surface) !important;
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
# ESTADO DE SESSÃO
# =============================================================================
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "model",
            "content": "Olá! Sou o **Vetra**, seu agente consultivo de engenharia de dados e BI. "
                       "Posso explorar esquemas, montar queries SQL e validar regras de negócio "
                       "usando as ferramentas conectadas via MCP. Como posso ajudar?",
        }
    ]
if "pending_prompt" not in st.session_state:
    st.session_state.pending_prompt = None
if "ultimo_modelo" not in st.session_state:
    st.session_state.ultimo_modelo = None
if "total_chamadas_mcp" not in st.session_state:
    st.session_state.total_chamadas_mcp = 0

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
            <div style="font-family:'Space Grotesk',sans-serif;font-size:1.1rem;margin-top:2px;">{st.session_state.total_chamadas_mcp}</div>
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
    st.caption("Vetra roda localmente via Streamlit + MCP, com a Google Gemini API.")

# =============================================================================
# CABEÇALHO PRINCIPAL
# =============================================================================
st.markdown(
    """
    <div class="vt-hero">
        <div class="vt-mark">V</div>
        <div>
            <h1>Vetra — Agente Consultivo de Análise de Dados</h1>
            <p>Conectado ao servidor MCP · Google Gemini API</p>
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


def chamar_gemini_com_fallback(client, contents, config):
    """Tenta cada modelo da lista; pula para o próximo em caso de 404, 503, 429 (cota excedida) 
    ou mensagens de erro equivalentes."""
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
            
            # Pula para o próximo modelo se o modelo estourar cota, estiver indisponível ou não for encontrado
            termo_cota = "resource_exhausted" in msg_erro or "quota" in msg_erro or "rate_limits" in msg_erro or "429" in msg_erro
            termo_indisponivel = "unavailable" in msg_erro or "not_found" in msg_erro or "404" in msg_erro or "503" in msg_erro
            
            if codigo in (429, 503, 404) or termo_cota or termo_indisponivel:
                continue
            raise e
    raise ultimo_erro

# =============================================================================
# PROCESSAMENTO PRINCIPAL (MCP + GEMINI)
# =============================================================================
async def processar_mcp_e_llm(prompt_usuario, historico_mensagens):
    if not os.environ.get("GEMINI_API_KEY"):
        return "⚠️ Erro: A variável de ambiente GEMINI_API_KEY não foi configurada. Defina-a no seu arquivo .bat ou terminal.", None, False

    env_vars = dict(os.environ)
    env_vars["PYTHONUNBUFFERED"] = "1"
    env_vars["PYTHONIOENCODING"] = "utf-8"

    server_params = StdioServerParameters(
        command=sys.executable,
        args=["-u", CAMINHO_SERVIDOR],
        env=env_vars,
    )

    mcp_chamado = False

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

                client = genai.Client()

                system_instruction = """
                Você é um especialista consultivo em engenharia de dados, business intelligence, RAG e SQL.
                Sua função é ajudar o usuário a entender seus esquemas de banco de dados, criar queries eficientes,
                validar regras de negócio e realizar análises de performance.
                Sempre utilize as ferramentas MCP disponíveis para consultar a base RAG, esquemas, regras ou executar queries antes de responder.
                """

                contents = []
                for m in historico_mensagens:
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
                    temperature=0.2,
                )

                response, modelo_usado = chamar_gemini_com_fallback(client, contents, config)

                if response.function_calls:
                    function_call = response.function_calls[0]
                    tool_name = function_call.name
                    tool_args = dict(function_call.args) if function_call.args else {}

                    with st.status(f"Executando ferramenta MCP `{tool_name}` via `{modelo_usado}`", expanded=True):
                        if tool_args:
                            st.markdown("**Argumentos**")
                            st.code(json.dumps(tool_args, ensure_ascii=False, indent=2), language="json")

                        resultado_mcp = await session.call_tool(tool_name, tool_args)

                        if resultado_mcp.content and len(resultado_mcp.content) > 0:
                            conteudo_retorno = resultado_mcp.content[0].text
                        else:
                            conteudo_retorno = "Ferramenta executada, porém sem retorno de texto."

                        st.markdown("**Retorno**")
                        st.code(conteudo_retorno, language="text")

                    mcp_chamado = True

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

                    response_final, modelo_final = chamar_gemini_com_fallback(client, contents, config)
                    
                    texto_extraido = ""
                    if response_final.candidates and response_final.candidates[0].content.parts:
                        for part in response_final.candidates[0].content.parts:
                            if hasattr(part, "text") and part.text:
                                texto_extraido += part.text

                    texto_final = texto_extraido if texto_extraido.strip() else response_final.text
                    return (texto_final or "Sem resposta do modelo."), modelo_final, mcp_chamado
                else:
                    texto_extraido = ""
                    if response.candidates and response.candidates[0].content.parts:
                        for part in response.candidates[0].content.parts:
                            if hasattr(part, "text") and part.text:
                                texto_extraido += part.text

                    texto = texto_extraido if texto_extraido.strip() else response.text
                    return (texto or "Sem resposta do modelo."), modelo_usado, mcp_chamado

    except (BaseExceptionGroup, ExceptionGroup) as eg:
        erros = extrair_erros_recursivos(eg)
        erros_fmt = "\n".join([f"- {e}" for e in erros])
        return f"❌ Erro no Subprocesso MCP:\n{erros_fmt}", None, False
    except Exception as e:
        return f"❌ Erro na integração MCP/Gemini: {type(e).__name__} - {str(e)}", None, False


def rodar_em_thread_limpa(prompt, historico):
    def worker():
        return anyio.run(processar_mcp_e_llm, prompt, historico, backend="asyncio")

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
# ENTRADA DO USUÁRIO
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

    with st.chat_message("assistant", avatar=AVATAR_MODELO):
        with st.spinner("Consultando MCP e processando com Gemini..."):
            resposta, modelo_usado, mcp_chamado = rodar_em_thread_limpa(prompt, historico_copia)
            st.markdown(resposta)
            if modelo_usado:
                st.caption(f"Respondido por `{modelo_usado}` · {datetime.now().strftime('%H:%M')}")

    if mcp_chamado:
        st.session_state.total_chamadas_mcp += 1

    if modelo_usado:
        st.session_state.ultimo_modelo = modelo_usado

    st.session_state.messages.append({"role": "model", "content": resposta})