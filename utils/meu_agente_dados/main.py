import os
import sys
import json
import asyncio
import concurrent.futures
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import anyio

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from google import genai
from google.genai import types
from dotenv import load_dotenv

# Fix para o bug de conexões STDIO do MCP no Windows
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# =============================================================================
# CONFIGURAÇÃO DE CAMINHOS DO PROJETO
# =============================================================================
load_dotenv()  # Carrega as variáveis de ambiente do arquivo .env

DIR_ATUAL = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIR_ATUAL, "../../"))
if RAIZ_PROJETO not in sys.path:
    sys.path.insert(0, RAIZ_PROJETO)

# Garante a busca do servidor MCP no diretório do main.py ou na raiz /app do Docker
CAMINHO_SERVIDOR_MCP = os.path.join(DIR_ATUAL, "servidor_mcp.py")
if not os.path.exists(CAMINHO_SERVIDOR_MCP):
    CAMINHO_SERVIDOR_MCP = os.path.abspath("servidor_mcp.py")

MODELOS_PREFERENCIA = [
    "gemini-3.8-flash",
    "gemini-3.1-pro-preview",
    "gemini-3-flash-preview",
]

# =============================================================================
# INICIALIZAÇÃO FASTAPI
# =============================================================================
app = FastAPI(
    title="Vetra API — Backend do Agente",
    version="1.0.0",
    description="API do backend orquestradora de LLM e MCP para a Vetra"
)

class Mensagem(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    prompt: str
    historico: Optional[List[Mensagem]] = []
    dialeto_sql: Optional[str] = "PostgreSQL"
    temperatura: Optional[float] = 0.2


# =============================================================================
# LÓGICA DE ORQUESTRAÇÃO (MCP + GEMINI)
# =============================================================================
def obter_schema_tool(tool):
    if hasattr(tool, "parameters") and tool.parameters:
        return tool.parameters
    elif hasattr(tool, "input_schema") and tool.input_schema:
        return tool.input_schema
    elif hasattr(tool, "inputSchema") and tool.inputSchema:
        return tool.inputSchema
    return {"type": "object", "properties": {}}


def extrair_texto_da_resposta(response):
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


async def processar_mcp_e_llm(prompt_usuario, historico_mensagens, dialeto_sql, temperatura=0.2):
    api_key_raw = os.environ.get("GEMINI_API_KEY", "")
    api_key = str(api_key_raw).replace('"', '').replace("'", "").replace('\n', '').replace('\r', '').strip()

    if not api_key:
        return "⚠️ Erro: GEMINI_API_KEY não foi configurada nas variáveis de ambiente.", None, False, None

    # Injeta variáveis de ambiente no subprocesso MCP para localização de módulos (PYTHONPATH)
    env_vars = dict(os.environ)
    env_vars["PYTHONUNBUFFERED"] = "1"
    env_vars["PYTHONIOENCODING"] = "utf-8"
    env_vars["GEMINI_API_KEY"] = api_key
    env_vars["PYTHONPATH"] = RAIZ_PROJETO

    server_params = StdioServerParameters(
        command=sys.executable,
        args=["-u", CAMINHO_SERVIDOR_MCP],
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

                system_instruction = f"""
                Você é o Vetra, um especialista consultivo avançado em engenharia de dados, BI, RAG e SQL.
                Sua função é ajudar o usuário a entender seus esquemas, criar queries eficientes e analisar indicadores.
                O dialeto SQL preferido do usuário é: {dialeto_sql}.
                """

                historico_recente = historico_mensagens[-10:] if len(historico_mensagens) > 10 else historico_mensagens

                contents = []
                for m in historico_recente:
                    role = "user" if m.get("role") == "user" else "model"
                    contents.append(
                        types.Content(
                            role=role,
                            parts=[types.Part.from_text(text=m.get("content", ""))]
                        )
                    )

                if not contents or contents[-1].role != "user" or contents[-1].parts[0].text != prompt_usuario:
                    contents.append(
                        types.Content(
                            role="user",
                            parts=[types.Part.from_text(text=prompt_usuario)]
                        )
                    )

                config = types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    tools=[types.Tool(function_declarations=function_declarations)],
                    temperature=temperatura,
                )

                response = None
                modelo_usado = None
                for mod in MODELOS_PREFERENCIA:
                    try:
                        response = client.models.generate_content(
                            model=mod,
                            contents=contents,
                            config=config,
                        )
                        modelo_usado = mod
                        break
                    except Exception:
                        continue

                if not response:
                    return "❌ Falha ao obter resposta dos modelos do Gemini.", None, False, None

                MAX_PASSOS_MCP = 5
                passo_atual = 0

                while getattr(response, "function_calls", None) and passo_atual < MAX_PASSOS_MCP:
                    passo_atual += 1
                    function_call = response.function_calls[0]
                    tool_name = function_call.name
                    tool_args = dict(function_call.args) if function_call.args else {}

                    try:
                        resultado_mcp = await asyncio.wait_for(
                            session.call_tool(tool_name, tool_args),
                            timeout=30.0
                        )
                        if resultado_mcp.content and len(resultado_mcp.content) > 0:
                            conteudo_retorno = resultado_mcp.content[0].text
                        else:
                            conteudo_retorno = "Ferramenta executada sem retorno."
                    except asyncio.TimeoutError:
                        conteudo_retorno = "⚠️ Erro: Ferramenta MCP excedeu o tempo limite."

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

                    response = client.models.generate_content(
                        model=modelo_usado,
                        contents=contents,
                        config=config,
                    )

                texto_final = extrair_texto_da_resposta(response)
                if not texto_final and conteudo_retorno:
                    texto_final = f"Consulta finalizada. Dados obtidos:\n\n{conteudo_retorno}"

                return texto_final, modelo_usado, mcp_chamado, conteudo_retorno

    except BaseException as e:
        msg_erro = str(e)
        if hasattr(e, "exceptions"):
            detalhes = "; ".join([str(sub) for sub in e.exceptions])
            msg_erro = f"{msg_erro} -> ({detalhes})"
        
        return f"⚠️ Aviso: Não foi possível carregar as ferramentas MCP no container ({msg_erro}). " \
               f"Processando resposta diretamente via Gemini...", None, False, None


def rodar_em_thread_limpa(prompt, historico, dialeto_sql, temperatura):
    try:
        return asyncio.run(processar_mcp_e_llm(prompt, historico, dialeto_sql, temperatura))
    except Exception as e:
        return f"❌ Erro ao executar assincronamente: {str(e)}", None, False, None


# =============================================================================
# ENDPOINTS DA API
# =============================================================================
@app.get("/health")
def health_check():
    return {"status": "online", "agente": "Vetra"}


@app.post("/chat")
def chat_endpoint(payload: ChatRequest):
    """Endpoint síncrono padrão para garantir estabilidade máxima."""
    try:
        historico_dict = [m.model_dump() for m in payload.historico] if payload.historico else []

        resposta, modelo_usado, mcp_chamado, retorno_mcp = rodar_em_thread_limpa(
            payload.prompt,
            historico_dict,
            payload.dialeto_sql,
            payload.temperatura
        )

        return {
            "status": "sucesso",
            "resposta": resposta,
            "modelo_usado": modelo_usado,
            "mcp_chamado": mcp_chamado,
            "dados_mcp_raw": retorno_mcp
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/chat/stream")
async def chat_stream_endpoint(payload: ChatRequest):
    """Endpoint SSE assíncrono e não-bloqueante seguro para Windows/Linux/Fly.io."""
    async def event_generator():
        try:
            historico_dict = [m.model_dump() for m in payload.historico] if payload.historico else []

            # Notifica que o processamento começou
            yield f"data: {json.dumps({'chunk': '⌛ *Consultando inteligência de dados e MCP...*\n\n'})}\n\n"

            # Executa a thread síncrona sem travar o EventLoop do asyncio
            resposta, modelo_usado, mcp_chamado, retorno_mcp = await anyio.to_thread.run_sync(
                rodar_em_thread_limpa,
                payload.prompt,
                historico_dict,
                payload.dialeto_sql,
                payload.temperatura
            )

            # Envia a estrutura final consolidada
            payload_resposta = {
                "resposta": resposta,
                "modelo_usado": modelo_usado,
                "mcp_chamado": mcp_chamado,
                "dados_mcp_raw": retorno_mcp
            }
            yield f"data: {json.dumps(payload_resposta)}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )