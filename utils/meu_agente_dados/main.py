from __future__ import annotations

import asyncio
import base64
import binascii
import json
import os
import sys
from typing import Any, AsyncIterator, List, Optional

import anyio
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from google import genai
from google.genai import types
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from pydantic import BaseModel, Field, field_validator

# Fix para conexões STDIO do MCP no Windows.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# =============================================================================
# CONFIGURAÇÃO DE CAMINHOS E LIMITES
# =============================================================================
load_dotenv()

DIR_ATUAL = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIR_ATUAL, "../../"))
if RAIZ_PROJETO not in sys.path:
    sys.path.insert(0, RAIZ_PROJETO)

CAMINHO_SERVIDOR_MCP = os.environ.get(
    "VETRA_MCP_SERVER_PATH",
    os.path.join(DIR_ATUAL, "servidor_mcp.py"),
)
if not os.path.isfile(CAMINHO_SERVIDOR_MCP):
    caminho_raiz = os.path.join(RAIZ_PROJETO, "servidor_mcp.py")
    caminho_cwd = os.path.abspath("servidor_mcp.py")
    if os.path.isfile(caminho_raiz):
        CAMINHO_SERVIDOR_MCP = caminho_raiz
    elif os.path.isfile(caminho_cwd):
        CAMINHO_SERVIDOR_MCP = caminho_cwd

MODELOS_PREFERENCIA = [
    modelo.strip()
    for modelo in os.environ.get(
        "GEMINI_MODELS",
        "gemini-3.8-flash,gemini-3.1-pro-preview,gemini-3-flash-preview",
    ).split(",")
    if modelo.strip()
]

MAX_PASSOS_MCP = int(os.environ.get("VETRA_MAX_PASSOS_MCP", "5"))
TIMEOUT_TOOL_SEGUNDOS = float(os.environ.get("VETRA_TOOL_TIMEOUT", "30"))
MAX_PROMPT_CHARS = int(os.environ.get("VETRA_MAX_PROMPT_CHARS", "100000"))
MAX_HISTORICO_MENSAGENS = int(os.environ.get("VETRA_MAX_HISTORY_MESSAGES", "10"))
MAX_ANEXOS = int(os.environ.get("VETRA_MAX_ATTACHMENTS", "10"))
MAX_ANEXO_BYTES = int(os.environ.get("VETRA_MAX_ATTACHMENT_BYTES", str(10 * 1024 * 1024)))
MAX_TOTAL_ANEXOS_BYTES = int(
    os.environ.get("VETRA_MAX_TOTAL_ATTACHMENT_BYTES", str(25 * 1024 * 1024))
)

# =============================================================================
# FASTAPI E MODELOS
# =============================================================================
app = FastAPI(
    title="Vetra API — Backend do Agente",
    version="1.1.0",
    description="API do backend orquestradora de LLM e MCP para a Vetra",
)


class Anexo(BaseModel):
    nome: str = Field(min_length=1, max_length=255)
    mime_type: str = Field(default="application/octet-stream", max_length=127)
    conteudo_b64: str = Field(min_length=1)


class Mensagem(BaseModel):
    role: str = Field(min_length=1, max_length=32)
    content: str = Field(default="", max_length=MAX_PROMPT_CHARS)

    @field_validator("role")
    @classmethod
    def validar_role(cls, valor: str) -> str:
        role = valor.strip().lower()
        if role == "model":
            return "assistant"
        if role not in {"user", "assistant", "system"}:
            raise ValueError("role deve ser user, assistant/model ou system")
        return role


class ChatRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=MAX_PROMPT_CHARS)
    historico: List[Mensagem] = Field(default_factory=list)
    anexos: List[Anexo] = Field(default_factory=list, max_length=MAX_ANEXOS)
    dialeto_sql: str = Field(default="PostgreSQL", min_length=1, max_length=64)
    temperatura: float = Field(default=0.2, ge=0.0, le=2.0)


# =============================================================================
# UTILITÁRIOS
# =============================================================================
def obter_schema_tool(tool: Any) -> dict[str, Any]:
    for atributo in ("parameters", "input_schema", "inputSchema"):
        valor = getattr(tool, atributo, None)
        if valor:
            if hasattr(valor, "model_dump"):
                return valor.model_dump(exclude_none=True)
            if hasattr(valor, "dict"):
                return valor.dict(exclude_none=True)
            if isinstance(valor, dict):
                return valor
    return {"type": "object", "properties": {}}


def extrair_texto_da_resposta(response: Any) -> str:
    partes_texto: list[str] = []
    try:
        texto = getattr(response, "text", None)
        if texto:
            partes_texto.append(str(texto))
    except Exception:
        pass

    for candidato in getattr(response, "candidates", None) or []:
        conteudo = getattr(candidato, "content", None)
        for parte in getattr(conteudo, "parts", None) or []:
            texto = getattr(parte, "text", None)
            if texto:
                partes_texto.append(str(texto))

    resultado: list[str] = []
    for texto in partes_texto:
        if texto and texto not in resultado:
            resultado.append(texto)
    return "\n".join(resultado).strip()


def normalizar_mime_type(mime_type: str, nome: str) -> str:
    mime = (mime_type or "application/octet-stream").lower().strip()
    extensao = os.path.splitext(nome)[1].lower()
    if "pdf" in mime or extensao == ".pdf":
        return "application/pdf"
    if "csv" in mime or extensao == ".csv":
        return "text/csv"
    if "text" in mime or extensao == ".txt":
        return "text/plain"
    if mime in {"image/png", "image/jpeg", "image/webp"}:
        return mime
    if extensao == ".png":
        return "image/png"
    if extensao in {".jpg", ".jpeg"}:
        return "image/jpeg"
    return mime


def decodificar_anexo(anexo: Any) -> tuple[bytes, str]:
    dados = anexo.model_dump() if hasattr(anexo, "model_dump") else dict(anexo)
    conteudo = str(dados.get("conteudo_b64") or "").strip()
    nome = os.path.basename(str(dados.get("nome") or "anexo"))
    if not conteudo:
        raise ValueError(f"O anexo '{nome}' não possui conteúdo.")

    try:
        bytes_arquivo = base64.b64decode(conteudo, validate=True)
    except (binascii.Error, ValueError):
        # Compatibilidade temporária com o app antigo, que enviava hexadecimal
        # no campo chamado conteudo_b64.
        try:
            bytes_arquivo = bytes.fromhex(conteudo)
        except ValueError as exc:
            raise ValueError(f"Conteúdo inválido no anexo '{nome}'.") from exc

    if not bytes_arquivo:
        raise ValueError(f"O anexo '{nome}' está vazio.")
    if len(bytes_arquivo) > MAX_ANEXO_BYTES:
        raise ValueError(
            f"O anexo '{nome}' excede o limite de "
            f"{MAX_ANEXO_BYTES // (1024 * 1024)} MB."
        )

    mime_type = normalizar_mime_type(str(dados.get("mime_type") or ""), nome)
    return bytes_arquivo, mime_type


def extrair_conteudo_mcp(resultado_mcp: Any) -> str:
    partes: list[str] = []
    for item in getattr(resultado_mcp, "content", None) or []:
        texto = getattr(item, "text", None)
        if texto:
            partes.append(str(texto))
            continue
        if hasattr(item, "model_dump"):
            partes.append(json.dumps(item.model_dump(), ensure_ascii=False, default=str))
        else:
            partes.append(str(item))
    return "\n".join(parte for parte in partes if parte).strip() or "Ferramenta executada sem retorno."


def obter_function_calls(response: Any) -> list[Any]:
    chamadas = getattr(response, "function_calls", None)
    return list(chamadas) if chamadas else []


def criar_env_subprocesso(api_key: str) -> dict[str, str]:
    env_vars = dict(os.environ)
    env_vars["PYTHONUNBUFFERED"] = "1"
    env_vars["PYTHONIOENCODING"] = "utf-8"
    env_vars["GEMINI_API_KEY"] = api_key
    caminhos = [DIR_ATUAL, RAIZ_PROJETO]
    pythonpath_existente = env_vars.get("PYTHONPATH")
    if pythonpath_existente:
        caminhos.append(pythonpath_existente)
    env_vars["PYTHONPATH"] = os.pathsep.join(caminhos)
    return env_vars


async def gerar_conteudo(client: genai.Client, modelo: str, contents: list[Any], config: Any) -> Any:
    # O SDK utilizado é síncrono; movê-lo para uma thread evita bloquear o loop.
    return await anyio.to_thread.run_sync(
        lambda: client.models.generate_content(
            model=modelo,
            contents=contents,
            config=config,
        )
    )


async def responder_diretamente_gemini(
    client: genai.Client,
    contents: list[Any],
    system_instruction: str,
    temperatura: float,
) -> tuple[str, Optional[str]]:
    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        temperature=temperatura,
    )
    erros: list[str] = []
    for modelo in MODELOS_PREFERENCIA:
        try:
            response = await gerar_conteudo(client, modelo, contents, config)
            texto = extrair_texto_da_resposta(response)
            if texto:
                return texto, modelo
            erros.append(f"{modelo}: resposta vazia")
        except Exception as exc:
            erros.append(f"{modelo}: {type(exc).__name__}: {exc}")
    detalhe = "; ".join(erros[-3:])
    return f"❌ Falha ao obter resposta dos modelos do Gemini. {detalhe}", None


# =============================================================================
# ORQUESTRAÇÃO MCP + GEMINI
# =============================================================================
async def processar_mcp_e_llm(
    prompt_usuario: str,
    historico_mensagens: list[dict[str, Any]],
    dialeto_sql: str,
    temperatura: float = 0.2,
    anexos: Optional[list[Any]] = None,
):
    api_key = (
        str(os.environ.get("GEMINI_API_KEY", ""))
        .replace('"', "")
        .replace("'", "")
        .replace("\n", "")
        .replace("\r", "")
        .strip()
    )
    if not api_key:
        return "⚠️ Erro: GEMINI_API_KEY não foi configurada.", None, False, None

    prompt_usuario = str(prompt_usuario or "").strip()
    if not prompt_usuario:
        return "⚠️ Erro: o prompt não pode ser vazio.", None, False, None

    client = genai.Client(api_key=api_key)
    system_instruction = (
        "Você é o Vetra, um especialista consultivo avançado em engenharia "
        "de dados, BI, RAG e SQL. Ajude o usuário a entender esquemas, criar "
        "queries eficientes, analisar anexos e indicadores. "
        f"O dialeto SQL preferido é: {dialeto_sql}."
    )

    historico_recente = list(historico_mensagens or [])[-MAX_HISTORICO_MENSAGENS:]
    contents: list[Any] = []
    for mensagem in historico_recente:
        if not isinstance(mensagem, dict):
            continue
        role_entrada = str(mensagem.get("role", "user")).lower()
        if role_entrada == "system":
            continue
        role = "user" if role_entrada == "user" else "model"
        texto = str(mensagem.get("content", ""))
        if texto:
            contents.append(
                types.Content(role=role, parts=[types.Part.from_text(text=texto)])
            )

    # O prompt atual é enviado separadamente. Remove eventual duplicata no fim
    # do histórico recebida de clientes antigos.
    if contents and contents[-1].role == "user":
        partes_finais = getattr(contents[-1], "parts", None) or []
        texto_final_historico = "".join(
            str(getattr(parte, "text", "") or "") for parte in partes_finais
        ).strip()
        if texto_final_historico == prompt_usuario:
            contents.pop()

    partes_mensagem_atual: list[Any] = []
    total_anexos = 0
    for anexo in anexos or []:
        bytes_arquivo, mime_type = decodificar_anexo(anexo)
        total_anexos += len(bytes_arquivo)
        if total_anexos > MAX_TOTAL_ANEXOS_BYTES:
            return (
                "⚠️ Erro: o total de anexos excede o limite permitido.",
                None,
                False,
                None,
            )
        partes_mensagem_atual.append(
            types.Part.from_bytes(data=bytes_arquivo, mime_type=mime_type)
        )
    partes_mensagem_atual.append(types.Part.from_text(text=prompt_usuario))
    contents.append(types.Content(role="user", parts=partes_mensagem_atual))

    # Se o servidor MCP não existir, responde diretamente em vez de tentar
    # iniciar um subprocesso inexistente.
    if not os.path.isfile(CAMINHO_SERVIDOR_MCP):
        texto, modelo = await responder_diretamente_gemini(
            client, contents, system_instruction, temperatura
        )
        aviso = f"⚠️ Servidor MCP não encontrado em '{CAMINHO_SERVIDOR_MCP}'.\n\n"
        return aviso + texto, modelo, False, None

    server_params = StdioServerParameters(
        command=sys.executable,
        args=["-u", CAMINHO_SERVIDOR_MCP],
        env=criar_env_subprocesso(api_key),
    )

    mcp_chamado = False
    retornos_mcp: list[str] = []

    try:
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                mcp_tools = await session.list_tools()
                function_declarations = [
                    types.FunctionDeclaration(
                        name=tool.name,
                        description=tool.description or "",
                        parameters=obter_schema_tool(tool),
                    )
                    for tool in mcp_tools.tools
                ]

                config = types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    tools=(
                        [types.Tool(function_declarations=function_declarations)]
                        if function_declarations
                        else None
                    ),
                    temperature=temperatura,
                )

                response = None
                modelo_usado: Optional[str] = None
                erros_modelos: list[str] = []
                for modelo in MODELOS_PREFERENCIA:
                    try:
                        response = await gerar_conteudo(client, modelo, contents, config)
                        modelo_usado = modelo
                        break
                    except Exception as exc:
                        erros_modelos.append(
                            f"{modelo}: {type(exc).__name__}: {exc}"
                        )

                if response is None or modelo_usado is None:
                    detalhe = "; ".join(erros_modelos[-3:])
                    return (
                        f"❌ Falha ao obter resposta dos modelos do Gemini. {detalhe}",
                        None,
                        False,
                        None,
                    )

                passo_atual = 0
                while obter_function_calls(response) and passo_atual < MAX_PASSOS_MCP:
                    passo_atual += 1
                    if getattr(response, "candidates", None) and response.candidates[0].content:
                        contents.append(response.candidates[0].content)

                    for function_call in obter_function_calls(response):
                        tool_name = function_call.name
                        tool_args = dict(function_call.args or {})
                        try:
                            resultado_mcp = await asyncio.wait_for(
                                session.call_tool(tool_name, tool_args),
                                timeout=TIMEOUT_TOOL_SEGUNDOS,
                            )
                            conteudo_retorno = extrair_conteudo_mcp(resultado_mcp)
                        except asyncio.TimeoutError:
                            conteudo_retorno = (
                                f"⚠️ A ferramenta '{tool_name}' excedeu o tempo limite."
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            conteudo_retorno = (
                                f"⚠️ Erro ao executar a ferramenta '{tool_name}': "
                                f"{type(exc).__name__}: {exc}"
                            )

                        mcp_chamado = True
                        retornos_mcp.append(conteudo_retorno)
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

                    response = await gerar_conteudo(
                        client, modelo_usado, contents, config
                    )

                texto_final = extrair_texto_da_resposta(response)
                retorno_consolidado = "\n\n".join(retornos_mcp) or None
                if not texto_final and retorno_consolidado:
                    texto_final = (
                        "Consulta finalizada. Dados obtidos:\n\n"
                        + retorno_consolidado
                    )
                if not texto_final:
                    texto_final = "A consulta foi concluída, mas o modelo não retornou texto."
                if obter_function_calls(response) and passo_atual >= MAX_PASSOS_MCP:
                    texto_final += "\n\n⚠️ O limite de etapas MCP foi atingido."

                return texto_final, modelo_usado, mcp_chamado, retorno_consolidado

    except asyncio.CancelledError:
        raise
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:
        # O código original dizia que faria fallback direto, mas apenas devolvia
        # um aviso. Aqui o fallback é realmente executado.
        texto_direto, modelo_direto = await responder_diretamente_gemini(
            client, contents, system_instruction, temperatura
        )
        aviso = (
            "⚠️ Não foi possível carregar as ferramentas MCP "
            f"({type(exc).__name__}: {exc}). Resposta gerada diretamente pelo Gemini.\n\n"
        )
        return aviso + texto_direto, modelo_direto, False, None


# =============================================================================
# ENDPOINTS
# =============================================================================
@app.get("/health")
def health_check():
    return {
        "status": "online",
        "agente": "Vetra",
        "mcp_server_encontrado": os.path.isfile(CAMINHO_SERVIDOR_MCP),
    }


@app.post("/chat")
async def chat_endpoint(payload: ChatRequest):
    try:
        historico = [m.model_dump() for m in payload.historico]
        anexos = [a.model_dump() for a in payload.anexos]
        resposta, modelo_usado, mcp_chamado, retorno_mcp = await processar_mcp_e_llm(
            payload.prompt,
            historico,
            payload.dialeto_sql,
            payload.temperatura,
            anexos,
        )
        return {
            "status": "sucesso",
            "resposta": resposta,
            "modelo_usado": modelo_usado,
            "mcp_chamado": mcp_chamado,
            "dados_mcp_raw": retorno_mcp,
        }
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/chat/stream")
async def chat_stream_endpoint(payload: ChatRequest):
    async def event_generator() -> AsyncIterator[str]:
        try:
            yield "data: " + json.dumps(
                {"chunk": "⌛ *Consultando inteligência de dados e MCP...*\n\n"},
                ensure_ascii=False,
            ) + "\n\n"

            historico = [m.model_dump() for m in payload.historico]
            anexos = [a.model_dump() for a in payload.anexos]
            resposta, modelo_usado, mcp_chamado, retorno_mcp = await processar_mcp_e_llm(
                payload.prompt,
                historico,
                payload.dialeto_sql,
                payload.temperatura,
                anexos,
            )
            evento_final = {
                "resposta": resposta,
                "modelo_usado": modelo_usado,
                "mcp_chamado": mcp_chamado,
                "dados_mcp_raw": retorno_mcp,
            }
            yield "data: " + json.dumps(evento_final, ensure_ascii=False) + "\n\n"
            yield "data: [DONE]\n\n"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            yield "data: " + json.dumps(
                {"error": str(exc)}, ensure_ascii=False
            ) + "\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
