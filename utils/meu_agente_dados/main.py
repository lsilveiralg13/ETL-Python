import os
import sys
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional

# Garante a importação dos módulos centrais
DIR_ATUAL = os.path.dirname(os.path.abspath(__file__))
if DIR_ATUAL not in sys.path:
    sys.path.insert(0, DIR_ATUAL)

# Importa a lógica que já roda no seu agente
from utils.meu_agente_dados.app import rodar_em_thread_limpa

app = FastAPI(
    title="Vetra API — Agente Consultivo de Dados",
    version="1.0.0",
    description="API do backend do agente de IA integrado ao servidor MCP"
)

# Modelo do corpo da requisição (JSON)
class Mensagem(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    prompt: str
    historico: Optional[List[Mensagem]] = []
    dialeto_sql: Optional[str] = "PostgreSQL"
    temperatura: Optional[float] = 0.2

# Endpoint principal da API
@app.post("/api/v1/chat")
async def chat_endpoint(payload: ChatRequest):
    try:
        # Converter mensagens do Pydantic para o formato de dicionário
        historico_dict = [m.model_dump() for m in payload.historico]
        
        # Executa a orquestração (LLM + MCP)
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

@app.get("/health")
def health_check():
    return {"status": "online", "agente": "Vetra"}