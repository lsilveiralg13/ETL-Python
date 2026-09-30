import os
import sys
import anyio
import concurrent.futures
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional

# Garante o path da raiz do projeto
DIR_ATUAL = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIR_ATUAL, "../../"))
if RAIZ_PROJETO not in sys.path:
    sys.path.insert(0, RAIZ_PROJETO)

app = FastAPI(
    title="Vetra API — Agente Consultivo de Dados",
    version="1.0.0"
)

# Modelo do corpo da requisição
class Mensagem(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    prompt: str
    historico: Optional[List[Mensagem]] = []
    dialeto_sql: Optional[str] = "PostgreSQL"
    temperatura: Optional[float] = 0.2

@app.get("/health")
def health_check():
    return {"status": "online", "agente": "Vetra"}

@app.post("/api/v1/chat")
async def chat_endpoint(payload: ChatRequest):
    try:
        # Tenta importar do app.py caso tenhas mantido a função lá
        from utils.meu_agente_dados.app import processar_mcp_e_llm
        
        historico_dict = [m.model_dump() for m in payload.historico]
        
        # Executa a função assíncrona do agente em thread limpa
        def worker():
            return anyio.run(
                processar_mcp_e_llm, 
                payload.prompt, 
                historico_dict, 
                payload.dialeto_sql, 
                payload.temperatura, 
                backend="asyncio"
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker)
            resposta, modelo_usado, mcp_chamado, retorno_mcp = future.result()

        return {
            "status": "sucesso",
            "resposta": resposta,
            "modelo_usado": modelo_usado,
            "mcp_chamado": mcp_chamado,
            "dados_mcp_raw": retorno_mcp
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))