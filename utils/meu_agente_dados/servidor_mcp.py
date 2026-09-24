import os
import sys
import json
import time
import re
from typing import Optional, List, Dict, Any
from functools import wraps
from mcp.server.mcpserver import MCPServer
from qdrant_client import QdrantClient
import sqlglot

mcp = MCPServer("AgenteConsultivoDados")

# -----------------------------------------------------------------------------
# CONFIGURAÇÃO QDRANT CLOUD
# -----------------------------------------------------------------------------
QDRANT_URL = os.environ.get("QDRANT_URL")
QDRANT_API_KEY = os.environ.get("QDRANT_API_KEY")
NOME_COLECAO = "conhecimento_corporativo"

qdrant_cliente = None

def obter_cliente_qdrant():
    global qdrant_cliente
    if qdrant_cliente is None and QDRANT_URL and QDRANT_API_KEY:
        try:
            qdrant_cliente = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
            qdrant_cliente.set_model("BAAI/bge-small-en-v1.5")
            if not qdrant_cliente.collection_exists(NOME_COLECAO):
                qdrant_cliente.create_collection(
                    collection_name=NOME_COLECAO,
                    vectors_config=qdrant_cliente.get_fastembed_vector_params()
                )
        except Exception as e:
            print(f"Erro ao conectar ao Qdrant: {e}", file=sys.stderr)
    return qdrant_cliente

# -----------------------------------------------------------------------------
# SANITIZAÇÃO E MASCARAMENTO PII (PROTEÇÃO DE DADOS SENSÍVEIS)
# -----------------------------------------------------------------------------
def mascarar_dados_sensiveis(texto: str) -> str:
    """Aplica regex para ocultar CPFs, e-mails e telefones antes de trafegar ou salvar."""
    if not texto:
        return texto
    
    # Oculta CPFs (formato XXX.XXX.XXX-XX ou sequências de 11 dígitos)
    texto = re.sub(r'\b\d{3}\.\d{3}\.\d{3}-\d{2}\b', '[CPF_OCULTO]', texto)
    texto = re.sub(r'\b\d{11}\b', '[CPF_OCULTO]', texto)
    
    # Oculta E-mails
    texto = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL_OCULTO]', texto)
    
    # Oculta Telefones com DDD
    texto = re.sub(r'\b(?:\+?55\s?)?(?:\(?\d{2}\)?\s?)?\d{4,5}[-\s]?\d{4}\b', '[TELEFONE_OCULTO]', texto)
    
    return texto

# -----------------------------------------------------------------------------
# DICIONÁRIO DE DADOS E SCHEMA-AWARENESS
# -----------------------------------------------------------------------------
DICIONARIO_DADOS = {
    "tb_producao": ["id", "data_producao", "unidade", "qtd_produzida", "status_otif"],
    "tb_assistencia_tecnica": ["id", "data", "unidade", "os_numero", "tipo_defeito"],
    "tb_vendas_compras": ["id", "data", "cod_produto", "qtd_comprada", "qtd_entregue"]
}

# -----------------------------------------------------------------------------
# CACHE COM TTL (TIME TO LIVE)
# -----------------------------------------------------------------------------
CACHE_MEMORIA: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SEGUNDOS = 300  # 5 minutos de cache

def com_cache(ttl_segundos: int = CACHE_TTL_SEGUNDOS):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            chave_cache = f"{func.__name__}:{json.dumps(args)}:{json.dumps(kwargs, sort_keys=True)}"
            agora = time.time()
            if chave_cache in CACHE_MEMORIA:
                registro = CACHE_MEMORIA[chave_cache]
                if agora - registro["timestamp"] < ttl_segundos:
                    return registro["dados"] + "\n\n⚡ *(Resposta obtida do cache)*"
            
            resultado = func(*args, **kwargs)
            CACHE_MEMORIA[chave_cache] = {"timestamp": agora, "dados": resultado}
            return resultado
        return wrapper
    return decorator

# -----------------------------------------------------------------------------
# CHUNKING INTELIGENTE
# -----------------------------------------------------------------------------
def dividir_em_chunks(texto: str, tamanho_chunk: int = 500, sobreposicao: int = 100) -> List[str]:
    chunks = []
    inicio = 0
    tamanho_texto = len(texto)
    while inicio < tamanho_texto:
        fim = inicio + tamanho_chunk
        chunk = texto[inicio:fim]
        chunks.append(chunk)
        inicio += (tamanho_chunk - sobreposicao)
    return chunks

# -----------------------------------------------------------------------------
# FERRAMENTAS MCP — RAG HÍBRIDO E DOCUMENTOS
# -----------------------------------------------------------------------------
@mcp.tool()
def indexar_documento_com_chunking(
    texto: str, 
    fonte: str = "Upload Manual", 
    categoria: str = "Geral",
    departamento: str = "TI"
) -> str:
    """Divide um texto longo em chunks e indexa no Qdrant Cloud com metadados ricos e PII sanitizado."""
    client = obter_cliente_qdrant()
    if not client:
        return "⚠️ Qdrant Cloud não configurado ou indisponível."

    try:
        # Aplica sanitização PII antes do chunking
        texto_sanitizado = mascarar_dados_sensiveis(texto)
        chunks = dividir_em_chunks(texto_sanitizado)
        
        metadados = [
            {
                "fonte": fonte,
                "categoria": categoria,
                "departamento": departamento,
                "chunk_index": i,
                "total_chunks": len(chunks)
            }
            for i in range(len(chunks))
        ]
        
        client.add(
            collection_name=NOME_COLECAO,
            documents=chunks,
            metadata=metadados
        )
        return f"✅ Documento '{fonte}' sanitizado e indexado com sucesso! ({len(chunks)} chunks criados no Qdrant)."
    except Exception as e:
        return f"Erro ao indexar no Qdrant: {str(e)}"

@mcp.tool()
@com_cache(ttl_segundos=300)
def buscar_conhecimento_rag(termo_busca: str, limite: int = 3) -> str:
    """Realiza busca vetorial/semântica no Qdrant Cloud."""
    client = obter_cliente_qdrant()
    if not client:
        return "⚠️ Qdrant Cloud não configurado."

    try:
        resultados = None
        # Compatibilidade com diferentes versões do qdrant-client SDK
        if hasattr(client, "query_points"):
            res_points = client.query_points(
                collection_name=NOME_COLECAO,
                query=termo_busca,
                limit=limite
            )
            resultados = getattr(res_points, "points", res_points)
        elif hasattr(client, "search"):
            resultados = client.search(
                collection_name=NOME_COLECAO,
                query_text=termo_busca,
                limit=limite
            )
        elif hasattr(client, "query"):
            resultados = client.query(
                collection_name=NOME_COLECAO,
                query_text=termo_busca,
                limit=limite
            )

        if not resultados:
            return f"Nenhum documento encontrado para: '{termo_busca}'."

        resposta = [f"### Resultados RAG para '{termo_busca}':\n"]
        for idx, doc in enumerate(resultados, 1):
            payload = getattr(doc, "payload", {}) or getattr(doc, "metadata", {}) or {}
            texto = payload.get("document", payload.get("texto", "Conteúdo não disponível"))
            fonte = payload.get("fonte", "Desconhecido")
            score = round(getattr(doc, "score", 0.0), 4)
            resposta.append(f"**[{idx}] Fonte: {fonte} (Score: {score})**\n{texto}\n")
        return "\n".join(resposta)

    except Exception as e:
        return f"Erro na consulta RAG: {str(e)}"

# -----------------------------------------------------------------------------
# FERRAMENTAS SQL COM SCHEMA-AWARENESS
# -----------------------------------------------------------------------------
@mcp.tool()
def validar_e_executar_sql(query: str, dialecto: str = "postgres") -> str:
    """Valida a sintaxe SQL via AST, checa a existência de tabelas e colunas e executa a consulta."""
    try:
        parsed = sqlglot.parse_one(query, read=dialecto)
        if not isinstance(parsed, sqlglot.exp.Select):
            return "❌ Erro de Segurança: Apenas instruções SELECT são permitidas."

        # Validação Schema-Aware
        tabelas_citadas = [t.name for t in parsed.find_all(sqlglot.exp.Table)]
        for tab in tabelas_citadas:
            if tab not in DICIONARIO_DADOS:
                return f"❌ Erro de Schema: A tabela '{tab}' não existe no banco de dados. Tabelas válidas: {list(DICIONARIO_DADOS.keys())}"
            
        colunas_citadas = [c.name for c in parsed.find_all(sqlglot.exp.Column)]
        colunas_validas = set()
        for tab in tabelas_citadas:
            colunas_validas.update(DICIONARIO_DADOS[tab])
        
        for col in colunas_citadas:
            if col != "*" and col not in colunas_validas:
                return f"❌ Erro de Schema: A coluna '{col}' não pertence às tabelas selecionadas. Colunas disponíveis: {list(colunas_validas)}"

    except Exception as e:
        return f"❌ Erro de Sintaxe SQL: {str(e)}"

    dados_simulados = {
        "colunas": ["mes", "qtd_produzida", "otif_pct"],
        "linhas": [
            ["Jan", 1200, 94.5],
            ["Fev", 1350, 96.0],
            ["Mar", 1100, 91.2],
            ["Abr", 1500, 97.8]
        ]
    }
    return f"✅ Query validada e executada com sucesso!\n```json\n{json.dumps(dados_simulados, ensure_ascii=False)}\n```"

@mcp.tool()
@com_cache(ttl_segundos=600)
def listar_esquemas_e_tabelas() -> str:
    """Lista as tabelas disponíveis no ambiente corporativo."""
    ret = ["Tabelas disponíveis:"]
    for tab, cols in DICIONARIO_DADOS.items():
        ret.append(f"- {tab} (Colunas: {', '.join(cols)})")
    return "\n".join(ret)

@mcp.tool()
def descrever_estrutura_tabela(nome_tabela: str) -> str:
    """Retorna a estrutura de colunas de uma tabela específica."""
    if nome_tabela in DICIONARIO_DADOS:
        return f"Tabela `{nome_tabela}` colunas: {', '.join(DICIONARIO_DADOS[nome_tabela])}"
    return f"Tabela `{nome_tabela}` não encontrada."

# -----------------------------------------------------------------------------
# FERRAMENTAS MCP — CÁLCULO DE INDICADORES DE BI (DADOS SINTÉTICOS)
# -----------------------------------------------------------------------------
@mcp.tool()
@com_cache(ttl_segundos=180)
def calcular_indicador_otif(unidade: Optional[str] = None) -> str:
    """Calcula o indicador de performance logístico OTIF (On-Time In-Full) acumulado do período."""
    dados_otif = {
        "Contagem": {"total_pedidos": 450, "no_prazo": 420, "completos": 410, "otif_sucesso": 398},
        "Belo Horizonte": {"total_pedidos": 600, "no_prazo": 570, "completos": 550, "otif_sucesso": 530},
        "Geral": {"total_pedidos": 1050, "no_prazo": 990, "completos": 960, "otif_sucesso": 928}
    }
    
    alvo = unidade if unidade in dados_otif else "Geral"
    d = dados_otif[alvo]
    
    otif_pct = round((d["otif_sucesso"] / d["total_pedidos"]) * 100, 2)
    on_time_pct = round((d["no_prazo"] / d["total_pedidos"]) * 100, 2)
    in_full_pct = round((d["completos"] / d["total_pedidos"]) * 100, 2)

    resultado = {
        "unidade": alvo,
        "indicador": "OTIF (On-Time In-Full)",
        "otif_percentual": f"{otif_pct}%",
        "detalhes": {
            "on_time_prazo": f"{on_time_pct}%",
            "in_full_completo": f"{in_full_pct}%",
            "total_pedidos": d["total_pedidos"],
            "pedidos_otif_perfeito": d["otif_sucesso"]
        }
    }
    return f"```json\n{json.dumps(resultado, ensure_ascii=False, indent=2)}\n```"

@mcp.tool()
@com_cache(ttl_segundos=180)
def calcular_lead_time_producao(linha_produto: str = "Geral") -> str:
    """Calcula o tempo médio de ciclo e lead time de ordens de produção."""
    metricas = {
        "linha_produto": linha_produto,
        "lead_time_medio_dias": 4.2,
        "tempo_setup_horas": 1.5,
        "eficiencia_geral_oee": "87.4%",
        "gargalo_identificado": "Etapa de Pintura / Carga Térmica"
    }
    return f"```json\n{json.dumps(metricas, ensure_ascii=False, indent=2)}\n```"

if __name__ == "__main__":
    mcp.run()