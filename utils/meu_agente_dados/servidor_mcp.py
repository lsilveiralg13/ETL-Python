import os
import sys
import asyncio
from typing import Optional
from mcp.server.mcpserver import MCPServer
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

# Inicializa o servidor MCP 2.x
mcp = MCPServer("AgenteConsultivoDados")

# -----------------------------------------------------------------------------
# INICIALIZAÇÃO E CONEXÃO COM QDRANT CLOUD
# -----------------------------------------------------------------------------
QDRANT_URL = os.environ.get("QDRANT_URL")
QDRANT_API_KEY = os.environ.get("QDRANT_API_KEY")
NOME_COLECAO = "conhecimento_corporativo"

qdrant_cliente = None

def obter_cliente_qdrant():
    global qdrant_cliente
    if qdrant_cliente is None and QDRANT_URL and QDRANT_API_KEY:
        try:
            # Conecta ao cluster cloud usando FastEmbed para gerar embeddings de 384 dimensões automaticamente
            qdrant_cliente = QdrantClient(
                url=QDRANT_URL,
                api_key=QDRANT_API_KEY
            )
            # Garante que a coleção existe
            qdrant_cliente.set_model("BAAI/bge-small-en-v1.5")
            if not qdrant_cliente.collection_exists(NOME_COLECAO):
                qdrant_cliente.create_collection(
                    collection_name=NOME_COLECAO,
                    vectors_config=qdrant_cliente.get_fastembed_vector_params()
                )
        except Exception as e:
            print(f"Erro ao conectar ao Qdrant Cloud: {e}", file=sys.stderr)
    return qdrant_cliente

# -----------------------------------------------------------------------------
# FERRAMENTAS EXISTENTES (BANCO DE DADOS E REGRAS)
# -----------------------------------------------------------------------------
@mcp.tool()
def listar_esquemas_e_tabelas(schema: Optional[str] = None) -> str:
    """Retorna a lista de tabelas e visões disponíveis no banco de dados."""
    return """
    Tabelas disponíveis no ambiente corporativo:
    1. tb_producao (id, data_producao, unidade, qtd_produzida, status_otif, status_sla)
    2. tb_assistencia_tecnica (id, data, unidade, os_numero, tipo_defeito)
    3. tb_vendas_compras (id, data, cod_produto, qtd_comprada, qtd_entregue, pendente)
    """

@mcp.tool()
def descrever_estrutura_tabela(nome_tabela: str) -> str:
    """Retorna a estrutura, DDL e colunas de uma tabela específica."""
    nome_limpo = nome_tabela.lower().strip()
    if "producao" in nome_limpo:
        return "Tabela: tb_producao | Colunas: id (INT), data_producao (DATE), unidade (VARCHAR), qtd_produzida (INT), status_otif (VARCHAR), status_sla (VARCHAR)"
    return f"Estrutura da tabela '{nome_tabela}' consultada com sucesso."

@mcp.tool()
def executar_query_sql(query: str, dialecto: str = "Databricks SQL") -> str:
    """Executa uma consulta SQL de leitura (SELECT) no banco de dados."""
    query_clean = query.strip().upper()
    if any(cmd in query_clean for cmd in ["DROP", "DELETE", "TRUNCATE", "ALTER"]):
        return "Erro de Segurança: Apenas consultas de leitura (SELECT) são permitidas."
    return f"Query executada com sucesso no dialeto {dialecto}.\n[Resultado Simulado]: 100 registros processados."

# -----------------------------------------------------------------------------
# NOVAS FERRAMENTAS RAG (QDRANT CLOUD)
# -----------------------------------------------------------------------------
@mcp.tool()
def buscar_conhecimento_rag(termo_busca: str, limite: int = 3) -> str:
    """
    Realiza uma busca vetorial semântica (RAG) no Qdrant Cloud por documentos, manuais, 
    politicas internas ou documentações técnicas relevantes.
    """
    client = obter_cliente_qdrant()
    if not client:
        return "⚠️ Qdrant Cloud não configurado ou indisponível. Verifique QDRANT_URL e QDRANT_API_KEY."

    try:
        # Busca vetorial híbrida/semântica no Qdrant
        resultados = client.query(
            collection_name=NOME_COLECAO,
            query_text=termo_busca,
            limit=limite
        )
        
        if not resultados:
            return f"Nenhum documento vetorial encontrado para o termo: '{termo_busca}'."

        resposta = [f"### Resultados da Busca Vetorial (RAG) para '{termo_busca}':\n"]
        for idx, doc in enumerate(resultados, 1):
            texto = doc.metadata.get("texto", "Conteúdo indisponível")
            fonte = doc.metadata.get("fonte", "Documento Interno")
            score = round(doc.score, 4)
            resposta.append(f"**[{idx}] Fonte: {fonte} (Relevância: {score})**\n{texto}\n")
        
        return "\n".join(resposta)
    except Exception as e:
        return f"Erro ao realizar consulta no Qdrant Cloud: {str(e)}"

@mcp.tool()
def indexar_documento_rag(texto: str, fonte: str = "Manual Interno") -> str:
    """
    Adiciona e indexa um novo trecho de conhecimento ou documento no banco vetorial Qdrant Cloud.
    """
    client = obter_cliente_qdrant()
    if not client:
        return "⚠️ Qdrant Cloud não configurado. Impossível indexar documento."

    try:
        client.add(
            collection_name=NOME_COLECAO,
            documents=[texto],
            metadata=[{"fonte": fonte, "data_criacao": str(asyncio.get_event_loop().time())}]
        )
        return f"✅ Documento da fonte '{fonte}' indexado com sucesso no Qdrant Cloud!"
    except Exception as e:
        return f"Erro ao indexar documento no Qdrant: {str(e)}"

# -----------------------------------------------------------------------------
# RUNNER
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run()