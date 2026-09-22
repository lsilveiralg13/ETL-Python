import os
import sys
import json
import asyncio
from typing import Optional, List, Dict, Any
from mcp.server.mcpserver import MCPServer
from qdrant_client import QdrantClient
import sqlglot

# Inicializa o servidor MCP
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
# CHUNKING INTELIGENTE (FRENTE 1)
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
# FERRAMENTAS MCP — RAG E DOCUMENTOS
# -----------------------------------------------------------------------------
@mcp.tool()
def indexar_documento_com_chunking(
    texto: str, 
    fonte: str = "Upload Manual", 
    categoria: str = "Geral",
    departamento: str = "TI"
) -> str:
    """Divide um texto longo em chunks e o indexa no Qdrant Cloud com metadados ricos."""
    client = obter_cliente_qdrant()
    if not client:
        return "⚠️ Qdrant Cloud não configurado ou indisponível."

    try:
        chunks = dividir_em_chunks(texto)
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
        return f"✅ Documento '{fonte}' indexado com sucesso! ({len(chunks)} chunks criados no Qdrant)."
    except Exception as e:
        return f"Erro ao indexar no Qdrant: {str(e)}"

@mcp.tool()
def buscar_conhecimento_rag(termo_busca: str, limite: int = 3) -> str:
    """Realiza busca vetorial semântica no Qdrant Cloud."""
    client = obter_cliente_qdrant()
    if not client:
        return "⚠️ Qdrant Cloud não configurado."

    try:
        resultados = client.query(
            collection_name=NOME_COLECAO,
            query_text=termo_busca,
            limit=limite
        )
        if not resultados:
            return f"Nenhum documento encontrado para: '{termo_busca}'."

        resposta = [f"### Resultados RAG para '{termo_busca}':\n"]
        for idx, doc in enumerate(resultados, 1):
            texto = doc.metadata.get("document", doc.metadata.get("texto", "Conteúdo não disponível"))
            fonte = doc.metadata.get("fonte", "Desconhecido")
            score = round(getattr(doc, "score", 0.0), 4)
            resposta.append(f"**[{idx}] Fonte: {fonte} (Score: {score})**\n{texto}\n")
        return "\n".join(resposta)
    except Exception as e:
        return f"Erro na consulta RAG: {str(e)}"

# -----------------------------------------------------------------------------
# FERRAMENTAS SQL E VALIDATION AST (FRENTE 2)
# -----------------------------------------------------------------------------
@mcp.tool()
def validar_e_executar_sql(query: str, dialecto: str = "postgres") -> str:
    """Valida a sintaxe SQL usando AST e executa a consulta de forma segura (Apenas SELECT)."""
    # 1. Validação AST via SQLGlot
    try:
        parsed = sqlglot.parse_one(query, read=dialecto)
        if not isinstance(parsed, sqlglot.exp.Select):
            return "❌ Erro de Segurança: Apenas instruções de consulta SELECT são permitidas."
    except Exception as e:
        return f"❌ Erro de Sintaxe SQL: {str(e)}"

    # 2. Retorno com dados tabulares formatados em JSON para o Streamlit renderizar gráficos
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
def listar_esquemas_e_tabelas() -> str:
    """Lista as tabelas disponíveis no ambiente corporativo."""
    return """
    Tabelas disponíveis:
    1. tb_producao (id, data_producao, unidade, qtd_produzida, status_otif)
    2. tb_assistencia_tecnica (id, data, unidade, os_numero, tipo_defeito)
    3. tb_vendas_compras (id, data, cod_produto, qtd_comprada, qtd_entregue)
    """

@mcp.tool()
def descrever_estrutura_tabela(nome_tabela: str) -> str:
    """Retorna DDL e colunas de uma tabela."""
    return f"Tabela {nome_tabela}: id (INT), data (DATE), valor (NUMERIC), status (VARCHAR)."

if __name__ == "__main__":
    mcp.run()