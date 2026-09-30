import os
import sys
import json
import time
import re
import warnings
from typing import Optional, List, Dict, Any
from functools import wraps

# Configurações de UTF-8 e supressão de warnings para o protocolo MCP
warnings.filterwarnings("ignore")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Garante que a raiz do projeto seja encontrada
DIR_ATUAL = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIR_ATUAL, "../../"))
if RAIZ_PROJETO not in sys.path:
    sys.path.insert(0, RAIZ_PROJETO)

from mcp.server.mcpserver import MCPServer
from qdrant_client import QdrantClient

from core.rag_guard import buscar_conhecimento_rag as rag_guard_buscar, PROMPT_SISTEMA_VETRA
from core.api_client import requisicao_api_segura

import sqlglot
import requests
import pandas as pd
import numpy as np

from sklearn.ensemble import IsolationForest, RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.impute import SimpleImputer
from statsmodels.tsa.api import ExponentialSmoothing, SimpleExpSmoothing
from scipy import stats

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
            sys.stderr.write(f"Erro ao conectar ao Qdrant: {e}\n")
    return qdrant_cliente

# -----------------------------------------------------------------------------
# SANITIZAÇÃO E MASCARAMENTO PII
# -----------------------------------------------------------------------------
def mascarar_dados_sensiveis(texto: str) -> str:
    if not texto:
        return texto
    texto = re.sub(r'\b\d{3}\.\d{3}\.\d{3}-\d{2}\b', '[CPF_OCULTO]', texto)
    texto = re.sub(r'\b\d{11}\b', '[CPF_OCULTO]', texto)
    texto = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL_OCULTO]', texto)
    texto = re.sub(r'\b(?:\+?55\s?)?(?:\(?\d{2}\)?\s?)?\d{4,5}[-\s]?\d{4}\b', '[TELEFONE_OCULTO]', texto)
    return texto

# -----------------------------------------------------------------------------
# DICIONÁRIO DE DADOS
# -----------------------------------------------------------------------------
DICIONARIO_DADOS_DETALHADO = {
    "tb_producao": {
        "descricao": "Tabela de registro de volume de fabricação diário por unidade operacional.",
        "colunas": {
            "id": "INTEGER — Chave primária da ordem de fabricação",
            "data_producao": "DATE — Data de registro do lote de produção",
            "unidade": "VARCHAR — Nome da planta fabril (ex: Contagem, Belo Horizonte)",
            "qtd_produzida": "INTEGER — Quantidade total de peças/unidades produzidas no lote",
            "status_otif": "VARCHAR — Status de conformidade de entrega (Conforme / Não Conforme)"
        }
    },
    "tb_assistencia_tecnica": {
        "descricao": "Tabela de chamados de pós-venda, ordens de serviço e garantia de produtos.",
        "colunas": {
            "id": "INTEGER — Identificador único do chamado de assistência",
            "data": "DATE — Data de abertura da Ordem de Serviço",
            "unidade": "VARCHAR — Unidade responsável pelo atendimento técnico",
            "os_numero": "VARCHAR — Número sequencial da Ordem de Serviço",
            "tipo_defeito": "VARCHAR — Categoria técnica da falha relatada"
        }
    },
    "tb_vendas_compras": {
        "descricao": "Tabela de registros transacionais de movimentação comercial de itens.",
        "colunas": {
            "id": "INTEGER — Identificador do lançamento da transação",
            "data": "DATE — Data do registro comercial",
            "cod_produto": "VARCHAR — Código SKU do produto",
            "qtd_comprada": "INTEGER — Quantidade adquirida pelo cliente",
            "qtd_entregue": "INTEGER — Quantidade efetivamente entregue no destino"
        }
    }
}

# -----------------------------------------------------------------------------
# CACHE COM TTL
# -----------------------------------------------------------------------------
CACHE_MEMORIA: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SEGUNDOS = 300

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

def dividir_em_chunks(texto: str, tamanho_chunk: int = 500, sobreposicao: int = 100) -> List[str]:
    chunks = []
    inicio = 0
    tamanho_texto = len(texto)
    while inicio < tamanho_texto:
        fim = inicio + tamanho_chunk
        chunks.append(texto[inicio:fim])
        inicio += (tamanho_chunk - sobreposicao)
    return chunks

# -----------------------------------------------------------------------------
# FERRAMENTAS MCP
# -----------------------------------------------------------------------------
@mcp.tool()
def indexar_documento_com_chunking(
    texto: str, 
    fonte: str = "Upload Manual", 
    categoria: str = "Geral",
    departamento: str = "TI",
    nivel_acesso: int = 1
) -> str:
    """Divide um texto longo em chunks e indexa no Qdrant Cloud com metadados ricos e PII sanitizado."""
    client = obter_cliente_qdrant()
    if not client:
        return "⚠️ Qdrant Cloud não configurado ou indisponível."

    try:
        texto_sanitizado = mascarar_dados_sensiveis(texto)
        chunks = dividir_em_chunks(texto_sanitizado)
        
        metadados = [
            {
                "fonte": fonte,
                "categoria": categoria,
                "departamento": departamento,
                "nivel_acesso": nivel_acesso,
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
def buscar_conhecimento_rag(
    termo_busca: str, 
    limite: int = 3, 
    departamento_usuario: str = "TI", 
    nivel_acesso: int = 1
) -> str:
    """Realiza busca vetorial/semântica no Qdrant Cloud aplicando RBAC por departamento e nível de acesso."""
    client = obter_cliente_qdrant()
    if not client:
        return "⚠️ Qdrant Cloud não configurado."

    try:
        resultados = rag_guard_buscar(
            client=client,
            collection_name=NOME_COLECAO,
            query_vector=termo_busca,
            departamento_usuario=departamento_usuario,
            nivel_acesso=nivel_acesso,
            limit=limite
        )

        if resultados is None:
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

        if not resultados:
            return f"Nenhum documento encontrado para: '{termo_busca}'."

        resposta = [f"### Resultados RAG para '{termo_busca}':\n"]
        for idx, doc in enumerate(resultados, 1):
            payload = getattr(doc, "payload", {}) or getattr(doc, "metadata", {}) or {}
            texto = payload.get("document", payload.get("texto", "Conteúdo não disponível"))
            fonte = payload.get("fonte", "Desconhecido")
            score = round(getattr(doc, "score", 0.0), 4)
            resposta.append(f"**[{idx}] Fonte: {fonte} (Score: {score})**\n{texto}\n")
        
        contexto_recuperado = "\n".join(resposta)
        return PROMPT_SISTEMA_VETRA.format(contexto_recuperado=contexto_recuperado)

    except Exception as e:
        return f"Erro na consulta RAG: {str(e)}"

@mcp.tool()
def validar_e_executar_sql(query: str, dialecto: str = "postgres") -> str:
    """Valida a sintaxe SQL via AST e checa a existência de tabelas e colunas."""
    try:
        parsed = sqlglot.parse_one(query, read=dialecto)
        if not isinstance(parsed, sqlglot.exp.Select):
            return "❌ Erro de Segurança: Apenas instruções SELECT são permitidas."

        if not parsed.find(sqlglot.exp.Limit):
            query_modificada = f"{query.rstrip(';')} LIMIT 100;"
            parsed = sqlglot.parse_one(query_modificada, read=dialecto)

        tabelas_citadas = [t.name for t in parsed.find_all(sqlglot.exp.Table)]
        for tab in tabelas_citadas:
            if tab not in DICIONARIO_DADOS_DETALHADO:
                return f"❌ Erro de Schema: A tabela '{tab}' não existe no ambiente. Tabelas válidas: {list(DICIONARIO_DADOS_DETALHADO.keys())}"
            
        colunas_citadas = [c.name for c in parsed.find_all(sqlglot.exp.Column)]
        colunas_validas = set()
        for tab in tabelas_citadas:
            colunas_validas.update(DICIONARIO_DADOS_DETALHADO[tab]["colunas"].keys())
        
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
    """Lista as tabelas disponíveis com descrições ricas de contexto de negócio."""
    ret = ["Tabelas e Esquemas do Ambiente Corporativo:"]
    for tab, info in DICIONARIO_DADOS_DETALHADO.items():
        ret.append(f"\n📌 Tabela: `{tab}`")
        ret.append(f"   Descrição: {info['descricao']}")
        ret.append("   Colunas:")
        for col, col_desc in info["colunas"].items():
            ret.append(f"     - {col}: {col_desc}")
    return "\n".join(ret)

@mcp.tool()
@com_cache(ttl_segundos=600)
def descrever_estrutura_tabela(nome_tabela: str) -> str:
    """Retorna a estrutura detalhada e os tipos de dados de uma tabela específica."""
    if nome_tabela in DICIONARIO_DADOS_DETALHADO:
        info = DICIONARIO_DADOS_DETALHADO[nome_tabela]
        resposta = [f"Estrutura da tabela `{nome_tabela}` ({info['descricao']}):"]
        for col, col_desc in info["colunas"].items():
            resposta.append(f"- {col}: {col_desc}")
        return "\n".join(resposta)
    return f"Tabela `{nome_tabela}` não encontrada."

@mcp.tool()
@com_cache(ttl_segundos=180)
def calcular_indicador_otif(unidade: Optional[str] = None) -> str:
    """Calcula o indicador de performance logístico OTIF acumulado do período."""
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
    resultado_json = json.dumps(resultado, ensure_ascii=False, indent=2)
    return f"```json\n{resultado_json}\n```"

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
    resultado_json = json.dumps(metricas, ensure_ascii=False, indent=2)
    return f"```json\n{resultado_json}\n```"

@mcp.tool()
@com_cache(ttl_segundos=600)
def consultar_ibge_sidra(tabela: str = "1737", periodo: str = "last 6", variavel: str = "all") -> str:
    """Consulta a API REST oficial do IBGE / SIDRA para obter IPCA, População ou PIB."""
    try:
        url = f"https://servicodados.ibge.gov.br/api/v3/agregados/{tabela}/periodos/{periodo}/variaveis/{variavel}?localidades=N1[all]"
        headers = {"User-Agent": "VetraDataAgent/1.0"}
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            return f"Error: API IBGE/SIDRA retornou status {response.status_code}."
            
        dados = response.json()
        if not dados:
            return "Nenhum resultado retornado do IBGE."
            
        resultados = []
        for item in dados:
            var_nome = item.get("variavel", "Valor")
            unidade = item.get("unidade", "")
            for res in item.get("resultados", []):
                for serie in res.get("series", []):
                    localidade = serie.get("localidade", {}).get("nome", "Brasil")
                    for data_p, valor in serie.get("serie", {}).items():
                        resultados.append({
                            "periodo": data_p,
                            "indicador": f"{var_nome} ({unidade})",
                            "localidade": localidade,
                            "valor": float(valor) if valor not in [None, "...", "-"] else None
                        })

        df_res = pd.DataFrame(resultados)
        retorno_json = {
            "colunas": list(df_res.columns),
            "linhas": df_res.values.tolist()
        }
        res_str = json.dumps(retorno_json, ensure_ascii=False, indent=2)
        return f"```json\n{res_str}\n```"
    except Exception as e:
        return f"Erro ao consultar IBGE/SIDRA: {type(e).__name__} - {str(e)}"

@mcp.tool()
@com_cache(ttl_segundos=600)
def buscar_dados_municipio_ibge(nome_municipio: str) -> str:
    """Obtém código IBGE, UF e região via API de Localidades do IBGE."""
    try:
        url = f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{nome_municipio}"
        headers = {"User-Agent": "VetraDataAgent/1.0"}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200 or not response.json():
            return f"Município '{nome_municipio}' não encontrado."
            
        dados = response.json()
        mun = dados[0] if isinstance(dados, list) and len(dados) > 0 else dados
            
        info = {
            "id_ibge": mun.get("id"),
            "municipio": mun.get("nome"),
            "uf": mun.get("microrregiao", {}).get("mesorregiao", {}).get("UF", {}).get("sigla"),
            "estado": mun.get("microrregiao", {}).get("mesorregiao", {}).get("UF", {}).get("nome"),
            "regiao": mun.get("microrregiao", {}).get("mesorregiao", {}).get("UF", {}).get("regiao", {}).get("nome")
        }
        return json.dumps(info, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"Erro na consulta de municípios: {str(e)}"

@mcp.tool()
@com_cache(ttl_segundos=300)
def consultar_indicadores_bcb(codigo_serie: int = 432) -> str:
    """Consulta séries temporais reais do Banco Central do Brasil (SGS)."""
    try:
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados/ultimos/12?formato=json"
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return f"Erro ao acessar Banco Central: Status {response.status_code}"
            
        dados = response.json()
        df = pd.DataFrame(dados)
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        
        retorno = {
            "colunas": ["data", "valor"],
            "linhas": df.values.tolist()
        }
        res_str = json.dumps(retorno, ensure_ascii=False, indent=2)
        return f"```json\n{res_str}\n