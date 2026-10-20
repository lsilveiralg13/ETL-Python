import json
import math
import os
import re
import sys
import time
import uuid
from collections import OrderedDict
from functools import wraps
from threading import RLock, local
from typing import Optional, List, Dict, Any

# Mantém o protocolo MCP em UTF-8; os warnings seguem disponíveis no stderr.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# Garante que a raiz do projeto seja encontrada
DIR_ATUAL = os.path.dirname(os.path.abspath(__file__))
RAIZ_PROJETO = os.path.abspath(os.path.join(DIR_ATUAL, "../../"))
if RAIZ_PROJETO not in sys.path:
    sys.path.insert(0, RAIZ_PROJETO)

from mcp.server.mcpserver import MCPServer

import sqlglot
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

mcp = MCPServer("AgenteConsultivoDados")

# -----------------------------------------------------------------------------
# CONFIGURAÇÃO QDRANT CLOUD
# -----------------------------------------------------------------------------
QDRANT_URL = os.environ.get("QDRANT_URL")
QDRANT_API_KEY = os.environ.get("QDRANT_API_KEY")
NOME_COLECAO = "conhecimento_corporativo"
NOME_MODELO_EMBEDDING = "BAAI/bge-small-en-v1.5"
DIMENSAO_EMBEDDING = 384
MAX_DOCUMENTO_CHARS = int(os.environ.get("VETRA_MAX_RAG_DOCUMENT_CHARS", "200000"))
RAG_DEPARTAMENTO_PADRAO = os.environ.get("VETRA_RAG_DEPARTMENT", "TI").strip()
RAG_NIVEL_ACESSO_PADRAO = int(os.environ.get("VETRA_RAG_ACCESS_LEVEL", "1"))
if not RAG_DEPARTAMENTO_PADRAO:
    raise ValueError("VETRA_RAG_DEPARTMENT não pode ser vazio.")
if RAG_NIVEL_ACESSO_PADRAO < 0:
    raise ValueError("VETRA_RAG_ACCESS_LEVEL não pode ser negativo.")
if MAX_DOCUMENTO_CHARS < 1:
    raise ValueError("VETRA_MAX_RAG_DOCUMENT_CHARS deve ser maior que zero.")

qdrant_cliente = None
modelo_embedding = None
_QDRANT_LOCK = RLock()
_HTTP_LOCAL = local()


def obter_sessao_http() -> requests.Session:
    sessao = getattr(_HTTP_LOCAL, "session", None)
    if sessao is None:
        politica_retentativa = Retry(
            total=2,
            connect=2,
            read=1,
            status=2,
            backoff_factor=0.25,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=politica_retentativa)
        sessao = requests.Session()
        sessao.mount("https://", adapter)
        sessao.mount("http://", adapter)
        _HTTP_LOCAL.session = sessao
    return sessao


def obter_cliente_qdrant():
    global qdrant_cliente

    if qdrant_cliente is not None:
        return qdrant_cliente

    if not QDRANT_URL or not QDRANT_API_KEY:
        return None

    with _QDRANT_LOCK:
        if qdrant_cliente is not None:
            return qdrant_cliente

        try:
            from qdrant_client import QdrantClient
            from qdrant_client.http.models import Distance, VectorParams

            cliente = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

            if not cliente.collection_exists(NOME_COLECAO):
                cliente.create_collection(
                    collection_name=NOME_COLECAO,
                    vectors_config=VectorParams(
                        size=DIMENSAO_EMBEDDING,
                        distance=Distance.COSINE,
                    ),
                )

            qdrant_cliente = cliente
        except Exception as e:
            qdrant_cliente = None
            sys.stderr.write(f"Erro ao conectar ao Qdrant: {e}\n")

    return qdrant_cliente


def obter_modelo_embedding():
    global modelo_embedding

    if modelo_embedding is not None:
        return modelo_embedding

    with _QDRANT_LOCK:
        if modelo_embedding is not None:
            return modelo_embedding
        from fastembed import TextEmbedding

        modelo_embedding = TextEmbedding(model_name=NOME_MODELO_EMBEDDING)
    return modelo_embedding


def obter_ids_documento_qdrant(
    client: Any,
    fonte: str,
    categoria: str,
) -> list[Any]:
    from qdrant_client.http import models

    filtro = models.Filter(
        must=[
            models.FieldCondition(
                key="departamento",
                match=models.MatchValue(value=RAG_DEPARTAMENTO_PADRAO),
            ),
            models.FieldCondition(
                key="fonte",
                match=models.MatchValue(value=fonte),
            ),
            models.FieldCondition(
                key="categoria",
                match=models.MatchValue(value=categoria),
            ),
        ]
    )
    ids: list[Any] = []
    offset = None
    while True:
        pontos, offset = client.scroll(
            collection_name=NOME_COLECAO,
            scroll_filter=filtro,
            limit=256,
            with_payload=False,
            with_vectors=False,
            offset=offset,
        )
        ids.extend(ponto.id for ponto in pontos)
        if offset is None:
            return ids


# -----------------------------------------------------------------------------
# SANITIZAÇÃO E MASCARAMENTO PII
# -----------------------------------------------------------------------------
def mascarar_dados_sensiveis(texto: str) -> str:
    if not texto:
        return texto
    texto = re.sub(r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b", "[CPF_OCULTO]", texto)
    texto = re.sub(r"\b\d{11}\b", "[CPF_OCULTO]", texto)
    texto = re.sub(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
        "[EMAIL_OCULTO]",
        texto,
    )
    texto = re.sub(
        r"\b(?:\+?55\s?)?(?:\(?\d{2}\)?\s?)?\d{4,5}[-\s]?\d{4}\b",
        "[TELEFONE_OCULTO]",
        texto,
    )
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
            "status_otif": "VARCHAR — Status de conformidade de entrega (Conforme / Não Conforme)",
        },
    },
    "tb_assistencia_tecnica": {
        "descricao": "Tabela de chamados de pós-venda, ordens de serviço e garantia de produtos.",
        "colunas": {
            "id": "INTEGER — Identificador único do chamado de assistência",
            "data": "DATE — Data de abertura da Ordem de Serviço",
            "unidade": "VARCHAR — Unidade responsável pelo atendimento técnico",
            "os_numero": "VARCHAR — Número sequencial da Ordem de Serviço",
            "tipo_defeito": "VARCHAR — Categoria técnica da falha relatada",
        },
    },
    "tb_vendas_compras": {
        "descricao": "Tabela de registros transacionais de movimentação comercial de itens.",
        "colunas": {
            "id": "INTEGER — Identificador do lançamento da transação",
            "data": "DATE — Data do registro comercial",
            "cod_produto": "VARCHAR — Código SKU do produto",
            "qtd_comprada": "INTEGER — Quantidade adquirida pelo cliente",
            "qtd_entregue": "INTEGER — Quantidade efetivamente entregue no destino",
        },
    },
}


# -----------------------------------------------------------------------------
# CACHE COM TTL
# -----------------------------------------------------------------------------
CACHE_MEMORIA: OrderedDict[str, Dict[str, Any]] = OrderedDict()
CACHE_TTL_SEGUNDOS = 300
CACHE_MAX_ENTRADAS = 256
_CACHE_LOCK = RLock()


def com_cache(ttl_segundos: int = CACHE_TTL_SEGUNDOS):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            chave_cache = (
                f"{func.__name__}:"
                f"{json.dumps(args, ensure_ascii=False, default=str)}:"
                f"{json.dumps(kwargs, ensure_ascii=False, sort_keys=True, default=str)}"
            )
            agora = time.monotonic()

            with _CACHE_LOCK:
                registro = CACHE_MEMORIA.get(chave_cache)
                if registro is not None:
                    if agora - registro["timestamp"] < registro.get(
                        "ttl", CACHE_TTL_SEGUNDOS
                    ):
                        CACHE_MEMORIA.move_to_end(chave_cache)
                        dados = registro["dados"]
                        if isinstance(dados, str):
                            return dados + "\n\n⚡ *(Resposta obtida do cache)*"
                        return dados
                    CACHE_MEMORIA.pop(chave_cache, None)

            resultado = func(*args, **kwargs)
            with _CACHE_LOCK:
                CACHE_MEMORIA[chave_cache] = {
                    "timestamp": time.monotonic(),
                    "ttl": ttl_segundos,
                    "dados": resultado,
                }
                CACHE_MEMORIA.move_to_end(chave_cache)
                while len(CACHE_MEMORIA) > CACHE_MAX_ENTRADAS:
                    CACHE_MEMORIA.popitem(last=False)
            return resultado

        return wrapper

    return decorator


def invalidar_cache_funcao(nome_funcao: str) -> None:
    prefixo = f"{nome_funcao}:"
    with _CACHE_LOCK:
        for chave in list(CACHE_MEMORIA):
            if chave.startswith(prefixo):
                CACHE_MEMORIA.pop(chave, None)


def dividir_em_chunks(
    texto: str,
    tamanho_chunk: int = 500,
    sobreposicao: int = 100,
) -> List[str]:
    if tamanho_chunk <= 0:
        raise ValueError("tamanho_chunk deve ser maior que zero.")
    if sobreposicao < 0 or sobreposicao >= tamanho_chunk:
        raise ValueError(
            "sobreposicao deve ser maior ou igual a zero e menor que tamanho_chunk."
        )
    if not texto:
        return []

    chunks = []
    inicio = 0
    tamanho_texto = len(texto)
    passo = tamanho_chunk - sobreposicao

    while inicio < tamanho_texto:
        fim = inicio + tamanho_chunk
        chunks.append(texto[inicio:fim])
        inicio += passo

    return chunks


# -----------------------------------------------------------------------------
# FERRAMENTAS MCP
# -----------------------------------------------------------------------------
@mcp.tool()
def indexar_documento_com_chunking(
    texto: str,
    fonte: str = "Upload Manual",
    categoria: str = "Geral",
) -> str:
    """Divide um texto longo em chunks e indexa no Qdrant Cloud com metadados ricos e PII sanitizado."""
    client = obter_cliente_qdrant()
    if not client:
        return "⚠️ Qdrant Cloud não configurado ou indisponível."

    try:
        if len(texto) > MAX_DOCUMENTO_CHARS:
            return (
                "Erro: O documento excede o limite de "
                f"{MAX_DOCUMENTO_CHARS:,} caracteres."
            )
        if len(fonte) > 255 or len(categoria) > 128:
            return "Erro: A fonte ou categoria excede o tamanho permitido."
        texto_sanitizado = mascarar_dados_sensiveis(texto)
        chunks = dividir_em_chunks(texto_sanitizado)

        if not chunks:
            return "Erro: O documento está vazio."

        metadados = [
            {
                "fonte": fonte,
                "categoria": categoria,
                "departamento": RAG_DEPARTAMENTO_PADRAO,
                "nivel_acesso": RAG_NIVEL_ACESSO_PADRAO,
                "chunk_index": i,
                "total_chunks": len(chunks),
                "document": chunk,
            }
            for i, chunk in enumerate(chunks)
        ]
        ids = [
            str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"vetra-rag:{RAG_DEPARTAMENTO_PADRAO}:{fonte}:{categoria}:{i}",
                )
            )
            for i in range(len(chunks))
        ]

        from qdrant_client.http import models

        modelo = obter_modelo_embedding()
        vetores = [vetor.tolist() for vetor in modelo.embed(chunks)]
        if len(vetores) != len(chunks):
            raise RuntimeError("O modelo de embedding retornou quantidade inesperada de vetores.")
        ids_existentes = obter_ids_documento_qdrant(client, fonte, categoria)
        pontos = [
            models.PointStruct(
                id=identificador,
                vector=vetor,
                payload=metadado,
            )
            for identificador, vetor, metadado in zip(ids, vetores, metadados)
        ]
        client.upsert(
            collection_name=NOME_COLECAO,
            points=pontos,
            wait=True,
        )
        ids_novos = set(ids)
        ids_obsoletos = [
            identificador
            for identificador in ids_existentes
            if str(identificador) not in ids_novos
        ]
        if ids_obsoletos:
            client.delete(
                collection_name=NOME_COLECAO,
                points_selector=ids_obsoletos,
                wait=True,
            )
        invalidar_cache_funcao("buscar_conhecimento_rag")
        return (
            f"✅ Documento '{fonte}' sanitizado e indexado com sucesso "
            "com embeddings locais e acesso controlado! "
            f"({len(chunks)} chunks criados no Qdrant)."
        )
    except Exception as e:
        return f"Erro ao indexar no Qdrant: {str(e)}"


@mcp.tool()
@com_cache(ttl_segundos=300)
def buscar_conhecimento_rag(
    termo_busca: str,
    limite: int = 3,
) -> str:
    """Realiza busca vetorial/semântica no Qdrant Cloud aplicando RBAC por departamento e nível de acesso."""
    client = obter_cliente_qdrant()
    if not client:
        return "⚠️ Qdrant Cloud não configurado."

    if not termo_busca.strip():
        return "Erro: O termo de busca não pode ser vazio."
    if limite < 1:
        return "Erro: O limite deve ser maior que zero."

    try:
        from core.rag_guard import (
            buscar_conhecimento_rag as rag_guard_buscar,
            PROMPT_SISTEMA_VETRA,
        )

        vetor_consulta = next(
            iter(obter_modelo_embedding().query_embed(termo_busca))
        ).tolist()
        resultados = rag_guard_buscar(
            client=client,
            collection_name=NOME_COLECAO,
            query_vector=vetor_consulta,
            departamento_usuario=RAG_DEPARTAMENTO_PADRAO,
            nivel_acesso=RAG_NIVEL_ACESSO_PADRAO,
            limit=limite,
        )

        # Não executa busca direta sem filtros. Isso preserva o RBAC aplicado
        # por rag_guard_buscar e evita que uma string seja interpretada como ID.
        if resultados is None:
            return (
                "Não foi possível realizar a consulta RAG com os controles "
                "de acesso configurados."
            )

        if not resultados:
            return f"Nenhum documento encontrado para: '{termo_busca}'."

        resposta = [f"### Resultados RAG para '{termo_busca}':\n"]
        for idx, doc in enumerate(resultados, 1):
            payload = (
                getattr(doc, "payload", {})
                or getattr(doc, "metadata", {})
                or {}
            )
            texto = (
                payload.get("document")
                or payload.get("texto")
                or payload.get("text")
                or "Conteúdo não disponível"
            )
            fonte = payload.get("fonte", "Desconhecido")
            score = round(float(getattr(doc, "score", 0.0) or 0.0), 4)
            resposta.append(
                f"**[{idx}] Fonte: {fonte} (Score: {score})**\n{texto}\n"
            )

        contexto_recuperado = "\n".join(resposta)
        return PROMPT_SISTEMA_VETRA.format(
            contexto_recuperado=contexto_recuperado
        )

    except Exception as e:
        return f"Erro na consulta RAG: {str(e)}"


@mcp.tool()
def validar_e_executar_sql(query: str, dialecto: str = "postgres") -> str:
    """Valida SELECT via AST para PostgreSQL, Databricks, MySQL, BigQuery ou Oracle; não executa SQL."""
    try:
        def normalizar_identificador(identificador: str) -> str:
            return (
                identificador.casefold()
                if dialecto == "oracle"
                else identificador
            )

        dialectos_validos = {
            "postgres": "postgres",
            "postgresql": "postgres",
            "databricks": "databricks",
            "mysql": "mysql",
            "bigquery": "bigquery",
            "oracle": "oracle",
        }
        dialecto = dialectos_validos.get(dialecto.lower().strip())
        if dialecto is None:
            return "❌ Erro: O dialeto SQL informado não é suportado."
        statements = sqlglot.parse(query, read=dialecto)
        if len(statements) != 1:
            return "❌ Erro de Segurança: Apenas uma instrução SQL é permitida."

        parsed = statements[0]
        if not isinstance(parsed, sqlglot.exp.Select):
            return "❌ Erro de Segurança: Apenas instruções SELECT são permitidas."
        if parsed.args.get("into") or any(
            isinstance(node, (sqlglot.exp.DML, sqlglot.exp.Command))
            for node in parsed.walk()
        ):
            return (
                "❌ Erro de Segurança: A consulta contém uma operação de "
                "escrita ou comando não permitido."
            )

        if not parsed.find(sqlglot.exp.Limit):
            parsed = parsed.limit(100)

        tabelas_citadas = []
        aliases_tabelas = {}
        for tabela in parsed.find_all(sqlglot.exp.Table):
            nome_tabela = tabela.name
            nome_normalizado = normalizar_identificador(nome_tabela)
            if tabela.catalog or (tabela.db and dialecto != "oracle"):
                return (
                    "❌ Erro de Schema: Catálogos e schemas explícitos não "
                    "são permitidos nesta validação, exceto owner/schema no Oracle."
                )

            if nome_normalizado == "dual" and dialecto == "oracle":
                tabela_real = "dual"
                colunas_tabela = {"dummy"}
            else:
                tabela_real = next(
                    (
                        nome
                        for nome in DICIONARIO_DADOS_DETALHADO
                        if normalizar_identificador(nome) == nome_normalizado
                    ),
                    None,
                )
                if tabela_real is None:
                    return (
                        f"❌ Erro de Schema: A tabela '{nome_tabela}' não "
                        f"existe no ambiente. Tabelas válidas: "
                        f"{list(DICIONARIO_DADOS_DETALHADO.keys())}"
                    )
                colunas_tabela = {
                    normalizar_identificador(coluna)
                    for coluna in DICIONARIO_DADOS_DETALHADO[tabela_real]["colunas"]
                }

            tabelas_citadas.append(tabela_real)
            if tabela.alias:
                aliases_tabelas[normalizar_identificador(tabela.alias)] = (
                    tabela_real,
                    colunas_tabela,
                )
            aliases_tabelas[nome_normalizado] = (tabela_real, colunas_tabela)

        if not tabelas_citadas:
            return "❌ Erro de Schema: Nenhuma tabela foi informada."

        tabelas_unicas = list(dict.fromkeys(tabelas_citadas))

        for coluna in parsed.find_all(sqlglot.exp.Column):
            nome_coluna = normalizar_identificador(coluna.name)
            qualificador = normalizar_identificador(coluna.table)

            if nome_coluna == "*":
                continue
            if dialecto == "oracle" and nome_coluna in {
                "level",
                "ora_rowscn",
                "rowid",
                "rownum",
            }:
                continue

            if qualificador:
                contexto_tabela = aliases_tabelas.get(qualificador)
                if contexto_tabela is None:
                    return (
                        f"❌ Erro de Schema: A tabela ou alias "
                        f"'{qualificador}' não existe na consulta."
                    )
                tabela_real, colunas_tabela = contexto_tabela
                if nome_coluna not in colunas_tabela:
                    return (
                        f"❌ Erro de Schema: A coluna '{nome_coluna}' não "
                        f"pertence à tabela '{tabela_real}'."
                    )
            else:
                tabelas_compativeis = [
                    tabela
                    for tabela in tabelas_unicas
                    if nome_coluna in (
                        {"dummy"}
                        if tabela == "dual"
                        else {
                            normalizar_identificador(coluna)
                            for coluna in DICIONARIO_DADOS_DETALHADO[tabela]["colunas"]
                        }
                    )
                ]

                if not tabelas_compativeis:
                    return (
                        f"❌ Erro de Schema: A coluna '{nome_coluna}' não "
                        "pertence às tabelas selecionadas."
                    )

                if len(tabelas_compativeis) > 1:
                    return (
                        f"❌ Erro de Schema: A coluna '{nome_coluna}' é "
                        "ambígua. Informe a tabela ou alias."
                    )

        query_validada = parsed.sql(dialect=dialecto)

    except Exception as e:
        return f"❌ Erro de Sintaxe SQL: {str(e)}"

    dados_simulados = {
        "aviso": "Execução simulada; nenhuma conexão de banco foi configurada.",
        "query_validada": query_validada,
        "colunas": ["mes", "qtd_produzida", "otif_pct"],
        "linhas": [
            ["Jan", 1200, 94.5],
            ["Fev", 1350, 96.0],
            ["Mar", 1100, 91.2],
            ["Abr", 1500, 97.8],
        ],
    }
    return (
        "✅ Query validada com sucesso!\n```json\n"
        f"{json.dumps(dados_simulados, ensure_ascii=False)}\n```"
    )


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
        resposta = [
            f"Estrutura da tabela `{nome_tabela}` ({info['descricao']}):"
        ]
        for col, col_desc in info["colunas"].items():
            resposta.append(f"- {col}: {col_desc}")
        return "\n".join(resposta)
    return f"Tabela `{nome_tabela}` não encontrada."


@mcp.tool()
@com_cache(ttl_segundos=180)
def calcular_indicador_otif(unidade: Optional[str] = None) -> str:
    """Calcula o indicador de performance logístico OTIF acumulado do período."""
    dados_otif = {
        "Contagem": {
            "total_pedidos": 450,
            "no_prazo": 420,
            "completos": 410,
            "otif_sucesso": 398,
        },
        "Belo Horizonte": {
            "total_pedidos": 600,
            "no_prazo": 570,
            "completos": 550,
            "otif_sucesso": 530,
        },
        "Geral": {
            "total_pedidos": 1050,
            "no_prazo": 990,
            "completos": 960,
            "otif_sucesso": 928,
        },
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
            "pedidos_otif_perfeito": d["otif_sucesso"],
        },
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
        "gargalo_identificado": "Etapa de Pintura / Carga Térmica",
    }
    resultado_json = json.dumps(metricas, ensure_ascii=False, indent=2)
    return f"```json\n{resultado_json}\n```"


@mcp.tool()
@com_cache(ttl_segundos=600)
def consultar_ibge_sidra(
    tabela: str = "1737",
    periodo: str = "last 6",
    variavel: str = "all",
) -> str:
    """Consulta a API REST oficial do IBGE / SIDRA para obter IPCA, População ou PIB."""
    try:
        url = (
            "https://servicodados.ibge.gov.br/api/v3/agregados/"
            f"{tabela}/periodos/{periodo}/variaveis/{variavel}"
            "?localidades=N1[all]"
        )
        headers = {"User-Agent": "VetraDataAgent/1.0"}
        response = obter_sessao_http().get(url, headers=headers, timeout=15)

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
                    localidade = serie.get("localidade", {}).get(
                        "nome", "Brasil"
                    )
                    for data_p, valor in serie.get("serie", {}).items():
                        resultados.append(
                            {
                                "periodo": data_p,
                                "indicador": f"{var_nome} ({unidade})",
                                "localidade": localidade,
                                "valor": (
                                    float(valor)
                                    if valor not in [None, "...", "-"]
                                    else None
                                ),
                            }
                        )

        import pandas as pd

        df_res = pd.DataFrame(resultados)
        retorno_json = {
            "colunas": list(df_res.columns),
            "linhas": df_res.values.tolist(),
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
        url = (
            "https://servicodados.ibge.gov.br/api/v1/localidades/municipios/"
            f"{nome_municipio}"
        )
        headers = {"User-Agent": "VetraDataAgent/1.0"}
        response = obter_sessao_http().get(url, headers=headers, timeout=10)

        if response.status_code != 200 or not response.json():
            return f"Município '{nome_municipio}' não encontrado."

        dados = response.json()
        mun = dados[0] if isinstance(dados, list) and len(dados) > 0 else dados

        info = {
            "id_ibge": mun.get("id"),
            "municipio": mun.get("nome"),
            "uf": mun.get("microrregiao", {})
            .get("mesorregiao", {})
            .get("UF", {})
            .get("sigla"),
            "estado": mun.get("microrregiao", {})
            .get("mesorregiao", {})
            .get("UF", {})
            .get("nome"),
            "regiao": mun.get("microrregiao", {})
            .get("mesorregiao", {})
            .get("UF", {})
            .get("regiao", {})
            .get("nome"),
        }
        return json.dumps(info, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"Erro na consulta de municípios: {str(e)}"


@mcp.tool()
@com_cache(ttl_segundos=300)
def consultar_indicadores_bcb(codigo_serie: int = 432) -> str:
    """Consulta séries temporais reais do Banco Central do Brasil (SGS)."""
    try:
        url = (
            "https://api.bcb.gov.br/dados/serie/"
            f"bcdata.sgs.{codigo_serie}/dados/ultimos/12?formato=json"
        )
        response = obter_sessao_http().get(url, timeout=10)
        if response.status_code != 200:
            return f"Erro ao acessar Banco Central: Status {response.status_code}"

        dados = response.json()
        import pandas as pd

        df = pd.DataFrame(dados)
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

        retorno = {
            "colunas": ["data", "valor"],
            "linhas": df.values.tolist(),
        }
        res_str = json.dumps(retorno, ensure_ascii=False, indent=2)
        return f"```json\n{res_str}\n```"
    except Exception as e:
        return f"Erro no Banco Central: {str(e)}"


@mcp.tool()
@com_cache(ttl_segundos=600)
def consultar_cnpj_brasilapi(cnpj: str) -> str:
    """Consulta dados cadastrais em tempo real de empresas na Receita Federal via BrasilAPI."""
    try:
        cnpj_limpo = re.sub(r"\D", "", cnpj)
        if len(cnpj_limpo) != 14:
            return "CNPJ inválido: informe 14 dígitos."

        url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}"
        res = obter_sessao_http().get(url, timeout=10)
        if res.status_code != 200:
            return f"CNPJ {cnpj} não encontrado."

        d = res.json()
        info = {
            "razao_social": d.get("razao_social"),
            "nome_fantasia": d.get("nome_fantasia"),
            "cnpj": d.get("cnpj"),
            "situacao_cadastral": d.get("descricao_situacao_cadastral"),
            "cnae_fiscal_descricao": d.get("cnae_fiscal_descricao"),
            "uf": d.get("uf"),
            "municipio": d.get("municipio"),
            "capital_social": d.get("capital_social"),
        }
        return json.dumps(info, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"Erro na consulta de CNPJ: {str(e)}"


@mcp.tool()
@com_cache(ttl_segundos=600)
def consultar_cep_brasilapi(cep: str) -> str:
    """Consulta endereço e geolocalização por CEP via BrasilAPI."""
    try:
        cep_limpo = re.sub(r"\D", "", cep)
        if len(cep_limpo) != 8:
            return "CEP inválido: informe 8 dígitos."

        url = f"https://brasilapi.com.br/api/cep/v2/{cep_limpo}"
        res = obter_sessao_http().get(url, timeout=10)
        if res.status_code != 200:
            return f"CEP {cep} não encontrado."

        return json.dumps(res.json(), ensure_ascii=False, indent=2)
    except Exception as e:
        return f"Erro na consulta de CEP: {str(e)}"


@mcp.tool()
@com_cache(ttl_segundos=180)
def consultar_cotacao_moeda(
    par_moedas: str = "USD-BRL,EUR-BRL",
) -> str:
    """Consulta cotações e variações percentuais em tempo real via AwesomeAPI."""
    try:
        url = f"https://economia.awesomeapi.com.br/last/{par_moedas}"
        res = obter_sessao_http().get(url, timeout=5)
        if res.status_code != 200:
            return f"Erro em cotações: Status {res.status_code}"

        dados = res.json()
        linhas = []
        for _, info in dados.items():
            linhas.append(
                [
                    info.get("name"),
                    float(info.get("bid", 0)),
                    float(info.get("ask", 0)),
                    f"{info.get('pctChange')}%",
                    info.get("create_date"),
                ]
            )

        retorno = {
            "colunas": [
                "moeda",
                "valor_compra",
                "valor_venda",
                "variacao_pct",
                "ultima_atualizacao",
            ],
            "linhas": linhas,
        }
        res_str = json.dumps(retorno, ensure_ascii=False, indent=2)
        return f"```json\n{res_str}\n```"
    except Exception as e:
        return f"Erro em cotações: {str(e)}"


@mcp.tool()
@com_cache(ttl_segundos=300)
def geocodificar_endereco(localidade: str) -> str:
    """Obtém coordenadas geográficas (Lat/Lon) via OpenStreetMap."""
    try:
        headers = {"User-Agent": "VetraDataAgent/1.0"}
        res = obter_sessao_http().get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": localidade, "format": "json", "limit": 1},
            headers=headers,
            timeout=10,
        )

        if res.status_code != 200 or not res.json():
            return f"Localidade '{localidade}' não encontrada."

        item = res.json()[0]
        return json.dumps(
            {
                "nome": item.get("display_name"),
                "latitude": float(item.get("lat")),
                "longitude": float(item.get("lon")),
            },
            ensure_ascii=False,
            indent=2,
        )
    except Exception as e:
        return f"Erro na geocodificação: {str(e)}"


@mcp.tool()
@com_cache(ttl_segundos=600)
def consultar_clima_open_meteo(
    latitude: float = -19.9167,
    longitude: float = -43.9345,
) -> str:
    """Consulta condições meteorológicas atuais via Open-Meteo API."""
    try:
        if not -90 <= latitude <= 90:
            return "Latitude inválida: informe um valor entre -90 e 90."
        if not -180 <= longitude <= 180:
            return "Longitude inválida: informe um valor entre -180 e 180."

        url = (
            "https://api.open-meteo.com/v1/forecast?"
            f"latitude={latitude}&longitude={longitude}&current_weather=true"
        )
        res = obter_sessao_http().get(url, timeout=10)
        if res.status_code != 200:
            return f"Erro ao acessar Open-Meteo: Status {res.status_code}"

        d = res.json().get("current_weather", {})
        info = {
            "temperatura_celsius": d.get("temperature"),
            "velocidade_vento_kmh": d.get("windspeed"),
            "direcao_vento": d.get("winddirection"),
            "horario": d.get("time"),
        }
        return json.dumps(info, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"Erro ao consultar clima: {str(e)}"


@mcp.tool()
@com_cache(ttl_segundos=300)
def consultar_futebol_liga(codigo_liga: str = "BSA") -> str:
    """Consulta a classificação em tempo real do Campeonato Brasileiro ou ligas internacionais."""
    try:
        url = (
            "https://api.football-data.org/v4/competitions/"
            f"{codigo_liga}/standings"
        )
        api_token = os.environ.get("FOOTBALL_DATA_API_KEY", "")
        headers = {"X-Auth-Token": api_token} if api_token else {}

        res = obter_sessao_http().get(url, headers=headers, timeout=10)
        if res.status_code != 200:
            return f"⚠️ API Futebol retornou status {res.status_code}."

        dados = res.json()
        standings = dados.get("standings", [])
        if not standings:
            return "Nenhum dado de tabela disponível para esta competição no momento."

        tabela = standings[0].get("table", [])
        linhas = []
        for item in tabela:
            linhas.append(
                [
                    item.get("position"),
                    item.get("team", {}).get("name"),
                    item.get("points"),
                    item.get("playedGames"),
                    item.get("won"),
                ]
            )

        retorno = {
            "liga": dados.get("competition", {}).get(
                "name", "Campeonato Brasileiro"
            ),
            "colunas": ["posicao", "clube", "pontos", "jogos", "vitorias"],
            "linhas": linhas,
        }
        res_str = json.dumps(retorno, ensure_ascii=False, indent=2)
        return f"```json\n{res_str}\n```"
    except Exception as e:
        return f"Erro ao consultar API de futebol: {str(e)}"


# -----------------------------------------------------------------------------
# MOTORES DE MACHINE LEARNING E ESTATÍSTICA
# -----------------------------------------------------------------------------
@mcp.tool()
def gerar_previsao_generica(
    dados_json: str,
    coluna_data: str,
    coluna_alvo: str,
    periodos_frente: int = 30,
) -> str:
    """Projeta uma variável numérica ao longo do tempo usando Suavização Exponencial."""
    try:
        import pandas as pd
        from statsmodels.tsa.api import ExponentialSmoothing, SimpleExpSmoothing

        if periodos_frente < 1:
            return "Erro: periodos_frente deve ser maior que zero."

        dados = json.loads(dados_json)
        df = (
            pd.DataFrame(dados["linhas"], columns=dados["colunas"])
            if isinstance(dados, dict) and "linhas" in dados
            else pd.DataFrame(dados)
        )

        if df.empty or coluna_data not in df.columns or coluna_alvo not in df.columns:
            return (
                f"Erro: Os dados devem conter as colunas '{coluna_data}' "
                f"e '{coluna_alvo}'."
            )

        df[coluna_data] = pd.to_datetime(df[coluna_data], errors="coerce")
        df[coluna_alvo] = pd.to_numeric(df[coluna_alvo], errors="coerce")
        df = df.dropna(subset=[coluna_data, coluna_alvo]).sort_values(
            by=coluna_data
        )

        if len(df) < 5:
            return (
                "Erro: O histórico é muito curto para gerar um modelo "
                "preditivo (mínimo de 5 pontos)."
            )

        df_ts = (
            df.groupby(coluna_data)[coluna_alvo]
            .sum()
            .asfreq("D")
            .ffill()
            .bfill()
        )

        try:
            modelo = ExponentialSmoothing(
                df_ts, trend="add", seasonal=None
            ).fit()
        except Exception:
            modelo = SimpleExpSmoothing(df_ts).fit()

        previsao = modelo.forecast(periodos_frente)
        datas_futuras = pd.date_range(
            start=df_ts.index[-1] + pd.Timedelta(days=1),
            periods=periodos_frente,
            freq="D",
        )

        resultado_pred = [
            {
                "data": d.strftime("%Y-%m-%d"),
                "valor_previsto": round(float(v), 2),
            }
            for d, v in zip(datas_futuras, previsao)
        ]
        historico_recente = [
            {
                "data": d.strftime("%Y-%m-%d"),
                "valor_historico": round(float(v), 2),
            }
            for d, v in zip(df_ts.index[-10:], df_ts.values[-10:])
        ]

        return json.dumps(
            {
                "status": "sucesso",
                "modelo_utilizado": modelo.__class__.__name__,
                "periodos_projetados": periodos_frente,
                "historico_recente": historico_recente,
                "previsoes": resultado_pred,
            },
            ensure_ascii=False,
        )

    except Exception as e:
        return f"Erro ao gerar previsão de Machine Learning: {str(e)}"


@mcp.tool()
def detectar_anomalias_generico(
    dados_json: str,
    colunas_analise: List[str],
    sensibilidade: float = 0.05,
) -> str:
    """Identifica padrões atípicos e outliers utilizando Isolation Forest."""
    try:
        import pandas as pd
        from sklearn.ensemble import IsolationForest
        from sklearn.impute import SimpleImputer
        from sklearn.preprocessing import StandardScaler

        dados = json.loads(dados_json)
        df = (
            pd.DataFrame(dados["linhas"], columns=dados["colunas"])
            if isinstance(dados, dict) and "linhas" in dados
            else pd.DataFrame(dados)
        )

        if df.empty:
            return "Erro: O conjunto de dados fornecido está vazio."

        cols_validas = [c for c in colunas_analise if c in df.columns]
        if not cols_validas:
            return (
                f"Erro: Nenhuma das colunas {colunas_analise} foi "
                "encontrada no dataset."
            )

        X = df[cols_validas].copy()
        for col in cols_validas:
            X[col] = pd.to_numeric(X[col], errors="coerce")

        X = X.dropna(axis=1, how="all")
        if X.shape[1] == 0:
            return "Erro: Nenhuma coluna contém valores numéricos válidos."

        X_imputed = SimpleImputer(strategy="median").fit_transform(X)
        X_scaled = StandardScaler().fit_transform(X_imputed)

        model = IsolationForest(
            contamination=max(0.01, min(0.2, sensibilidade)),
            random_state=42,
        )
        df["anomalia_score"] = model.fit_predict(X_scaled)

        anomalias = df[df["anomalia_score"] == -1].copy()
        amostra = (
            anomalias.head(15)
            .drop(columns=["anomalia_score"])
            .to_dict(orient="records")
        )

        return json.dumps(
            {
                "status": "sucesso",
                "algoritmo": "Isolation Forest",
                "total_linhas_analisadas": len(df),
                "total_anomalias_encontradas": len(anomalias),
                "percentual_anomalias": round(
                    (len(anomalias) / len(df)) * 100, 2
                ),
                "colunas_avaliadas": list(X.columns),
                "amostra_anomalias_detectadas": amostra,
            },
            ensure_ascii=False,
        )

    except Exception as e:
        return f"Erro na detecção de anomalias: {str(e)}"


@mcp.tool()
def agrupar_dados_generico(
    dados_json: str,
    colunas_caracteristicas: List[str],
    num_clusters: int = 0,
) -> str:
    """Descobre agrupamentos e perfis nos dados utilizando K-Means."""
    try:
        import pandas as pd
        from sklearn.cluster import KMeans
        from sklearn.impute import SimpleImputer
        from sklearn.preprocessing import StandardScaler

        dados = json.loads(dados_json)
        df = (
            pd.DataFrame(dados["linhas"], columns=dados["colunas"])
            if isinstance(dados, dict) and "linhas" in dados
            else pd.DataFrame(dados)
        )

        if df.empty:
            return "Erro: O conjunto de dados fornecido está vazio."

        cols_validas = [c for c in colunas_caracteristicas if c in df.columns]
        if not cols_validas:
            return (
                f"Erro: Nenhuma coluna válida encontrada entre "
                f"{colunas_caracteristicas}."
            )

        X = df[cols_validas].copy()
        for col in cols_validas:
            X[col] = pd.to_numeric(X[col], errors="coerce")

        X = X.dropna(axis=1, how="all")
        if X.shape[1] == 0:
            return "Erro: Nenhuma coluna contém valores numéricos válidos."

        if len(df) < 2:
            return "Erro: São necessárias pelo menos 2 linhas para agrupamento."

        if num_clusters >= 2:
            if num_clusters > len(df):
                return (
                    f"Erro: num_clusters ({num_clusters}) não pode ser maior "
                    f"que o número de linhas ({len(df)})."
                )
            k = num_clusters
        else:
            k = min(len(df), min(4, max(2, len(df) // 10)))

        X_imputed = SimpleImputer(strategy="mean").fit_transform(X)
        X_scaled = StandardScaler().fit_transform(X_imputed)

        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        clusters = kmeans.fit_predict(X_scaled)
        df["cluster"] = clusters

        # Usa os valores numéricos imputados para calcular as médias.
        df_numerico = pd.DataFrame(
            X_imputed,
            columns=X.columns,
            index=df.index,
        )
        df_numerico["cluster"] = clusters

        resumo_clusters = []
        for cluster_id in range(k):
            sub_df = df_numerico[df_numerico["cluster"] == cluster_id]
            medias = sub_df.drop(columns=["cluster"]).mean().to_dict()
            resumo_clusters.append(
                {
                    "cluster_id": int(cluster_id),
                    "quantidade_elementos": len(sub_df),
                    "percentual_do_total": round(
                        (len(sub_df) / len(df)) * 100, 2
                    ),
                    "medias_das_caracteristicas": {
                        col: round(float(val), 2)
                        for col, val in medias.items()
                    },
                }
            )

        return json.dumps(
            {
                "status": "sucesso",
                "algoritmo": "K-Means Clustering",
                "total_clusters": k,
                "colunas_utilizadas": list(X.columns),
                "resumo_perfis_clusters": resumo_clusters,
            },
            ensure_ascii=False,
        )

    except Exception as e:
        return f"Erro no agrupamento de dados (Clustering): {str(e)}"


@mcp.tool()
def analisar_correlacao_e_importancia(
    dados_json: str,
    coluna_alvo: str,
    colunas_explicativas: List[str],
) -> str:
    """Avalia a importância relativa de cada variável para explicar uma métrica-alvo."""
    try:
        import pandas as pd
        from sklearn.ensemble import RandomForestRegressor

        dados = json.loads(dados_json)
        df = (
            pd.DataFrame(dados["linhas"], columns=dados["colunas"])
            if isinstance(dados, dict) and "linhas" in dados
            else pd.DataFrame(dados)
        )

        if df.empty or coluna_alvo not in df.columns:
            return (
                f"Erro: A coluna alvo '{coluna_alvo}' não foi encontrada "
                "nos dados."
            )

        cols_exp = [
            c
            for c in colunas_explicativas
            if c in df.columns and c != coluna_alvo
        ]
        if not cols_exp:
            return "Erro: Nenhuma coluna explicativa válida foi fornecida."

        df_clean = (
            df[[coluna_alvo] + cols_exp]
            .apply(pd.to_numeric, errors="coerce")
            .replace([float("inf"), float("-inf")], pd.NA)
            .dropna()
        )

        if len(df_clean) < 10:
            return (
                "Erro: Dados insuficientes após limpeza para calcular "
                "importância estatística."
            )

        X = df_clean[cols_exp]
        y = df_clean[coluna_alvo]

        rf = RandomForestRegressor(n_estimators=50, random_state=42)
        rf.fit(X, y)

        importancias = sorted(
            [
                {
                    "variavel": col,
                    "importancia_percentual": round(float(imp) * 100, 2),
                }
                for col, imp in zip(cols_exp, rf.feature_importances_)
            ],
            key=lambda x: x["importancia_percentual"],
            reverse=True,
        )

        correlacoes = {
            col: round(float(val), 3)
            for col, val in df_clean.corr()[coluna_alvo]
            .drop(coluna_alvo)
            .to_dict()
            .items()
        }

        return json.dumps(
            {
                "status": "sucesso",
                "variavel_alvo": coluna_alvo,
                "ranking_importancia_variaveis": importancias,
                "correlacao_linear_pearson": correlacoes,
            },
            ensure_ascii=False,
        )

    except Exception as e:
        return f"Erro na análise de importância/correlação: {str(e)}"


@mcp.tool()
def testar_hipotese_estatistica(
    dados_json: str,
    coluna_grupo: str,
    coluna_metrica: str,
) -> str:
    """Realiza teste T de Student em duas amostras para avaliar diferença entre dois grupos."""
    try:
        import pandas as pd
        from scipy import stats

        dados = json.loads(dados_json)
        df = (
            pd.DataFrame(dados["linhas"], columns=dados["colunas"])
            if isinstance(dados, dict) and "linhas" in dados
            else pd.DataFrame(dados)
        )

        if (
            df.empty
            or coluna_grupo not in df.columns
            or coluna_metrica not in df.columns
        ):
            return (
                f"Erro: Colunas '{coluna_grupo}' ou '{coluna_metrica}' "
                "não encontradas nos dados."
            )

        df[coluna_metrica] = pd.to_numeric(
            df[coluna_metrica], errors="coerce"
        )
        grupos = df[coluna_grupo].dropna().unique()

        if len(grupos) != 2:
            return (
                "Erro: O Teste T exige exatamente 2 grupos distintos. "
                f"Grupos encontrados: {list(grupos)}"
            )

        grupo_a = df[df[coluna_grupo] == grupos[0]][coluna_metrica].dropna()
        grupo_b = df[df[coluna_grupo] == grupos[1]][coluna_metrica].dropna()

        if len(grupo_a) < 2 or len(grupo_b) < 2:
            return (
                "Erro: Cada grupo precisa ter pelo menos 2 observações "
                "numéricas válidas."
            )

        stat, p_valor = stats.ttest_ind(grupo_a, grupo_b, equal_var=False)
        if not math.isfinite(float(stat)) or not math.isfinite(float(p_valor)):
            return (
                "Erro: Não foi possível calcular o Teste T. Verifique "
                "a variância e os valores dos grupos."
            )

        significante = bool(p_valor < 0.05)

        return json.dumps(
            {
                "status": "sucesso",
                "grupo_1": str(grupos[0]),
                "media_grupo_1": round(float(grupo_a.mean()), 2),
                "grupo_2": str(grupos[1]),
                "media_grupo_2": round(float(grupo_b.mean()), 2),
                "diferenca_abs_medias": round(
                    float(abs(grupo_a.mean() - grupo_b.mean())), 2
                ),
                "p_valor": round(float(p_valor), 5),
                "estatisticamente_significante_95pct": significante,
                "conclusao": (
                    "Há evidência estatística de diferença entre os grupos "
                    "(p < 0.05)."
                    if significante
                    else "Não há evidência estatística suficiente de diferença "
                    "entre os grupos (p >= 0.05)."
                ),
            },
            ensure_ascii=False,
        )

    except Exception as e:
        return f"Erro ao realizar teste de hipótese estatística: {str(e)}"


if __name__ == "__main__":
    mcp.run()
