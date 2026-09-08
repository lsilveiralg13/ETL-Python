# Databricks notebook source
# DBTITLE 1,AVISO - Cópia Git (NÃO EXECUTAR)
# MAGIC %md
# MAGIC # ⚠️ CÓPIA PARA VERSIONAMENTO GIT — NÃO EXECUTAR
# MAGIC
# MAGIC > **Este notebook é uma cópia read-only para versionamento no repositório `ETL-Python`.**
# MAGIC >
# MAGIC > - As células de **escrita na tabela Delta** (`saveAsTable`) estão **comentadas** para evitar sobrescritas acidentais.
# MAGIC > - O notebook de **PRODUÇÃO** (executado pelo Job) fica em:
# MAGIC >   `/Users/lucas.barros@belmicro.com.br/Pipeline Qualidade Telecontrol OS`
# MAGIC > - **NÃO descomente** as linhas de escrita. Se precisar alterar o pipeline, edite o notebook original e copie para cá depois.
# MAGIC > - **NÃO substitua** este notebook pelo original sem antes comentar as escritas.

# COMMAND ----------

# DBTITLE 1,Documentação do Pipeline
# MAGIC %md
# MAGIC # Pipeline Qualidade Telecontrol OS
# MAGIC
# MAGIC ## Objetivo
# MAGIC Pipeline de qualidade de dados para a tabela `gold.analistas.tb_dados_telecontrol_excel`, fonte: arquivo Excel TELECONTROL OS.xlsx (aba DADOS-OS).
# MAGIC
# MAGIC ## Fluxo: RAW → BRONZE → SILVER → Persistência
# MAGIC
# MAGIC | Etapa | Descrição |
# MAGIC | --- | --- |
# MAGIC | 0 | Leitura da tabela (detecta se schema é RAW ou CLEAN) |
# MAGIC | 1 | BRONZE: Padronização de nomes de colunas |
# MAGIC | 2 | BRONZE: Cast de tipos + parsing de datas Excel |
# MAGIC | 3 | SILVER: Limpeza textual, UPPER/TRIM, normalização de acentuação |
# MAGIC | 4 | SILVER: Deduplicação por NumeroOS + ChaveSkuPA |
# MAGIC | 4.5 | Enriquecimento: NomeGerente (canal comercial da venda original) |
# MAGIC | 5 | Relatório de qualidade (nulidade por coluna) |
# MAGIC | 6 | Persistência (overwrite com overwriteSchema=true) |
# MAGIC
# MAGIC ## Alterações — 2026-08-12
# MAGIC
# MAGIC ### ETAPA 4.5 — NomeGerente (NOVA)
# MAGIC - **Fonte:** `gold.sankhya.fato_operacoes` + `gold.sankhya.fato_itens`
# MAGIC - **Estratégia híbrida:**
# MAGIC   - Prioridade 1: JOIN por `NumeroNF` + `CodigoProduto` via `fato_itens` (99.5% precisão)
# MAGIC   - Prioridade 2: JOIN por `NumeroNF` apenas, gerente mais recente (fallback)
# MAGIC - **Cobertura alcançada:** 97.0% (16.794 de 17.312 registros)
# MAGIC - **Nota:** `NomeGerente` representa o **canal comercial** (MAGAZINE LUIZA, B2W, MERCADO LIVRE, etc.), não uma pessoa física
# MAGIC - **Correção aplicada:** `try_cast` + `regexp_replace` no NumeroNF para lidar com valores não-numéricos como '00.001.090'
# MAGIC
# MAGIC ### ETAPA 3 — Normalização de acentuação (EXPANDIDA)
# MAGIC - `Status`: AGUARDANDO PECAS → AGUARDANDO PEÇAS (561 registros)
# MAGIC - `DefeitoConstatado`: VIDEO → VÍDEO, AUDIO → ÁUDIO (64 registros)
# MAGIC - `ServicoRealizado`: SEM INFORMAÇÕES → SEM INFORMAÇÃO (29 registros)
# MAGIC
# MAGIC ### View `gold.analistas.vw_superset_indicadorat` (ATUALIZADA)
# MAGIC - `NomeGerente` agora lido de `T.NomeGerente` (antes era `CAST(NULL AS STRING)`)
# MAGIC - 3 colunas de reparo adicionadas: `DefeitoReclamado`, `DefeitoConstatado`, `StatusConserto`
# MAGIC - Corrigido artefato de texto colado na linha do SeriePA
# MAGIC
# MAGIC ## Fornecedores (nota)
# MAGIC - `HQ`, `HQ KGH`, `HQ KONKA` são distinções intencionais — identificam a fábrica OEM (KGH e KONKA) que produziu as TVs da marca HQ. Manter separados para rastreabilidade de peças/garantia.

# COMMAND ----------

# DBTITLE 1,ETAPA 0 - Leitura RAW e Profiling
# ============================================================================
# TELECONTROL OS — Pipeline de Qualidade de Dados
# Fonte: TELECONTROL OS.xlsx (aba DADOS-OS) — leitura direta do arquivo
# Processo: RAW (leitura) → BRONZE (schema + tipos) → SILVER (limpeza + dedup)
# Para atualizar: substitua o arquivo .xlsx no path abaixo e re-execute.
# ============================================================================

# --- ETAPA 0: Leitura da tabela (idempotente - funciona com schema raw ou clean) ---
TABELA = "gold.analistas.tb_dados_telecontrol_excel"

df_raw = spark.table(TABELA)

# Dropar _rescued_data se existir (artefato do upload raw)
if "_rescued_data" in df_raw.columns:
    df_raw = df_raw.drop("_rescued_data")

# Detectar se a tabela está no formato RAW (_c0, _c1...) ou já processada (ID, NumeroOS...)
IS_RAW = "_c0" in df_raw.columns

print(f"=== LEITURA ({TABELA}) ===")
print(f"Schema: {'RAW (precisa processar)' if IS_RAW else 'CLEAN (já processado)'}")
print(f"Colunas: {len(df_raw.columns)} | Linhas: {df_raw.count():,}")

# COMMAND ----------

# DBTITLE 1,ETAPA 1 - BRONZE: Schema + Tipagem
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType

# ============================================================================
# ETAPA 1 — BRONZE: Padronização de nomes de colunas (condicional)
# Se IS_RAW=True: renomeia de _c0/_c1 para nomes limpos + remove header row
# Se IS_RAW=False (já processado): apenas passa adiante
# ============================================================================

if IS_RAW:
    # Schema RAW: _c0, _c1, ... precisa renomear
    RAW_RENAME = {
        "_c0": "ID", "_c1": "NumeroOS", "_c2": "Status", "_c3": "TipoAtendimento",
        "_c4": "DataAbertura", "_c5": "DataFinalizacao", "_c6": "DataConserto",
        "_c7": "Observacao", "_c8": "OsInteracao", "_c9": "DefeitoReclamado",
        "_c10": "DefeitoConstatado", "_c11": "Fornecedor", "_c12": "LinhaProdutos",
        "_c13": "TipoProduto", "_c14": "ChaveSkuPA", "_c15": "CodigoProduto",
        "_c16": "DescricaoProduto", "_c17": "NumeroSerie", "_c18": "Cidade",
        "_c19": "UF", "_c20": "Revenda", "_c21": "NumeroNF", "_c22": "DataCompra",
        "_c23": "CodigoPeca", "_c24": "DescricaoPeca", "_c25": "Modelo",
        "_c26": "SeriePeca", "_c27": "QtdePecas", "_c28": "ServicoRealizado",
        "_c29": "Pedido", "_c30": "StatusPedido", "_c31": "NFPedido",
        "_c32": "DataEmissao", "_c33": "QtdePedido", "_c34": "Qtde",
        "_c35": "LocalAssistencia"
    }
    df_bronze = df_raw.filter(F.col("_c0") != "ID")  # Remover header row
    for old_col, new_col in RAW_RENAME.items():
        if old_col in df_bronze.columns:
            df_bronze = df_bronze.withColumnRenamed(old_col, new_col)
else:
    # Schema já limpo: apenas repassa
    df_bronze = df_raw

print(f"BRONZE: {df_bronze.count():,} linhas | {len(df_bronze.columns)} colunas")
print(f"Colunas: {df_bronze.columns}")

# COMMAND ----------

# DBTITLE 1,ETAPA 2 - BRONZE: Cast de Tipos + Parsing Datas
# ============================================================================
# ETAPA 2 - BRONZE: Casting de tipos + Parsing de datas (condicional)
# Se tabela ja esta tipada (DATE/INT), pula o cast. Senao, aplica SQL.
# ============================================================================
from pyspark.sql.types import StringType

needs_cast = isinstance(df_bronze.schema["DataAbertura"].dataType, StringType)
DATE_COLS = ["DataAbertura", "DataFinalizacao", "DataConserto", "DataCompra", "DataEmissao"]
INT_COLS = ["ID", "QtdePecas", "QtdePedido", "Qtde"]

if needs_cast:
    df_bronze.createOrReplaceTempView("bronze_raw")
    df_bronze = spark.sql("""
    SELECT
        try_cast(ID as INT) AS ID, NumeroOS, Status, TipoAtendimento,
        CASE WHEN DataAbertura RLIKE '^[0-9]+$' THEN date_add('1899-12-30', cast(DataAbertura as int))
             WHEN DataAbertura RLIKE '^[0-9]{4}-' THEN try_to_date(substring(DataAbertura, 1, 10), 'yyyy-MM-dd')
             ELSE coalesce(try_to_date(DataAbertura, 'M/d/yy'), try_to_date(DataAbertura, 'M/d/yyyy')) END AS DataAbertura,
        CASE WHEN DataFinalizacao RLIKE '^[0-9]+$' THEN date_add('1899-12-30', cast(DataFinalizacao as int))
             WHEN DataFinalizacao RLIKE '^[0-9]{4}-' THEN try_to_date(substring(DataFinalizacao, 1, 10), 'yyyy-MM-dd')
             ELSE coalesce(try_to_date(DataFinalizacao, 'M/d/yy'), try_to_date(DataFinalizacao, 'M/d/yyyy')) END AS DataFinalizacao,
        CASE WHEN DataConserto RLIKE '^[0-9]+$' THEN date_add('1899-12-30', cast(DataConserto as int))
             WHEN DataConserto RLIKE '^[0-9]{4}-' THEN try_to_date(substring(DataConserto, 1, 10), 'yyyy-MM-dd')
             ELSE coalesce(try_to_date(DataConserto, 'M/d/yy'), try_to_date(DataConserto, 'M/d/yyyy')) END AS DataConserto,
        Observacao, OsInteracao, DefeitoReclamado, DefeitoConstatado,
        Fornecedor, LinhaProdutos, TipoProduto, ChaveSkuPA, CodigoProduto,
        DescricaoProduto, NumeroSerie, Cidade, UF, Revenda, NumeroNF,
        CASE WHEN DataCompra RLIKE '^[0-9]+$' THEN date_add('1899-12-30', cast(DataCompra as int))
             WHEN DataCompra RLIKE '^[0-9]{4}-' THEN try_to_date(substring(DataCompra, 1, 10), 'yyyy-MM-dd')
             ELSE coalesce(try_to_date(DataCompra, 'M/d/yy'), try_to_date(DataCompra, 'M/d/yyyy')) END AS DataCompra,
        CodigoPeca, DescricaoPeca,
        CASE WHEN upper(trim(Modelo)) IN ('ERROR:#N/A','#N/A','#REF!','#VALUE!','N/A') THEN NULL ELSE Modelo END AS Modelo,
        CAST(try_cast(QtdePecas AS DOUBLE) AS INT) AS QtdePecas,
        ServicoRealizado, Pedido, StatusPedido, NFPedido,
        CASE WHEN DataEmissao RLIKE '^[0-9]+$' THEN date_add('1899-12-30', cast(DataEmissao as int))
             WHEN DataEmissao RLIKE '^[0-9]{4}-' THEN try_to_date(substring(DataEmissao, 1, 10), 'yyyy-MM-dd')
             ELSE coalesce(try_to_date(DataEmissao, 'M/d/yy'), try_to_date(DataEmissao, 'M/d/yyyy')) END AS DataEmissao,
        CAST(try_cast(QtdePedido AS DOUBLE) AS INT) AS QtdePedido,
        CAST(try_cast(Qtde AS DOUBLE) AS INT) AS Qtde,
        LocalAssistencia
    FROM bronze_raw
    WHERE coalesce(NumeroOS, Status, DataAbertura) IS NOT NULL
    """)
    print("Cast aplicado (RAW -> tipado)")
else:
    print("Schema ja tipado, skip cast")

# Remover SeriePeca (fórmula Excel sem cache — pandas não resolve)
if "SeriePeca" in df_bronze.columns:
    df_bronze = df_bronze.drop("SeriePeca")

print(f"Schema: {len(df_bronze.columns)} colunas | {df_bronze.count():,} linhas")
df_bronze.printSchema()

# COMMAND ----------

# DBTITLE 1,ETAPA 3 - SILVER: Limpeza Textual + Normalização
# ============================================================================
# ETAPA 3 — SILVER: Limpeza textual, normalização e remoção de vazios
# ============================================================================

# Colunas categóricas que devem ser UPPER + TRIM
CATEGORICAL_UPPER = [
    "Status", "TipoAtendimento", "DefeitoReclamado", "DefeitoConstatado",
    "Fornecedor", "LinhaProdutos", "TipoProduto", "Cidade", "UF",
    "Revenda", "ServicoRealizado", "StatusPedido", "LocalAssistencia"
]

# Colunas de texto livre que só recebem TRIM (sem UPPER)
TEXT_TRIM_ONLY = [
    "Observacao", "OsInteracao", "DescricaoProduto", "DescricaoPeca", "Modelo"
]

df_silver = df_bronze

# 1. UPPER + TRIM nas categóricas
for col_name in CATEGORICAL_UPPER:
    df_silver = df_silver.withColumn(
        col_name,
        F.upper(F.trim(F.col(col_name)))
    )

# 2. TRIM nas colunas de texto livre
for col_name in TEXT_TRIM_ONLY:
    df_silver = df_silver.withColumn(
        col_name,
        F.trim(F.col(col_name))
    )

# 3. TRIM em todas as demais STRING que não foram tratadas acima
treated = set(CATEGORICAL_UPPER + TEXT_TRIM_ONLY + DATE_COLS + INT_COLS)
for col_name in df_silver.columns:
    if col_name not in treated:
        df_silver = df_silver.withColumn(
            col_name,
            F.trim(F.col(col_name))
        )

# 4. Substituir strings vazias por NULL em TODAS as colunas string
string_cols = [f.name for f in df_silver.schema.fields if str(f.dataType) == "StringType()"]
for col_name in string_cols:
    df_silver = df_silver.withColumn(
        col_name,
        F.when(F.col(col_name) == "", None).otherwise(F.col(col_name))
    )

# 5. Normalizar Status (remover espaços extras, padronizar)
df_silver = df_silver.withColumn(
    "Status",
    F.regexp_replace(F.col("Status"), r"\s+", " ")
)

# 6. Normalizar acentuação inconsistente
# Status: PECAS → PEÇAS
df_silver = df_silver.withColumn(
    "Status",
    F.regexp_replace(F.col("Status"), "AGUARDANDO PECAS", "AGUARDANDO PEÇAS")
)

# DefeitoConstatado: VIDEO → VÍDEO, AUDIO → ÁUDIO
df_silver = df_silver.withColumn(
    "DefeitoConstatado",
    F.regexp_replace(F.col("DefeitoConstatado"), "VIDEO", "VÍDEO")
)
df_silver = df_silver.withColumn(
    "DefeitoConstatado",
    F.regexp_replace(F.col("DefeitoConstatado"), "AUDIO", "ÁUDIO")
)

# ServicoRealizado: SEM INFORMAÇÕES → SEM INFORMAÇÃO
df_silver = df_silver.withColumn(
    "ServicoRealizado",
    F.regexp_replace(F.col("ServicoRealizado"), "SEM INFORMAÇÕES", "SEM INFORMAÇÃO")
)

print("=== LIMPEZA TEXTUAL CONCLUÍDA ===")
print(f"Linhas: {df_silver.count():,}")

# Amostra
display(df_silver.select("ID", "NumeroOS", "Status", "TipoAtendimento", "DataAbertura", 
                          "Fornecedor", "DefeitoReclamado", "DefeitoConstatado").limit(5))

# COMMAND ----------

# DBTITLE 1,ETAPA 4 - SILVER: Deduplicação
# ============================================================================
# ETAPA 4 — SILVER: Deduplicação inteligente
# ============================================================================
from pyspark.sql.window import Window

# Identificar duplicatas:
# Chave natural: NumeroOS + ChaveSkuPA (OS + item específico)
# Em caso de duplicata, manter a linha mais recente (maior ID)

total_antes = df_silver.count()

# Checar duplicatas pela chave natural
duplicatas = df_silver.groupBy("NumeroOS", "ChaveSkuPA").count().filter(F.col("count") > 1)
qtd_duplicatas = duplicatas.count()

print(f"=== DEDUPLICAÇÃO ===")
print(f"Total linhas antes: {total_antes:,}")
print(f"Combinações NumeroOS+ChaveSkuPA duplicadas: {qtd_duplicatas:,}")

if qtd_duplicatas > 0:
    # Mostrar exemplos de duplicatas
    print("\nExemplos de duplicatas:")
    display(duplicatas.orderBy(F.col("count").desc()).limit(5))

    # Deduplicar: manter o maior ID (registro mais recente)
    w = Window.partitionBy("NumeroOS", "ChaveSkuPA").orderBy(F.col("ID").desc())
    df_silver = (
        df_silver
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    total_depois = df_silver.count()
    print(f"\nTotal linhas após dedup: {total_depois:,}")
    print(f"Removidas: {total_antes - total_depois:,}")
else:
    print("\n✅ Nenhuma duplicata encontrada!")

# COMMAND ----------

# DBTITLE 1,ETAPA 4.5 - Enriquecimento: NomeGerente (canal comercial)
# ============================================================================
# ETAPA 4.5 — ENRIQUECIMENTO: NomeGerente (canal comercial da venda original)
# Fonte: gold.sankhya.fato_operacoes + gold.sankhya.fato_itens
# Estratégia híbrida:
#   Prioridade 1: NF + SKU via fato_itens (99.5% precisão)
#   Prioridade 2: NF-only, gerente mais recente (fallback ~91.5% cobertura total)
# ============================================================================
from pyspark.sql.window import Window

# --- Lookup 1: NF + SKU (alta precisão) ---
df_fato_op = spark.table("gold.sankhya.fato_operacoes").filter(
    (F.col("TipoAnalise") == "Venda") &
    (F.col("NomeGerente").isNotNull()) &
    (F.col("NomeGerente") != "<SEM VENDEDOR>")
).select("NumUnicoNota", "NumNota", "NomeGerente", "DataFaturamento")

df_fato_itens = spark.table("gold.sankhya.fato_itens").select("NumUnicoNota", "CodProduto")

# Join fato_operacoes + fato_itens para obter NF+SKU → Gerente
w_nf_sku = Window.partitionBy("NumNota", "CodProduto").orderBy(F.col("DataFaturamento").desc())

df_gerente_nf_sku = (
    df_fato_op
    .join(df_fato_itens, "NumUnicoNota")
    .select("NumNota", "CodProduto", "NomeGerente", "DataFaturamento")
    .withColumn("rn", F.row_number().over(w_nf_sku))
    .filter(F.col("rn") == 1)
    .select(
        F.col("NumNota").alias("_lk1_NumNota"),
        F.col("CodProduto").alias("_lk1_SKU"),
        F.col("NomeGerente").alias("_lk1_Gerente")
    )
)

# --- Lookup 2: NF-only (fallback - gerente mais recente para aquela NF) ---
w_nf = Window.partitionBy("NumNota").orderBy(F.col("DataFaturamento").desc())

df_gerente_nf = (
    df_fato_op
    .withColumn("rn", F.row_number().over(w_nf))
    .filter(F.col("rn") == 1)
    .select(
        F.col("NumNota").alias("_lk2_NumNota"),
        F.col("NomeGerente").alias("_lk2_Gerente")
    )
)

# --- Join com df_silver ---
# Preparar colunas de join (cast para INT)
df_silver = df_silver.withColumn("_NumNF_int", F.expr("try_cast(regexp_replace(NumeroNF, '[^0-9]', '') as INT)"))
df_silver = df_silver.withColumn("_SKU_int", F.expr("try_cast(CodigoProduto as INT)"))

# Join Prioridade 1: NF + SKU
df_silver = df_silver.join(
    df_gerente_nf_sku,
    (F.col("_NumNF_int") == F.col("_lk1_NumNota")) & (F.col("_SKU_int") == F.col("_lk1_SKU")),
    "left"
)

# Join Prioridade 2: NF-only (fallback)
df_silver = df_silver.join(
    df_gerente_nf,
    F.col("_NumNF_int") == F.col("_lk2_NumNota"),
    "left"
)

# COALESCE: prioriza NF+SKU, fallback para NF-only
df_silver = df_silver.withColumn(
    "NomeGerente",
    F.coalesce(F.col("_lk1_Gerente"), F.col("_lk2_Gerente"))
)

# Limpar colunas auxiliares
df_silver = df_silver.drop(
    "_NumNF_int", "_SKU_int",
    "_lk1_NumNota", "_lk1_SKU", "_lk1_Gerente",
    "_lk2_NumNota", "_lk2_Gerente"
)

# --- Relatório ---
total = df_silver.count()
com_gerente = df_silver.filter(F.col("NomeGerente").isNotNull()).count()
pct = round(com_gerente * 100.0 / total, 1)

print("=== ENRIQUECIMENTO: NomeGerente ===")
print(f"Total linhas: {total:,}")
print(f"Com NomeGerente: {com_gerente:,} ({pct}%)")
print(f"Sem NomeGerente: {total - com_gerente:,} ({round(100 - pct, 1)}%)")
print(f"\nCanais distintos: {df_silver.filter(F.col('NomeGerente').isNotNull()).select('NomeGerente').distinct().count()}")
print("\nTop 10 canais:")
display(df_silver.groupBy("NomeGerente").count().orderBy(F.col("count").desc()).limit(10))

# COMMAND ----------

# DBTITLE 1,ETAPA 5 - Relatório de Qualidade
# ============================================================================
# ETAPA 5 — RELATÓRIO DE QUALIDADE FINAL
# ============================================================================
import pandas as pd

total = df_silver.count()

# Calcular nulidade por coluna
null_report = []
for col_name in df_silver.columns:
    null_count = df_silver.filter(F.col(col_name).isNull()).count()
    null_pct = (null_count / total) * 100 if total > 0 else 0
    null_report.append({
        "Coluna": col_name,
        "Tipo": str(df_silver.schema[col_name].dataType),
        "Nulos": null_count,
        "Pct_Nulos": round(null_pct, 1),
        "Preenchidos": total - null_count
    })

df_quality = pd.DataFrame(null_report).sort_values("Pct_Nulos", ascending=False)

print(f"=" * 60)
print(f"  RELATÓRIO DE QUALIDADE — TELECONTROL OS (SILVER)")
print(f"=" * 60)
print(f"  Total de registros: {total:,}")
print(f"  Total de colunas:   {len(df_silver.columns)}")
print(f"  Colunas 100% preenchidas: {len(df_quality[df_quality['Pct_Nulos'] == 0])}")
print(f"  Colunas com >50% nulos:   {len(df_quality[df_quality['Pct_Nulos'] > 50])}")
print(f"=" * 60)

display(df_quality)

# COMMAND ----------

# DBTITLE 1,ETAPA 6 - Persistência (COMENTADA - cópia Git)
# ============================================================================
# ETAPA 6 — PERSISTÊNCIA (COMENTADA — cópia Git)
# ============================================================================
#
# ⚠️ ESCRITA COMENTADA — esta é uma CÓPIA para versionamento Git.
# O notebook de PRODUÇÃO fica em: /Users/lucas.barros@belmicro.com.br/Pipeline Qualidade Telecontrol OS
# NÃO descomente as linhas abaixo sem necessidade explícita.
#
# TABELA_DESTINO = "gold.analistas.tb_dados_telecontrol_excel"
#
# (
#     df_silver.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .saveAsTable(TABELA_DESTINO)
# )
#
# df_final = spark.table(TABELA_DESTINO)
# print(f"✅ Tabela {TABELA_DESTINO} atualizada com sucesso!")
# print(f"   Registros: {df_final.count():,} | Colunas: {len(df_final.columns)}")
# df_final.printSchema()