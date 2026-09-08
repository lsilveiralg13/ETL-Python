# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,AVISO - Cópia Git (NÃO EXECUTAR)
# MAGIC %md
# MAGIC # ⚠️ CÓPIA PARA VERSIONAMENTO GIT — NÃO EXECUTAR
# MAGIC
# MAGIC > **Este notebook é uma cópia read-only para versionamento no repositório `ETL-Python`.**
# MAGIC >
# MAGIC > - As células de **escrita na tabela Delta** (`saveAsTable`) estão **comentadas** para evitar sobrescritas acidentais.
# MAGIC > - O notebook de **PRODUÇÃO** (executado pelo Job) fica em:
# MAGIC >   `/Users/lucas.barros@belmicro.com.br/Pipeline de Ingestao Sharepoint - Databricks (TELECONTROL OS)`
# MAGIC > - **NÃO descomente** as linhas de escrita. Se precisar alterar o pipeline, edite o notebook original e copie para cá depois.
# MAGIC > - **NÃO substitua** este notebook pelo original sem antes comentar as escritas.

# COMMAND ----------

# DBTITLE 1,Documentação do Pipeline
# MAGIC %md
# MAGIC # Download SharePoint — TELECONTROL OS
# MAGIC
# MAGIC Pipeline automatizado de ingestão do arquivo `TELECONTROL OS.xlsx` (aba **DADOS-OS**) do SharePoint para a tabela `gold.analistas.tb_dados_telecontrol_excel`.
# MAGIC
# MAGIC **Fluxo:** SharePoint → Download → Workspace → Leitura Excel → Overwrite tabela Delta (dados RAW)
# MAGIC
# MAGIC | Etapa | Descrição |
# MAGIC | --- | --- |
# MAGIC | 1 | Instalar dependência `Office365-REST-Python-Client` |
# MAGIC | 2 | Ler credenciais do Databricks Secrets |
# MAGIC | 3 | Baixar o .xlsx do SharePoint para /tmp e copiar ao Workspace |
# MAGIC | 4 | Ler aba DADOS-OS com `read_files` e sobrescrever a tabela Delta |
# MAGIC | 5 | Validar contagem de linhas e colunas |
# MAGIC
# MAGIC > Este notebook é a **primeira task** do job "Pipeline Qualidade Telecontrol OS - Refresh Diário". Após a execução, o notebook de pipeline aplica as transformações (Bronze → Silver).

# COMMAND ----------

# DBTITLE 1,Configuração de Secrets (executar UMA VEZ)
# MAGIC %md
# MAGIC ## ⚙️ Configuração de Secrets (executar UMA VEZ)
# MAGIC
# MAGIC Antes da primeira execução, armazene suas credenciais no scope `sharepoint-creds`.  
# MAGIC Abra **um notebook separado** (ou este mesmo, em uma célula avulsa) e execute o código abaixo **uma única vez**:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # Substitua pelos seus valores reais
# MAGIC w.secrets.put_secret(scope="sharepoint-creds", key="sharepoint-email", string_value="SEU_EMAIL@belmicro.com.br")
# MAGIC w.secrets.put_secret(scope="sharepoint-creds", key="sharepoint-password", string_value="SUA_SENHA")
# MAGIC ```
# MAGIC
# MAGIC > ⚠️ **Nunca** cole credenciais diretamente no código deste notebook. Os secrets são criptografados e acessíveis apenas via `dbutils.secrets.get()`.
# MAGIC >
# MAGIC > 💡 Se sua conta tiver **MFA habilitado**, a autenticação por usuário/senha não funcionará. Nesse caso, registre um **App Registration** no Azure AD e use `client_id`/`client_secret` (veja comentário na célula de download).

# COMMAND ----------

# DBTITLE 1,Instalação de Dependências
# MAGIC %pip install openpyxl --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuração e Autenticação
# ============================================================================
# CONFIGURAÇÃO
# ============================================================================
import os

# --- Credenciais do SharePoint (Databricks Secrets) ---
SP_EMAIL = dbutils.secrets.get(scope="sharepoint-creds", key="sharepoint-email")
SP_PASSWORD = dbutils.secrets.get(scope="sharepoint-creds", key="sharepoint-password")

# --- SharePoint ---
SITE_URL = "https://belmicrotech.sharepoint.com/sites/FileServerBelmicro"

# Caminho server-relative do arquivo no SharePoint
# Se o download falhar com 404, o arquivo pode estar em "Shared Documents".
# Nesse caso, descomente a linha alternativa abaixo.
FILE_SERVER_RELATIVE_URL = (
    "/sites/FileServerBelmicro/"
    "07 INDUSTRIAL/QUALIDADE/TELECONTROL/"
    "00 - ANUAL 2024, 2025 e 2026/TELECONTROL OS.xlsx"
)
# Alternativa (se estiver na biblioteca "Shared Documents"):
# FILE_SERVER_RELATIVE_URL = (
#     "/sites/FileServerBelmicro/Shared Documents/"
#     "07 INDUSTRIAL/QUALIDADE/TELECONTROL/"
#     "00 - ANUAL 2024, 2025 e 2026/TELECONTROL OS.xlsx"
# )

SHEET_NAME = "DADOS-OS"

# --- Paths ---
LOCAL_TMP_PATH = "/tmp/TELECONTROL_OS.xlsx"
WORKSPACE_PATH = "/Workspace/Users/lucas.barros@belmicro.com.br/TELECONTROL_OS.xlsx"

# --- Tabela destino ---
TABELA_DESTINO = "gold.analistas.tb_dados_telecontrol_excel"

print("✅ Configuração carregada")
print(f"   Site: {SITE_URL}")
print(f"   Arquivo: .../{os.path.basename(FILE_SERVER_RELATIVE_URL)}")
print(f"   Aba: {SHEET_NAME}")
print(f"   Tabela destino: {TABELA_DESTINO}")

# COMMAND ----------

# DBTITLE 1,Download do Arquivo do SharePoint
# ============================================================================
# DOWNLOAD DO ARQUIVO DO SHAREPOINT
# ============================================================================
import msal
import requests
import urllib.parse
import shutil

# --- Autenticação via MSAL (OAuth 2.0 ROPC) ---
# Microsoft aposentou o fluxo SAML/ACS para SharePoint Online.
# Usamos ROPC (Resource Owner Password Credential) via MSAL.
# Requer que a conta NÃO tenha MFA habilitado.
TENANT = SP_EMAIL.split("@")[1]  # belmicro.com.br
SP_RESOURCE = "https://belmicrotech.sharepoint.com"

print("⏳ Autenticando no SharePoint via OAuth 2.0...")
app = msal.PublicClientApplication(
    client_id="9bc3ab49-b65d-410a-85ad-de819febfddc",  # SharePoint Online Management Shell
    authority=f"https://login.microsoftonline.com/{TENANT}",
)
result = app.acquire_token_by_username_password(
    username=SP_EMAIL,
    password=SP_PASSWORD,
    scopes=[f"{SP_RESOURCE}/.default"],
)

if "access_token" not in result:
    error_msg = result.get("error_description", result.get("error", "Erro desconhecido"))
    raise RuntimeError(
        f"Falha na autenticação: {error_msg}\n\n"
        "Possíveis causas:\n"
        "  - MFA habilitado na conta\n"
        "  - Tenant bloqueou o fluxo ROPC\n"
        "  - Credenciais incorretas\n\n"
        "Solução: registrar um App Registration no Azure AD "
        "com permissões Sites.Read.All e usar client_id/client_secret."
    )

print("✅ Autenticação bem-sucedida!")

# --- Download via SharePoint REST API ---
encoded_path = urllib.parse.quote(FILE_SERVER_RELATIVE_URL, safe="/")
download_url = (
    f"{SITE_URL}/_api/web/GetFileByServerRelativeUrl('{encoded_path}')/$value"
)

print(f"⏳ Baixando: {os.path.basename(FILE_SERVER_RELATIVE_URL)}")
resp = requests.get(
    download_url,
    headers={"Authorization": f"Bearer {result['access_token']}"},
    stream=True,
)
resp.raise_for_status()

with open(LOCAL_TMP_PATH, "wb") as f:
    for chunk in resp.iter_content(chunk_size=65536):
        f.write(chunk)

file_size_mb = os.path.getsize(LOCAL_TMP_PATH) / (1024 * 1024)
print(f"✅ Download concluído: {file_size_mb:.2f} MB")

# Copiar para o Workspace (read_files funciona com paths /Workspace)
shutil.copy2(LOCAL_TMP_PATH, WORKSPACE_PATH)
print(f"✅ Copiado para: {WORKSPACE_PATH}")

# COMMAND ----------

# DBTITLE 1,Leitura do Excel e Escrita na Tabela Delta
# ============================================================================
# LEITURA DO EXCEL (aba DADOS-OS) E ESCRITA NA TABELA DELTA
# ============================================================================
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType

pdf = pd.read_excel(LOCAL_TMP_PATH, sheet_name=SHEET_NAME)

# --- DIAGNÓSTICO: verificar colunas de fórmula/data antes da conversão ---
DIAG_COLS = {
    "Data Abertura": "DataAbertura", "Data Finalização": "DataFinalizacao",
    "Data Conserto": "DataConserto", "Data Compra": "DataCompra",
    "EMISSAO": "DataEmissao", "SÉRIE DA PEÇA": "SeriePeca", "QTDE_3": "QtdePecas",
}
print("=== DIAGNÓSTICO PRÉ-CONVERSÃO ===")
for excel_col, clean_name in DIAG_COLS.items():
    if excel_col in pdf.columns:
        col = pdf[excel_col]
        nn = col.notna().sum()
        print(f"  {clean_name} ({excel_col}): {nn}/{len(pdf)} non-null | dtype={col.dtype} | sample={col.dropna().head(3).tolist()}")
    else:
        print(f"  {clean_name} ({excel_col}): COLUNA NÃO ENCONTRADA!")
print()

# Renomear colunas do Excel → nomes limpos esperados pelo pipeline downstream
COLUMN_MAP = {
    "ID": "ID", "Numero_OS": "NumeroOS", "Status": "Status",
    "Tipo Atendimento": "TipoAtendimento", "Data Abertura": "DataAbertura",
    "Data Finalização": "DataFinalizacao", "Data Conserto": "DataConserto",
    "Obs.": "Observacao", "Os Interação": "OsInteracao",
    "Defeito Reclamado": "DefeitoReclamado", "Defeito Constatado": "DefeitoConstatado",
    "FORNECEDOR": "Fornecedor", "Linha de Produtos": "LinhaProdutos",
    "Tipo de Produto": "TipoProduto", "CHAVE_SKU_PA": "ChaveSkuPA",
    "Código Produto": "CodigoProduto", "Descrição Produto": "DescricaoProduto",
    "Número de Série": "NumeroSerie", "Cidade": "Cidade", "UF": "UF",
    "Revenda": "Revenda", "Número NF": "NumeroNF", "Data Compra": "DataCompra",
    "CODIGO PEÇA": "CodigoPeca", "DESCRIÇÃO PEÇA": "DescricaoPeca",
    "MODELO": "Modelo", "SÉRIE DA PEÇA": "SeriePeca", "QTDE_3": "QtdePecas",
    "SERVIÇO REALIZ.": "ServicoRealizado", "PEDIDO": "Pedido",
    "STATUS PEDIDO": "StatusPedido", "NF": "NFPedido", "EMISSAO": "DataEmissao",
    "QTDE2": "QtdePedido", "QTDE": "Qtde", "LOCAL ASSISTENCIA": "LocalAssistencia",
}
pdf = pdf.rename(columns=COLUMN_MAP)

# Converter tudo para string — evita erro Arrow em colunas com tipos mistos
# (ex: NumeroOS tem int e str). O pipeline downstream faz a tipagem correta.
pdf = pdf.astype(str).replace({"nan": None, "NaT": None, "None": None})
schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
df_raw = spark.createDataFrame(pdf, schema=schema)

# Dropar _rescued_data se existir (artefato do read_files)
if "_rescued_data" in df_raw.columns:
    df_raw = df_raw.drop("_rescued_data")

row_count = df_raw.count()
col_count = len(df_raw.columns)

print(f"📊 Dados lidos do Excel:")
print(f"   Linhas: {row_count:,}")
print(f"   Colunas: {col_count}")

if row_count == 0:
    raise ValueError("❌ Nenhuma linha lida do Excel! Verifique o nome da aba e o dataAddress.")

# ⚠️ ESCRITA COMENTADA — esta é uma CÓPIA para versionamento Git.
# O notebook de PRODUÇÃO fica em: /Users/lucas.barros@belmicro.com.br/Pipeline de Ingestao Sharepoint - Databricks (TELECONTROL OS)
# NÃO descomente as linhas abaixo sem necessidade explícita.
#
# (
#     df_raw.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .saveAsTable(TABELA_DESTINO)
# )
#
# print(f"\n✅ Tabela {TABELA_DESTINO} sobrescrita com dados RAW!")
# print(f"   Próxima etapa: notebook 'Pipeline Qualidade Telecontrol OS' (transformações)")

# COMMAND ----------

# DBTITLE 1,Validação Final
# ============================================================================
# VALIDAÇÃO
# ============================================================================

df_check = spark.table(TABELA_DESTINO)
check_count = df_check.count()
check_cols = len(df_check.columns)

print("=" * 50)
print("  VALIDAÇÃO — INGESTÃO TELECONTROL OS")
print("=" * 50)
print(f"  Tabela: {TABELA_DESTINO}")
print(f"  Linhas: {check_count:,}")
print(f"  Colunas: {check_cols}")

if check_count > 0:
    print(f"\n✅ Ingestão concluída com sucesso!")
else:
    raise ValueError("❌ Tabela vazia após ingestão! Verifique as etapas anteriores.")

# Preview dos primeiros registros
display(df_check.limit(5))