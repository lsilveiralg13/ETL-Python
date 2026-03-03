import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, BigInteger, String, Numeric, Float, Text
from sqlalchemy.schema import Table, Column, MetaData, CreateTable
import os

# --- Seção 1: Configurações ---
EXCEL_FILE = "Análise de Recorrência MM.xlsx"
EXCEL_SHEET_NAME = "BASE" # Ajuste o nome da aba se necessário

DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_recorrencia_multimarcas'

# Novo mapeamento de colunas baseado na sua lista
COLUMN_MAPPING_AND_TYPES = {
    'Código': {'new_name': 'codigo_parceiro', 'type': BigInteger},
    'Nome Fantasia': {'new_name': 'nome_fantasia', 'type': String(255)},
    'CNPJ': {'new_name': 'cnpj', 'type': String(20)},
    'Data Última Compra': {'new_name': 'data_ultima_compra', 'type': DateTime},
    'Mês de Apuração': {'new_name': 'mes_apuracao', 'type': String(50)},
    'Quantidade de Pedidos': {'new_name': 'qtd_pedidos', 'type': Integer},
    'Nº Dias Sem Compra': {'new_name': 'dias_sem_compra', 'type': Integer},
    'QTD': {'new_name': 'qtd_total', 'type': Integer},
    'STATUS': {'new_name': 'status', 'type': String(50)},
    'GRUPO STATUS': {'new_name': 'grupo_status', 'type': String(50)},
    'GRUPO RECORRÊNCIA': {'new_name': 'grupo_recorrencia', 'type': String(50)},
    'TIPO RECORRÊNCIA': {'new_name': 'tipo_recorrencia', 'type': String(50)},
    'DATA HOJE': {'new_name': 'data_hoje', 'type': DateTime},
    'DATA CADASTRO': {'new_name': 'data_cadastro', 'type': DateTime},
    'MMMM/AAAA': {'new_name': 'mes_ano_ref', 'type': String(20)},
    'RÉGUA CADASTRO': {'new_name': 'regua_cadastro', 'type': String(50)},
    'TEMPO BASE': {'new_name': 'tempo_base', 'type': String(50)},
    'CATEGORIA': {'new_name': 'categoria', 'type': String(50)},
    'Cidade': {'new_name': 'cidade', 'type': String(100)},
    'UF': {'new_name': 'uf', 'type': String(2)},
    'LIMITE TOTAL': {'new_name': 'limite_total', 'type': Numeric(15, 2)},
    'LIMITE UTILIZADO': {'new_name': 'limite_utilizado', 'type': Numeric(15, 2)},
    'LIMITE DISPONÍVEL': {'new_name': 'limite_disponivel', 'type': Numeric(15, 2)},
    'LIMITE SHOWROOM': {'new_name': 'limite_showroom', 'type': Numeric(15, 2)},
    'CLASSIFICAÇÃO LIMITE': {'new_name': 'classificacao_limite', 'type': String(50)},
    'Vendedor': {'new_name': 'vendedor', 'type': String(100)},
    'SDR': {'new_name': 'sdr', 'type': String(100)},
    'FATURAMENTO 30D': {'new_name': 'faturamento_30d', 'type': Numeric(15, 2)},
    'QTD PEDIDOS 30D': {'new_name': 'qtd_pedidos_30d', 'type': Integer},
    'FATURAMENTO 90D': {'new_name': 'faturamento_90d', 'type': Numeric(15, 2)},
    'QTD PEDIDOS 90 D': {'new_name': 'qtd_pedidos_90d', 'type': Integer},
    'TKM': {'new_name': 'tkm', 'type': Numeric(15, 2)},
    'data_carga_dw': {'new_name': 'data_carga_dw', 'type': DateTime}
}

# --- Seção 2: Funções Auxiliares (Mesma lógica do original) ---

def limpar_numericos(valor):
    if pd.isna(valor): return None
    if isinstance(valor, (int, float)): return float(valor)
    s = str(valor).replace('R$', '').replace('.', '').replace(',', '.').strip()
    try:
        return float(s)
    except:
        return None

# --- Seção 3: Pipeline ETL ---

def run_etl():
    try:
        print(f"--- Iniciando ETL Recorrência e TKM ---")
        df_raw = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
        df = df_raw.copy()

        # 1. Transformação: Renomeação e Seleção
        columns_to_select = {k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items() if k in df.columns}
        df = df[list(columns_to_select.keys())].rename(columns=columns_to_select)

        # 2. Limpeza de colunas de valores (Dinheiro e TKM)
        cols_financeiras = ['limite_total', 'limite_utilizado', 'limite_disponivel', 
                            'limite_showroom', 'faturamento_30d', 'faturamento_90d', 'tkm']
        for col in cols_financeiras:
            if col in df.columns:
                df[col] = df[col].apply(limpar_numericos)

        # 3. Tratamento de Datas
        cols_datas = ['data_ultima_compra', 'data_hoje', 'data_cadastro']
        for col in cols_datas:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        df['data_carga_dw'] = pd.Timestamp.now()

        # 4. Carga MySQL
        engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Criar Tabela DDL automática
        metadata = MetaData()
        table_columns = [Column(c['new_name'], c['type'], nullable=True) for c in COLUMN_MAPPING_AND_TYPES.values()]
        table_obj = Table(STAGING_TABLE_NAME, metadata, *table_columns)
        
        cursor.execute(f"DROP TABLE IF EXISTS `{STAGING_TABLE_NAME}`")
        cursor.execute(str(CreateTable(table_obj).compile(engine)))
        
        # Inserção em Lote (Bulk Insert)
        cols_sql = ", ".join([f"`{c['new_name']}`" for c in COLUMN_MAPPING_AND_TYPES.values()])
        placeholders = ", ".join(["%s"] * len(COLUMN_MAPPING_AND_TYPES))
        insert_sql = f"INSERT INTO `{STAGING_TABLE_NAME}` ({cols_sql}) VALUES ({placeholders})"

        # Preparar dados para tuplas
        data_to_insert = []
        for _, row in df.iterrows():
            processed_row = [None if pd.isna(row[c['new_name']]) else row[c['new_name']] for c in COLUMN_MAPPING_AND_TYPES.values()]
            data_to_insert.append(tuple(processed_row))

        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        print(f"Sucesso! {len(data_to_insert)} linhas carregadas na tabela {STAGING_TABLE_NAME}.")

    except Exception as e:
        print(f"Erro fatal: {e}")
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    run_etl()