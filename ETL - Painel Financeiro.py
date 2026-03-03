import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, String, Numeric, Text
from sqlalchemy.schema import Table, Column, MetaData, CreateTable
from datetime import datetime

# --- Seção 1: Configurações ---
EXCEL_FILE = "PAINEL FINANCEIRO.xlsx"
EXCEL_SHEET_NAME = "BASE"

DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_financeiro_multimarcas'

# Mapeamento atualizado conforme sua solicitação (13 colunas + timestamp)
COLUMN_MAPPING_AND_TYPES = {
    'Loja': {'new_name': 'loja', 'type': String(100)},
    'Código': {'new_name': 'codigo_parceiro', 'type': Integer},
    'Parceiro': {'new_name': 'nome_parceiro', 'type': String(255)},
    'CNPJ': {'new_name': 'cnpj_cpf', 'type': String(20)},
    'Limite Total': {'new_name': 'limite_total', 'type': Numeric(15, 2)},
    'Limite Disponível': {'new_name': 'limite_disponivel', 'type': Numeric(15, 2)},
    'Qtd. NF Faturadas': {'new_name': 'qtd_nf_faturadas', 'type': Integer},
    'Total em Aberto': {'new_name': 'total_em_aberto', 'type': Numeric(15, 2)},
    'Inadimplente': {'new_name': 'inadimplente', 'type': String(10)},
    'Dt. Cadastro': {'new_name': 'data_cadastro', 'type': DateTime},
    'MÊS': {'new_name': 'mes', 'type': String(20)},
    'ANO': {'new_name': 'ano', 'type': Numeric(15, 2)},
    'RÉGUA CADASTRO': {'new_name': 'regua_cadastro', 'type': String(50)},
    'vendedor': {'new_name': 'vendedor', 'type': String(100)},
    'status_vendedor': {'new_name': 'status_vendedor', 'type': String(50)},
    'STATUS': {'new_name': 'status', 'type': String(50)},
    'BASE DE ATIVOS': {'new_name': 'base_ativos', 'type': String(50)},
    'Maior Atraso Pgto': {'new_name': 'maior_atraso', 'type': Numeric(15, 2)},
    'data_carga_dw': {'new_name': 'data_carga_dw', 'type': DateTime}
}

# --- Seção 2: Pipeline ETL ---

def run_etl():
    df = None
    connection = None
    cursor = None

    try:
        print(f"--- Iniciando ETL de Parceiros para o Data Warehouse ---")

        # 1. Extração
        df_raw = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
        df = df_raw.copy()

        print("\n2. Iniciando transformações...")

        # 2.1 Seleção e renomeação
        # Filtramos apenas as colunas que você listou como desejadas
        columns_to_select = {excel_col: config['new_name'] for excel_col, config in COLUMN_MAPPING_AND_TYPES.items() if excel_col in df.columns}
        
        df = df[list(columns_to_select.keys())].rename(columns=columns_to_select)

        # 2.2 Conversão de tipos
        if 'data_cadastro' in df.columns:
            df['data_cadastro'] = pd.to_datetime(df['data_cadastro'], errors='coerce')
        
        numeric_cols = ['limite_total', 'limite_disponivel', 'total_em_aberto']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 2.3 Timestamp de carga
        df['data_carga_dw'] = pd.Timestamp.now()

        # 3. Carga para MySQL
        mysql_connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(mysql_connection_string)
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # DROP e CREATE TABLE
        cursor.execute(f"DROP TABLE IF EXISTS `{STAGING_TABLE_NAME}`;")
        
        metadata = MetaData()
        table_columns = [Column(config['new_name'], config['type'], nullable=True) for config in COLUMN_MAPPING_AND_TYPES.values()]
        table_obj = Table(STAGING_TABLE_NAME, metadata, *table_columns, mysql_engine='InnoDB', mysql_charset='utf8mb4')
        
        create_table_sql = str(CreateTable(table_obj).compile(engine))
        cursor.execute(create_table_sql)
        
        # Inserção
        column_order = list(df.columns)
        columns_sql = ", ".join([f"`{col}`" for col in column_order])
        placeholders = ", ".join(["%s"] * len(column_order))
        insert_sql = f"INSERT INTO `{STAGING_TABLE_NAME}` ({columns_sql}) VALUES ({placeholders})"

        data_to_insert = [tuple(x) for x in df.to_numpy()]
        
        cursor.executemany(insert_sql, data_to_insert)
        connection.commit()

        print(f"--- ETL Concluído! {len(df)} linhas carregadas em '{STAGING_TABLE_NAME}' ---")

    except Exception as e:
        print(f"Erro fatal: {e}")
    finally:
        if cursor: cursor.close()
        if connection: connection.close()

if __name__ == "__main__":
    run_etl()