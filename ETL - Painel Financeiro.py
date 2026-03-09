import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, String, Numeric
from sqlalchemy.schema import Table, Column, MetaData, CreateTable
import pymysql

# --- Seção 1: Configurações ---
EXCEL_FILE = "PAINEL FINANCEIRO.xlsx"
EXCEL_SHEET_NAME = "BASE"

DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_financeiro_multimarcas'

# Mapeamento: Apenas colunas que EXISTEM no Excel
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
    'Valor (R$) Inadim.': {'new_name': 'valor_inadimplente', 'type': Numeric(15, 2)},
    'Atraso em Dias': {'new_name': 'atraso_dias', 'type': Integer},
    'INADIMPLENTES': {'new_name': 'inadimplentes', 'type': String(10)},
    'MÊS': {'new_name': 'mes', 'type': String(20)},
    'ANO': {'new_name': 'ano', 'type': Numeric(15, 2)},
    'RÉGUA CADASTRO': {'new_name': 'regua_cadastro', 'type': String(50)},
    'vendedor': {'new_name': 'vendedor', 'type': String(100)},
    'status_vendedor': {'new_name': 'status_vendedor', 'type': String(50)},
    'STATUS': {'new_name': 'status', 'type': String(50)},
    'BASE DE ATIVOS': {'new_name': 'base_ativos', 'type': String(50)},
    'Maior Atraso Pgto': {'new_name': 'maior_atraso', 'type': Numeric(15, 2)}
}

# --- Seção 2: Pipeline ETL ---

def run_etl():
    connection = None
    cursor = None

    try:
        print(f"--- Iniciando ETL para o Data Warehouse ---")

        # 1. Extração
        df_raw = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
        
        # 2. Transformação
        print("\n2. Iniciando transformações...")

        # 2.1 Seleção apenas das colunas que existem no Excel e no mapeamento
        valid_cols = [col for col in COLUMN_MAPPING_AND_TYPES.keys() if col in df_raw.columns]
        df = df_raw[valid_cols].copy()
        
        # Renomeação
        rename_dict = {k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items()}
        df = df.rename(columns=rename_dict)

        # 2.2 Conversão de tipos e limpeza
        if 'data_cadastro' in df.columns:
            df['data_cadastro'] = pd.to_datetime(df['data_cadastro'], errors='coerce')
        
        numeric_cols = ['limite_total', 'limite_disponivel', 'total_em_aberto', 'valor_inadimplente', 'maior_atraso', 'ano']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 2.3 Adição da coluna de controle (Timestamp) após o filtro do Excel
        df['data_carga_dw'] = pd.Timestamp.now()

        # 3. Carga para MySQL
        mysql_connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(mysql_connection_string)
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # DROP TABLE
        cursor.execute(f"DROP TABLE IF EXISTS `{STAGING_TABLE_NAME}`;")
        
        # 4. Criação dinâmica da tabela baseada nas colunas REAIS do DataFrame final
        metadata = MetaData()
        table_columns = []
        
        for col_name in df.columns:
            # Busca o tipo no dicionário original ou define como DateTime para a coluna de carga
            if col_name == 'data_carga_dw':
                col_type = DateTime
            else:
                # Localiza o tipo original pelo novo nome
                col_type = next((v['type'] for k, v in COLUMN_MAPPING_AND_TYPES.items() if v['new_name'] == col_name), String(255))
            
            table_columns.append(Column(col_name, col_type, nullable=True))

        table_obj = Table(STAGING_TABLE_NAME, metadata, *table_columns, mysql_engine='InnoDB', mysql_charset='utf8mb4')
        
        # Executa o Create Table
        create_table_sql = str(CreateTable(table_obj).compile(engine))
        cursor.execute(create_table_sql)
        
        # 5. Inserção
        columns_sql = ", ".join([f"`{col}`" for col in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO `{STAGING_TABLE_NAME}` ({columns_sql}) VALUES ({placeholders})"

        # Converte NaT/NaN para None (que o MySQL entende como NULL)
        data_to_insert = df.where(pd.notnull(df), None).values.tolist()
        data_to_insert = [tuple(x) for x in data_to_insert]
        
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