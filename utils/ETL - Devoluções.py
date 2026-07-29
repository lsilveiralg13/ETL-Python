import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, String, Numeric, Text
from sqlalchemy.schema import Table, Column, MetaData, CreateTable
import pymysql

# --- Seção 1: Configurações ---
EXCEL_FILE = "DEVOLUÇÕES MM.xlsx"
EXCEL_SHEET_NAME = "BASE DEVOLUÇÃO"

DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_devolucoes_multimarcas'

# Mapeamento: Colunas do Excel -> Nomes SQL e Tipos
COLUMN_MAPPING_AND_TYPES = {
    'MOTIVO': {'new_name': 'motivo', 'type': String(255)},
    'JUSTIFICATIVA': {'new_name': 'justificativa', 'type': Text},
    'Dt. do Movimento': {'new_name': 'data_movimento', 'type': DateTime},
    'CHAVE MMM': {'new_name': 'chave_mmm', 'type': String(100)},
    'ANO': {'new_name': 'chave_aaa', 'type': Integer},
    'Nome Parceiro (Parceiro)': {'new_name': 'nome_parceiro', 'type': String(255)},
    'Vlr. Nota': {'new_name': 'valor_faturado', 'type': Numeric(15, 2)},
    'Apelido (Vendedor)': {'new_name': 'vendedor', 'type': String(100)},
    'Cod. Parceiro': {'new_name': 'codigo_parceiro', 'type': Integer},
    'CIDADE': {'new_name': 'cidade', 'type': String(100)},
    'ESTADO': {'new_name': 'uf', 'type': String(2)}
}

# --- Seção 2: Pipeline ETL ---

def run_etl_devolucoes():
    connection = None
    cursor = None

    try:
        print(f"--- Iniciando ETL de Devoluções para o Data Warehouse ---")

        # 1. Extração
        df_raw = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
        
        # 2. Transformação
        print("\n2. Iniciando transformações...")

        # 2.1 Seleção apenas das colunas desejadas que existem no arquivo
        valid_cols = [col for col in COLUMN_MAPPING_AND_TYPES.keys() if col in df_raw.columns]
        df = df_raw[valid_cols].copy()
        
        # Renomeação conforme o dicionário
        rename_dict = {k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items()}
        df = df.rename(columns=rename_dict)

        # 2.2 Conversão de tipos e limpeza
        if 'data_movimento' in df.columns:
            df['data_movimento'] = pd.to_datetime(df['data_movimento'], errors='coerce')
        
        # Tratamento de Numéricos
        numeric_cols = ['valor_nota', 'ano', 'codigo_parceiro']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 2.3 Coluna de controle de carga
        df['data_carga_dw'] = pd.Timestamp.now()

        # 3. Configuração da Carga (MySQL)
        mysql_connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(mysql_connection_string)
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # Limpa tabela anterior
        cursor.execute(f"DROP TABLE IF EXISTS `{STAGING_TABLE_NAME}`;")
        
        # 4. Criação dinâmica da tabela
        metadata = MetaData()
        table_columns = []
        
        for col_name in df.columns:
            if col_name == 'data_carga_dw':
                col_type = DateTime
            else:
                # Busca o tipo no mapeamento; se não achar, usa String(255)
                col_type = next((v['type'] for k, v in COLUMN_MAPPING_AND_TYPES.items() if v['new_name'] == col_name), String(255))
            
            table_columns.append(Column(col_name, col_type, nullable=True))

        table_obj = Table(STAGING_TABLE_NAME, metadata, *table_columns, mysql_engine='InnoDB', mysql_charset='utf8mb4')
        create_table_sql = str(CreateTable(table_obj).compile(engine))
        cursor.execute(create_table_sql)
        
        # 5. Inserção em Massa
        columns_sql = ", ".join([f"`{col}`" for col in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO `{STAGING_TABLE_NAME}` ({columns_sql}) VALUES ({placeholders})"

        # Tratamento final para converter NaT/NaN em None (NULL no MySQL)
        data_to_insert = df.where(pd.notnull(df), None).values.tolist()
        data_to_insert = [tuple(x) for x in data_to_insert]
        
        cursor.executemany(insert_sql, data_to_insert)
        connection.commit()

        print(f"--- ETL Concluído! {len(df)} linhas inseridas em '{STAGING_TABLE_NAME}' ---")

    except Exception as e:
        print(f"Erro no processamento: {e}")
    finally:
        if cursor: cursor.close()
        if connection: connection.close()

if __name__ == "__main__":
    run_etl_devolucoes()