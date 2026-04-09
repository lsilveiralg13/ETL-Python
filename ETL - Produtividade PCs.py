import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, String, DateTime, Text, Float
from sqlalchemy.schema import Table, Column, MetaData

# --- Seção 1: Configurações de Caminho ---
# Mantive o caminho original, ajuste se necessário para o novo arquivo
EXCEL_FILE_PATH = r"C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python\PRODUÇÃO PCs ETL.xlsx"
EXCEL_SHEET_NAME = "BASE"

# --- Seção 2: Configurações de Banco ---
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'belmicro'
STAGING_TABLE_NAME = 'staging_producao_pcs' # Sugestão de nome para diferenciar

# --- Seção 3: Mapeamento de Colunas (Atualizado) ---
COLUMN_MAPPING_AND_TYPES = {
    'DATA': {'new_name': 'data', 'type': DateTime},
    'NUMPS': {'new_name': 'num_ps', 'type': String(50)},
    'IDIPROC': {'new_name': 'id_processo', 'type': String(50)},
    'CODPRODPA': {'new_name': 'sku', 'type': String(50)},
    'DESCRPROD': {'new_name': 'descricao_produto', 'type': String(255)},
    'QTDOP': {'new_name': 'qtd_op', 'type': Integer},
    'SEPARACAO': {'new_name': 'separacao', 'type': Integer},
    'QUALIDADE': {'new_name': 'qualidade', 'type': Integer},
    'REPARO': {'new_name': 'reparo', 'type': Integer},
    'RUNIN': {'new_name': 'runin', 'type': Integer},
    'EMBALAGEM': {'new_name': 'embalagem', 'type': Integer},
    'PENDENTES': {'new_name': 'pendentes', 'type': Integer},
    'CONFIRMADO_NOTA': {'new_name': 'confirmado_nota', 'type': Integer},
    'PERCENTUAL_CONFIRMADO': {'new_name': 'perc_confirmado', 'type': Float},
    'STATUS_PRODUCAO': {'new_name': 'status_producao', 'type': String(100)},
    'CHAVE_MMM': {'new_name': 'chave_mes', 'type': String(15)},
    'CHAVE_AAA': {'new_name': 'chave_ano', 'type': Integer},
    'FORNECEDOR': {'new_name': 'fornecedor', 'type': String(255)}
}

def auditoria_simples(df):
    print("\n" + "📊" + " —" * 20)
    print(f"RESUMO DA CARGA: {len(df)} linhas")
    if 'status_producao' in df.columns:
        print(df['status_producao'].value_counts())
    print("— " * 21 + "🚀")

def run_etl_producao():
    try:
        print(f"--- Iniciando Carga Staging: {STAGING_TABLE_NAME} ---")

        if not os.path.exists(EXCEL_FILE_PATH):
            print(f"❌ Arquivo não encontrado: {EXCEL_FILE_PATH}")
            return

        # 1. Leitura
        df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=EXCEL_SHEET_NAME)
        
        # 2. Filtro e Renomeação
        cols_presentes = [c for c in COLUMN_MAPPING_AND_TYPES.keys() if c in df.columns]
        df = df[cols_presentes].rename(columns={k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items()})

        # 3. Tratamento de tipos
        if 'data' in df.columns:
            df['data'] = pd.to_datetime(df['data'], errors='coerce')
        
        # Preenchimento de nulos para colunas numéricas (evita erro na carga)
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)
        
        df['data_carga_dw'] = pd.Timestamp.now()

        # 4. Conexão e Carga
        engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        metadata = MetaData()
        
        # Definição da estrutura da tabela com ID
        table_columns = [
            Column('id', Integer, primary_key=True, autoincrement=True) 
        ]
        
        for v in COLUMN_MAPPING_AND_TYPES.values():
            table_columns.append(Column(v['new_name'], v['type']))
        
        table_columns.append(Column('data_carga_dw', DateTime))
        
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE_NAME}"))
            
            staging_table = Table(STAGING_TABLE_NAME, metadata, *table_columns)
            staging_table.create(conn)
            
            # O Pandas ignora o ID e o MySQL gera automaticamente
            df.to_sql(STAGING_TABLE_NAME, conn, if_exists='append', index=False)
            auditoria_simples(df)

        print(f"✅ CARGA CONCLUÍDA: {STAGING_TABLE_NAME} pronta!")

    except Exception as e:
        print(f"❌ ERRO: {e}")

if __name__ == "__main__":
    run_etl_producao()