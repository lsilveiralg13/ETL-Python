import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, String, DateTime, Text
from sqlalchemy.schema import Table, Column, MetaData

# --- Seção 1: Configurações de Caminho ---
EXCEL_FILE_PATH = r"C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python\REPARO PLACAS ETL.xlsx"
EXCEL_SHEET_NAME = "BASE"

# --- Seção 2: Configurações de Banco ---
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'belmicro'
STAGING_TABLE_NAME = 'staging_reparos'

# --- Seção 3: Mapeamento de Colunas (Atualizado com as Chaves) ---
COLUMN_MAPPING_AND_TYPES = {
    'ORIGEM': {'new_name': 'origem', 'type': String(50)},
    'SÉRIE': {'new_name': 'num_serie', 'type': String(100)},
    'SKU': {'new_name': 'sku', 'type': String(50)},
    'FORNECEDOR': {'new_name': 'fornecedor', 'type': String(100)},
    'MODELO': {'new_name': 'modelo', 'type': String(100)},
    'DEFEITO': {'new_name': 'defeito_reclamado', 'type': String(255)},
    'DIAGNÓSTICO': {'new_name': 'diagnostico_tecnico', 'type': Text},
    'AÇÃO': {'new_name': 'acao_realizada', 'type': String(255)},
    'SITUAÇÃO': {'new_name': 'situacao', 'type': String(50)},
    'TÉCNICO': {'new_name': 'tecnico', 'type': String(100)},
    'DATA DO REPARO': {'new_name': 'data_reparo', 'type': DateTime},
    'TESTE 100%': {'new_name': 'teste_final', 'type': String(10)},
    'PRODUTO': {'new_name': 'produto_desc', 'type': String(255)},
    'CHAVE_MES': {'new_name': 'chave_mes', 'type': String(15)},
    'CHAVE_ANO': {'new_name': 'chave_ano', 'type': Integer},
    'QTD': {'new_name': 'quantidade', 'type': Integer}
}

def auditoria_simples(df):
    print("\n" + "📊" + " —" * 20)
    print(f"RESUMO DA CARGA: {len(df)} linhas")
    if 'situacao' in df.columns:
        print(df['situacao'].value_counts())
    print("— " * 21 + "🚀")

def run_etl_reparos():
    try:
        print(f"--- Iniciando Carga Staging: {STAGING_TABLE_NAME} ---")

        if not os.path.exists(EXCEL_FILE_PATH):
            print(f"❌ Arquivo não encontrado: {EXCEL_FILE_PATH}")
            return

        # 1. Leitura
        df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=EXCEL_SHEET_NAME)
        
        # 2. Filtro e Renomeação (Garante que as chaves entrem no DataFrame)
        cols_presentes = [c for c in COLUMN_MAPPING_AND_TYPES.keys() if c in df.columns]
        df = df[cols_presentes].rename(columns={k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items()})

        # 3. Tratamento de tipos
        if 'data_reparo' in df.columns:
            df['data_reparo'] = pd.to_datetime(df['data_reparo'], errors='coerce')
        df['quantidade'] = df['quantidade'].fillna(1).astype(int)
        df['data_carga_dw'] = pd.Timestamp.now()

        # 4. Conexão e Carga
        engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        metadata = MetaData()
        
        # Define colunas fisicamente para o SQL
        table_columns = [Column(v['new_name'], v['type']) for v in COLUMN_MAPPING_AND_TYPES.values()]
        table_columns.append(Column('data_carga_dw', DateTime))
        
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE_NAME}"))
            
            # Cria a tabela garantindo que chave_mes e chave_ano existam
            staging_table = Table(STAGING_TABLE_NAME, metadata, *table_columns)
            staging_table.create(conn)
            
            df.to_sql(STAGING_TABLE_NAME, conn, if_exists='append', index=False)
            auditoria_simples(df)

        print(f"✅ CARGA CONCLUÍDA: {STAGING_TABLE_NAME} está atualizada.")

    except Exception as e:
        print(f"❌ ERRO: {e}")

if __name__ == "__main__":
    run_etl_reparos()