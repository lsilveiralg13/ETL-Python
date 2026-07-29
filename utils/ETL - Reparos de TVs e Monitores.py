import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, String, DateTime, Text, Float
from sqlalchemy.schema import Table, Column, MetaData

# --- Seção 1: Configurações de Caminho ---
EXCEL_FILE_PATH = r"C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python\REPAROS TVS MONITORES ETL.xlsx"
EXCEL_SHEET_NAME = "BASE"

# --- Seção 2: Configurações de Banco ---
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'belmicro'
STAGING_TABLE_NAME = 'staging_reparos_tvs'

# --- Seção 3: Mapeamento de Colunas ---
COLUMN_MAPPING_AND_TYPES = {
    'FABRICANTE': {'new_name': 'fabricante', 'type': String(100)},
    'SKU': {'new_name': 'sku', 'type': Integer},
    'SERIES': {'new_name': 'num_serie', 'type': String(100)},
    'APARELHO': {'new_name': 'aparelho', 'type': String(100)},
    'POLEGADAS': {'new_name': 'polegadas', 'type': String(50)},
    'DEFEITO': {'new_name': 'defeito_reclamado', 'type': String(255)},
    'DIAGNÓSTICO': {'new_name': 'diagnostico_tecnico', 'type': Text},
    'AÇÃO': {'new_name': 'acao_realizada', 'type': String(255)},
    'DESMONTE E MONTAGEM TELA': {'new_name': 'desmonte_montagem_tela', 'type': String(100)},
    'EQUIPAMENTO USADO': {'new_name': 'equipamento_usado', 'type': String(100)},
    'SITUAÇÃO': {'new_name': 'situacao', 'type': String(50)},
    'TÉCNICO': {'new_name': 'tecnico', 'type': String(100)},
    'DATA DO REPARO': {'new_name': 'data_reparo', 'type': DateTime},
    'CHAVE_MMM': {'new_name': 'chave_mes', 'type': String(15)},
    'CHAVE_AAA': {'new_name': 'chave_ano', 'type': Integer},
    'QTD': {'new_name': 'quantidade', 'type': Integer}
}

def auditoria_simples(df):
    print("\n" + "📊" + " —" * 20)
    print(f"RESUMO DA CARGA (TVs/MONITORES): {len(df)} linhas")
    if 'situacao' in df.columns:
        print(df['situacao'].value_counts())
    print("— " * 21 + "🚀")

def run_etl_reparos_tvs():
    try:
        print(f"--- Iniciando Carga Staging: {STAGING_TABLE_NAME} ---")

        if not os.path.exists(EXCEL_FILE_PATH):
            print(f"❌ Arquivo não encontrado: {EXCEL_FILE_PATH}")
            return

        # 1. Leitura
        df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=EXCEL_SHEET_NAME)
        df.columns = df.columns.str.strip()
        
        # 2. Filtro e Renomeação
        cols_presentes = [c for c in COLUMN_MAPPING_AND_TYPES.keys() if c in df.columns]
        df = df[cols_presentes].rename(columns={k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items()})

        # 3. Tratamento de tipos
        if 'data_reparo' in df.columns:
            df['data_reparo'] = pd.to_datetime(df['data_reparo'], errors='coerce')
        
        if 'quantidade' in df.columns:
            df['quantidade'] = pd.to_numeric(df['quantidade'], errors='coerce').fillna(1).astype(int)
        
        if 'chave_ano' in df.columns:
             df['chave_ano'] = pd.to_numeric(df['chave_ano'], errors='coerce').fillna(0).astype(int)

        df['data_carga_dw'] = pd.Timestamp.now()

        # 4. Conexão e Carga
        engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        metadata = MetaData()
        
        # --- BLOCO DE DEFINIÇÃO DA TABELA COM AUTO_INCREMENT ---
        table_columns = [
            Column('id', Integer, primary_key=True, autoincrement=True) # ID automático definido aqui
        ]
        
        # Mapeia as outras colunas conforme o dicionário
        for v in COLUMN_MAPPING_AND_TYPES.values():
            table_columns.append(Column(v['new_name'], v['type']))
        
        table_columns.append(Column('data_carga_dw', DateTime))
        
        with engine.begin() as conn:
            # Dropa a tabela anterior para recriar com a PK
            conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE_NAME}"))
            
            # Cria a estrutura fisicamente no banco
            staging_table = Table(STAGING_TABLE_NAME, metadata, *table_columns)
            staging_table.create(conn)
            
            # Insere os dados (O MySQL gera os IDs automaticamente para cada linha inserida)
            df.to_sql(STAGING_TABLE_NAME, conn, if_exists='append', index=False)
            auditoria_simples(df)

        print(f"✅ CARGA CONCLUÍDA: {STAGING_TABLE_NAME} criada com ID Auto-Increment.")

    except Exception as e:
        print(f"❌ ERRO: {e}")

if __name__ == "__main__":
    run_etl_reparos_tvs()