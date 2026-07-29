import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, DateTime, String
from sqlalchemy.schema import Table, Column, MetaData

# --- Seção 1: Configurações do Novo ETL ---
EXCEL_FILE = "OCORRENCIAS ETL.xlsx"
EXCEL_SHEET_NAME = "BASE"

# --- Seção 2: Configurações de Banco ---
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'belmicro'
STAGING_TABLE_NAME = 'staging_ocorrencias'

# --- Seção 3: Mapeamento Simplificado para Ocorrências ---
COLUMN_MAPPING_AND_TYPES = {
    'Localidade': {'new_name': 'localidade', 'type': String(150)},
    'Data': {'new_name': 'data_ocorrencia', 'type': DateTime},
    'Mapa de Ocorrências e Riscos': {'new_name': 'mapa_riscos', 'type': String(255)},
    'QTD': {'new_name': 'quantidade', 'type': Integer},
    'CHAVE_MMM': {'new_name': 'chave_mes', 'type': String(10)},
    'CHAVE_AAA': {'new_name': 'chave_ano', 'type': Integer},
    'REGUA DIAS': {'new_name': 'regua_dias', 'type': Integer}
}

def auditoria_simples(df):
    """ Auditoria rápida para o novo fluxo de ocorrências """
    print("\n" + "🔍" + " —" * 20)
    print("RESUMO DE AUDITORIA: OCORRÊNCIAS")
    print("— " * 21)
    print(f"- Total de Linhas: {len(df)}")
    
    if 'quantidade' in df.columns:
        print(f"- Soma Total de QTD: {df['quantidade'].sum()}")
    
    if 'data_ocorrencia' in df.columns:
        print(f"- Período: {df['data_ocorrencia'].min()} até {df['data_ocorrencia'].max()}")
    
    nulos = df['localidade'].isna().sum() if 'localidade' in df.columns else 0
    if nulos > 0:
        print(f"⚠️ Alerta: Existem {nulos} registros sem Localidade!")
    else:
        print("✅ Dados de Localidade íntegros.")
    print("— " * 21 + "🚀")

def run_etl_ocorrencias():
    try:
        print(f"--- Iniciando ETL OCORRÊNCIAS: {DB_NAME} ---")

        # 1. Extração
        if not os.path.exists(EXCEL_FILE):
            print(f"❌ Arquivo não encontrado: {EXCEL_FILE}")
            return
            
        df = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
        
        # 2. Transformação
        cols_to_use = [c for c in COLUMN_MAPPING_AND_TYPES.keys() if c in df.columns]
        df = df[cols_to_use].copy()
        
        new_names = {k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items()}
        df = df.rename(columns=new_names)

        if 'data_ocorrencia' in df.columns:
            df['data_ocorrencia'] = pd.to_datetime(df['data_ocorrencia'], errors='coerce')
        
        if 'quantidade' in df.columns:
            df['quantidade'] = pd.to_numeric(df['quantidade'], errors='coerce').fillna(0).astype(int)

        df['data_carga_dw'] = pd.Timestamp.now()

        # 3. Conexão e Definição de Estrutura
        mysql_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(mysql_url)
        metadata = MetaData()
        
        # --- ESTRUTURA COM PRIMARY KEY E AUTO-INCREMENT ---
        table_columns = [
            Column('id', Integer, primary_key=True, autoincrement=True)
        ]
        
        for v in COLUMN_MAPPING_AND_TYPES.values():
            table_columns.append(Column(v['new_name'], v['type']))
        
        table_columns.append(Column('data_carga_dw', DateTime))
        
        # 4. Carga
        with engine.begin() as conn:
            print(f"🗑️ Limpando tabela: {STAGING_TABLE_NAME}")
            conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE_NAME}"))
            
            print(f"🔨 Criando estrutura com ID PK...")
            staging_table = Table(STAGING_TABLE_NAME, metadata, *table_columns)
            staging_table.create(conn)
            
            print(f"📤 Enviando {len(df)} linhas para o MySQL...")
            df.to_sql(STAGING_TABLE_NAME, conn, if_exists='append', index=False)
            
            auditoria_simples(df)

        print(f"✅ ETL OCORRÊNCIAS FINALIZADO!")

    except Exception as e:
        print(f"❌ ERRO NO PIPELINE: {e}")

if __name__ == "__main__":
    run_etl_ocorrencias()