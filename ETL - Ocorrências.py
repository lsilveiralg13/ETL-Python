import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, DateTime, String
from sqlalchemy.schema import Table, Column, MetaData

# --- Seção 1: Configurações do Novo ETL ---
EXCEL_FILE = "OCORRENCIAS ETL.xlsx"
EXCEL_SHEET_NAME = "BASE"

DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'belmicro'
STAGING_TABLE_NAME = 'staging_ocorrencias' # Nome da nova tabela

# Mapeamento Simplificado para Ocorrências
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
    print(f"- Soma Total de QTD: {df['quantidade'].sum()}")
    
    if 'data_ocorrencia' in df.columns:
        print(f"- Período: {df['data_ocorrencia'].min()} até {df['data_ocorrencia'].max()}")
    
    # Verifica se há nulos na localidade (que quebraria o dashboard)
    nulos = df['localidade'].isna().sum()
    if nulos > 0:
        print(f"⚠️ Alerta: Existem {nulos} registros sem Localidade!")
    else:
        print("✅ Dados de Localidade íntegros.")
    print("— " * 21 + "🚀")

def run_etl_ocorrencias():
    try:
        print(f"--- Iniciando ETL OCORRÊNCIAS: {DB_NAME} ---")

        # 1. Extração
        df = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
        
        # 2. Transformação (Filtra apenas as colunas mapeadas)
        cols_to_use = list(COLUMN_MAPPING_AND_TYPES.keys())
        df = df[cols_to_use].copy()
        
        # Renomeia colunas
        new_names = {k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items()}
        df = df.rename(columns=new_names)

        # 2.1 Tratamento de Datas
        df['data_ocorrencia'] = pd.to_datetime(df['data_ocorrencia'], errors='coerce')
        
        # 2.2 Tratamento de Quantidade (Garante que é número)
        df['quantidade'] = pd.to_numeric(df['quantidade'], errors='coerce').fillna(0).astype(int)

        # 2.3 Timestamp de Carga
        df['data_carga_dw'] = pd.Timestamp.now()

        # 3. Carga (MySQL)
        mysql_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(mysql_url)
        
        metadata = MetaData()
        table_columns = [Column(v['new_name'], v['type']) for v in COLUMN_MAPPING_AND_TYPES.values()]
        table_columns.append(Column('data_carga_dw', DateTime))
        
        # Define a estrutura da tabela
        table_obj = Table(STAGING_TABLE_NAME, metadata, *table_columns)
        
        with engine.begin() as conn:
            print(f"🗑️ Limpando tabela: {STAGING_TABLE_NAME}")
            conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE_NAME}"))
            
            print(f"🔨 Criando estrutura...")
            metadata.create_all(conn)
            
            print(f"📤 Enviando {len(df)} linhas para o MySQL...")
            df.to_sql(STAGING_TABLE_NAME, conn, if_exists='append', index=False)
            
            # Auditoria
            auditoria_simples(df)

        print(f"✅ ETL OCORRÊNCIAS FINALIZADO!")

    except Exception as e:
        print(f"❌ ERRO NO PIPELINE: {e}")

if __name__ == "__main__":
    run_etl_ocorrencias()