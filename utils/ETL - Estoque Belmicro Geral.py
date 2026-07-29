import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, String, DateTime
from sqlalchemy.schema import Table, Column, MetaData

# --- Seção 1: Configurações ---
EXCEL_FILE = "ESTOQUE BELMICRO ETL.xlsx"
EXCEL_SHEET_NAME = "BASE"

# --- Seção 2: Configurações de Banco ---
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'belmicro'
STAGING_TABLE_NAME = 'staging_estoque_belmicro'

# --- Seção 3: Mapeamento de Colunas ---
COLUMN_MAPPING_AND_TYPES = {
    'Grupo': {'new_name': 'grupo', 'type': String(100)},
    'Marca': {'new_name': 'marca', 'type': String(100)},
    'SKU': {'new_name': 'sku', 'type': String(50)},
    'Produto': {'new_name': 'produto', 'type': String(255)},
    'Estoque': {'new_name': 'estoque', 'type': Integer},
    'Reserva': {'new_name': 'reserva', 'type': Integer}
}

def auditoria_estoque(df):
    print("\n" + "🔍" + " —" * 25)
    print("RESUMO DE AUDITORIA: ESTOQUE BELMICRO")
    print("— " * 26)

    print(f"📌 CHECK-UP DE DADOS:")
    print(f"- SKUs Processados: {len(df)}")
    
    total_disponivel = df['estoque'].sum() - df['reserva'].sum()
    print(f"- Total Peças em Estoque: {df['estoque'].sum()}")
    print(f"- Total Peças Reservadas: {df['reserva'].sum()}")
    print(f"- Saldo Disponível (Net): {total_disponivel}")

    if 'estoque' in df.columns:
        print(f"\n📈 TOP 3 PRODUTOS (MAIOR ESTOQUE):")
        top_3 = df.nlargest(3, 'estoque')[['produto', 'estoque']]
        for _, row in top_3.iterrows():
            print(f"  • {row['produto']}: {row['estoque']} un.")

    print("\n" + "— " * 30 + "🚀")

def run_etl_estoque():
    try:
        print(f"--- Iniciando ETL de Estoque: {DB_NAME} ---")

        # 1. Extração
        if not os.path.exists(EXCEL_FILE):
            print(f"❌ Arquivo não encontrado: {EXCEL_FILE}")
            return
            
        df = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
        print(f"✅ Extração concluída: {len(df)} linhas lidas.")

        # 2. Transformação
        cols_to_keep = [c for c in COLUMN_MAPPING_AND_TYPES.keys() if c in df.columns]
        df = df[cols_to_keep].rename(columns={k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items()})

        df['estoque'] = df['estoque'].fillna(0).astype(int)
        df['reserva'] = df['reserva'].fillna(0).astype(int)
        df['data_carga_dw'] = pd.Timestamp.now()

        # 3. Conexão e Definição de Estrutura
        mysql_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(mysql_url)
        metadata = MetaData()
        
        # --- DEFINIÇÃO DA COLUNA ID COM AUTO-INCREMENT ---
        table_columns = [
            Column('id', Integer, primary_key=True, autoincrement=True)
        ]
        
        # Adiciona colunas do mapeamento
        for v in COLUMN_MAPPING_AND_TYPES.values():
            table_columns.append(Column(v['new_name'], v['type']))
        
        table_columns.append(Column('data_carga_dw', DateTime))
        
        # 4. Carga
        with engine.begin() as conn:
            print(f"🗑️ Limpando staging: {STAGING_TABLE_NAME}")
            conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE_NAME}"))
            
            print(f"🔨 Criando estrutura com ID PK...")
            staging_table = Table(STAGING_TABLE_NAME, metadata, *table_columns)
            staging_table.create(conn)
            
            print(f"📤 Enviando dados...")
            df.to_sql(STAGING_TABLE_NAME, conn, if_exists='append', index=False)
            
            auditoria_estoque(df)

        print(f"✅ ETL ESTOQUE FINALIZADO! Tabela: {STAGING_TABLE_NAME}")

    except Exception as e:
        print(f"❌ ERRO NO PIPELINE: {e}")

if __name__ == "__main__":
    run_etl_estoque()