import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, DateTime, String, Numeric, Text
from sqlalchemy.schema import Table, Column, MetaData

# --- Seção 1: Configurações ---
EXCEL_FILE = "TELECONTROL ETL.xlsx"
EXCEL_SHEET_NAME = "BASE"

# --- Seção 2: Configurações de Banco ---
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'belmicro'
STAGING_TABLE_NAME = 'staging_telecontrol'

# --- Seção 3: Mapeamento Completo ---
COLUMN_MAPPING_AND_TYPES = {
    'Numero_OS': {'new_name': 'numero_os', 'type': String(50)},
    'Status': {'new_name': 'status', 'type': String(50)},
    'Tipo Atendimento': {'new_name': 'tipo_atendimento', 'type': String(100)},
    'Data Abertura': {'new_name': 'data_abertura', 'type': DateTime},
    'Data Finalização': {'new_name': 'data_finalizacao', 'type': DateTime},
    'Data Conserto': {'new_name': 'data_conserto', 'type': DateTime},
    'Defeito Reclamado': {'new_name': 'defeito_reclamado', 'type': Text},
    'Defeito Constatado': {'new_name': 'defeito_constatado', 'type': Text},
    'AGRUPAMENTO': {'new_name': 'agrupamento', 'type': String(100)},
    'FORNECEDOR': {'new_name': 'fornecedor', 'type': String(100)},
    'Linha de Produtos': {'new_name': 'linha_produto', 'type': String(100)},
    'Tipo de Produto': {'new_name': 'tipo_produto', 'type': String(100)},
    'Código Produto': {'new_name': 'codigo_produto', 'type': String(50)},
    'Descrição Produto': {'new_name': 'descricao_produto', 'type': String(255)},
    'Cidade': {'new_name': 'cidade', 'type': String(100)},
    'UF': {'new_name': 'uf', 'type': String(2)},
    'Revenda': {'new_name': 'revenda', 'type': String(255)},
    'Número NF': {'new_name': 'numero_nf', 'type': String(50)},
    'Data Compra': {'new_name': 'data_compra', 'type': DateTime},
    'CODIGO PEÇA': {'new_name': 'codigo_peca', 'type': String(100)},
    'QTDE': {'new_name': 'quantidade', 'type': Integer},
    'LOCAL ASSISTENCIA': {'new_name': 'local_assistencia', 'type': String(255)},
    'CHAVE_MMM': {'new_name': 'chave_mes', 'type': String(10)},
    'CHAVE_AAA': {'new_name': 'chave_ano', 'type': Integer},
    'DIAS_FINALIZAÇÃO': {'new_name': 'dias_finalizacao', 'type': Integer}
}

def auditoria_e_insights(df):
    print("\n" + "🔍" + " —" * 25)
    print("RESUMO DE AUDITORIA E INSIGHTS PARA VALIDAÇÃO")
    print("— " * 26)

    print(f"\n📌 CHECK-UP DE QUALIDADE:")
    print(f"- Registros Extraídos: {len(df)}")
    print(f"- OS sem Fornecedor (NULL): {df['fornecedor'].isna().sum()}")
    
    if 'dias_finalizacao' in df.columns:
        sla_negativo = len(df[df['dias_finalizacao'] < 0])
        print(f"- OS com SLA Negativo: {sla_negativo} {'⚠️ (Verificar datas no Excel)' if sla_negativo > 0 else '✅'}")

    print(f"\n📈 INSIGHTS E SUGESTÕES DE VIEW:")
    
    if 'status' in df.columns:
        status_counts = df['status'].value_counts()
        if not status_counts.empty:
            print(f"- Status Predominante: {status_counts.idxmax()} ({status_counts.max()} registros)")

    if 'linha_produto' in df.columns:
        if not df['linha_produto'].dropna().empty:
            top_linha = df['linha_produto'].value_counts().idxmax()
            print(f"- Linha com Maior Volume: {top_linha}")

    print("\n" + "— " * 30 + "🚀")

def run_etl():
    try:
        print(f"--- Iniciando ETL Telecontrol: {DB_NAME} ---")

        # 1. Extração
        if not os.path.exists(EXCEL_FILE):
            print(f"❌ Arquivo não encontrado: {EXCEL_FILE}")
            return
            
        df = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
        print(f"✅ Extração concluída: {len(df)} linhas encontradas.")

        # 2. Transformação
        existing_cols = {k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items() if k in df.columns}
        df = df[list(existing_cols.keys())].rename(columns=existing_cols)

        date_cols = ['data_abertura', 'data_finalizacao', 'data_conserto', 'data_compra']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if 'data_finalizacao' in df.columns and 'data_abertura' in df.columns:
            df['dias_finalizacao'] = (df['data_finalizacao'] - df['data_abertura']).dt.days
            df['dias_finalizacao'] = df['dias_finalizacao'].fillna(0).astype(int)

        df['data_carga_dw'] = pd.Timestamp.now()

        # 3. Conexão e Carga
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
        
        with engine.begin() as conn:
            print(f"🗑️ Removendo tabela antiga: {STAGING_TABLE_NAME}")
            conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE_NAME}"))
            
            print(f"🔨 Criando nova estrutura com ID PK...")
            staging_table = Table(STAGING_TABLE_NAME, metadata, *table_columns)
            staging_table.create(conn)
            
            print(f"📤 Carregando dados...")
            # index=False garante que o ID seja gerado pelo MySQL, não pelo Pandas
            df.to_sql(STAGING_TABLE_NAME, conn, if_exists='append', index=False)
            
            auditoria_e_insights(df)

        print(f"✅ ETL FINALIZADO COM SUCESSO! Tabela: {STAGING_TABLE_NAME}")

    except Exception as e:
        print(f"❌ ERRO FATAL NO PIPELINE: {e}")

if __name__ == "__main__":
    run_etl()