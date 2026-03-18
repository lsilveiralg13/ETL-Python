import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, DateTime, String, Numeric, Text
from sqlalchemy.schema import Table, Column, MetaData

# --- Seção 1: Configurações ---
EXCEL_FILE = "TELECONTROL ETL.xlsx"
EXCEL_SHEET_NAME = "BASE"

DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'belmicro'
STAGING_TABLE_NAME = 'staging_telecontrol'

# Mapeamento Completo
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
    """
    Realiza uma varredura nos dados carregados para apoiar a validação do Analista.
    """
    print("\n" + "🔍" + " —" * 25)
    print("RESUMO DE AUDITORIA E INSIGHTS PARA VALIDAÇÃO")
    print("— " * 26)

    # 1. Qualidade dos Dados
    print(f"\n📌 CHECK-UP DE QUALIDADE:")
    print(f"- Registros Extraídos: {len(df)}")
    print(f"- OS sem Fornecedor (NULL): {df['fornecedor'].isna().sum()}")
    
    if 'dias_finalizacao' in df.columns:
        sla_negativo = len(df[df['dias_finalizacao'] < 0])
        print(f"- OS com SLA Negativo: {sla_negativo} {'⚠️ (Verificar datas no Excel)' if sla_negativo > 0 else '✅'}")

    # 2. Sugestões de KPIs para suas Views
    print(f"\n📈 INSIGHTS E SUGESTÕES DE VIEW:")
    
    if 'status' in df.columns:
        status_counts = df['status'].value_counts()
        print(f"- Status Predominante: {status_counts.idxmax()} ({status_counts.max()} registros)")

    if 'fornecedor' in df.columns and 'dias_finalizacao' in df.columns:
        media_geral = df[df['status'].str.contains('Finaliza', case=False, na=False)]['dias_finalizacao'].mean()
        print(f"- SLA Médio Geral (Finalizadas): {media_geral:.1f} dias")
        print(f"💡 DICA: Crie sua View de SLA agrupando por 'fornecedor' e filtrando 'fornecedor IS NOT NULL'.")

    if 'linha_produto' in df.columns:
        top_linha = df['linha_produto'].value_counts().idxmax()
        print(f"- Linha com Maior Volume: {top_linha}")

    print("\n" + "— " * 30 + "🚀")

def run_etl():
    try:
        print(f"--- Iniciando ETL: Schema {DB_NAME} ---")

        # 1. Extração
        df = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
        print(f"✅ Extração concluída: {len(df)} linhas encontradas.")

        # 2. Transformação
        existing_cols = {k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items() if k in df.columns}
        df = df[list(existing_cols.keys())].rename(columns=existing_cols)

        # 2.2 Conversão de Datas
        date_cols = ['data_abertura', 'data_finalizacao', 'data_conserto', 'data_compra']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # 2.3 Cálculo de Dias
        if 'data_finalizacao' in df.columns and 'data_abertura' in df.columns:
            print("⚙️ Calculando DIAS_FINALIZAÇÃO...")
            df['dias_finalizacao'] = (df['data_finalizacao'] - df['data_abertura']).dt.days
            df['dias_finalizacao'] = df['dias_finalizacao'].fillna(0).astype(int)

        # 2.4 Coluna de Auditoria
        df['data_carga_dw'] = pd.Timestamp.now()

        # 3. Carga (MySQL)
        mysql_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(mysql_url)
        
        metadata = MetaData()
        table_columns = [Column(v['new_name'], v['type']) for v in COLUMN_MAPPING_AND_TYPES.values()]
        table_columns.append(Column('data_carga_dw', DateTime))
        
        table_obj = Table(STAGING_TABLE_NAME, metadata, *table_columns)
        
        with engine.begin() as conn:
            print(f"🗑️ Removendo tabela antiga: {STAGING_TABLE_NAME}")
            conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE_NAME}"))
            
            print(f"🔨 Criando nova estrutura de tabela...")
            metadata.create_all(conn)
            
            print(f"📤 Carregando dados para o MySQL...")
            df.to_sql(STAGING_TABLE_NAME, conn, if_exists='append', index=False)
            
            # --- Executa o Complemento de Auditoria ---
            auditoria_e_insights(df)

        print(f"✅ ETL FINALIZADO COM SUCESSO! Tabela: {DB_NAME}.{STAGING_TABLE_NAME}")

    except Exception as e:
        print(f"❌ ERRO FATAL NO PIPELINE: {e}")

if __name__ == "__main__":
    run_etl()