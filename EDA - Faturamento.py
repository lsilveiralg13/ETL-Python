import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, BigInteger, String, Numeric
from sqlalchemy.schema import Table, Column, MetaData, CreateTable
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- SEÇÃO 1: CONFIGURAÇÕES ---
EXCEL_FILE = "FATURAMENTO ETL.xlsx"
EXCEL_SHEET_NAME = "FAT - TOTAL"
DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = 'root', 'root', 'localhost', 3306, 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_faturamento_multimarcas'

COLUMN_MAPPING_AND_TYPES = {
    'Nro. Nota': {'new_name': 'numero_nota', 'type': Integer},
    'Dt. Neg.': {'new_name': 'data_negociacao', 'type': DateTime},
    'Vlr. Nota': {'new_name': 'valor_faturado', 'type': Numeric(15, 2)},
    'Parceiro': {'new_name': 'codigo_parceiro', 'type': BigInteger},
    'UF': {'new_name': 'estado_cliente', 'type': String(2)},
    'REGIÃO': {'new_name': 'regiao', 'type': String(50)},
    'Apelido (Vendedor)': {'new_name': 'vendedor', 'type': String(100)},
    'Status NF-e': {'new_name': 'status_nfe', 'type': String(50)},
    'Qtd Itens': {'new_name': 'qtd_itens', 'type': Integer}
}

ESTADOS_BR = pd.DataFrame([
    ('Centro-Oeste', 'DF'), ('Centro-Oeste', 'GO'), ('Centro-Oeste', 'MS'), ('Centro-Oeste', 'MT'),
    ('Nordeste', 'AL'), ('Nordeste', 'BA'), ('Nordeste', 'CE'), ('Nordeste', 'MA'), ('Nordeste', 'PB'),
    ('Nordeste', 'PE'), ('Nordeste', 'PI'), ('Nordeste', 'RN'), ('Nordeste', 'SE'),
    ('Norte', 'AC'), ('Norte', 'AM'), ('Norte', 'AP'), ('Norte', 'PA'), ('Norte', 'RO'), ('Norte', 'RR'), ('Norte', 'TO'),
    ('Sudeste', 'ES'), ('Sudeste', 'MG'), ('Sudeste', 'RJ'), ('Sudeste', 'SP'),
    ('Sul', 'PR'), ('Sul', 'RS'), ('Sul', 'SC')
], columns=['regiao', 'estado_cliente'])

def p_mes_nome(n):
    meses = {1:'JANEIRO', 2:'FEVEREIRO', 3:'MARÇO', 4:'ABRIL', 5:'MAIO', 6:'JUNHO',
             7:'JULHO', 8:'AGOSTO', 9:'SETEMBRO', 10:'OUTUBRO', 11:'NOVEMBRO', 12:'DEZEMBRO'}
    return meses.get(int(n))

# --- SEÇÃO 2: FUNÇÕES AUXILIARES ---
def limpar_moedas(valor):
    if pd.isna(valor) or isinstance(valor, (int, float)): return valor
    s = str(valor).replace('R$', '').strip()
    if ',' in s: s = s.replace('.', '').replace(',', '.')
    try: return float(s)
    except: return None

# --- SEÇÃO 3: PROCESSAMENTO E ANÁLISE ---
def run_full_process():
    # 1. EXTRAÇÃO E ETL (Base do seu código anterior)
    df_raw = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
    df = df_raw.copy()
    
    # Transformações
    cols_to_use = {k: v['new_name'] for k, v in COLUMN_MAPPING_AND_TYPES.items() if k in df.columns}
    df = df[list(cols_to_use.keys())].rename(columns=cols_to_use)
    df['valor_faturado'] = df['valor_faturado'].apply(limpar_moedas)
    df['data_negociacao'] = pd.to_datetime(df['data_negociacao'], errors='coerce')
    df.dropna(subset=['data_negociacao'], inplace=True)

    # Carga MySQL (Simplificada para o exemplo, mantendo sua lógica de engine)
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    df.to_sql(STAGING_TABLE_NAME, engine, if_exists='replace', index=False)

    # --- INÍCIO DA ANÁLISE EXPLORATÓRIA ---
    print("\n--- Configuração da Análise ---")
    mes_base = int(input("Mês base (1-12): "))
    ano_base = int(input("Ano base: "))

    # Datas para o SFA (3 meses)
    data_fim = datetime(ano_base, mes_base, 1) + relativedelta(months=1) - relativedelta(days=1)
    data_ini = datetime(ano_base, mes_base, 1) - relativedelta(months=2)

    # Query única para performance
    query = f"""
        SELECT *, MONTH(data_negociacao) as mes_num, YEAR(data_negociacao) as ano_num 
        FROM {STAGING_TABLE_NAME} 
        WHERE (data_negociacao BETWEEN '{data_ini.strftime('%Y-%m-%d')}' AND '{data_fim.strftime('%Y-%m-%d')}')
           OR (MONTH(data_negociacao) = {mes_base} AND YEAR(data_negociacao) = {ano_base - 1})
        AND status_nfe = 'Aprovada'
    """
    df_analise = pd.read_sql(query, engine)

    # Gerar Arquivo Excel
    file_output = f"Analise_Exploratoria_{p_mes_nome(mes_base)}_{ano_base}.xlsx"
    with pd.ExcelWriter(file_output, engine='xlsxwriter') as writer:
        workbook = writer.book
        fmt_money = workbook.add_format({'num_format': 'R$ #,##0.00', 'border': 1})
        fmt_perc = workbook.add_format({'num_format': '0%', 'border': 1})
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#D9D9D9', 'border': 1})

        # --- ANÁLISE 1: SFA (Blocos Verticais + Comparativo Imagem 4) ---
        ws1 = workbook.add_worksheet('Análise 1 - SFA')
        row_idx = 1
        for m_offset in range(-2, 1):
            curr_date = datetime(ano_base, mes_base, 1) + relativedelta(months=m_offset)
            m_num, a_num = curr_date.month, curr_date.year
            
            df_m = df_analise[(df_analise['mes_num'] == m_num) & (df_analise['ano_num'] == a_num)]
            res = df_m.groupby('vendedor').agg(Faturamento=('valor_faturado', 'sum'), Clientes_Unicos=('codigo_parceiro', 'nunique')).reset_index()
            res['Ticket_Medio'] = (res['Faturamento'] / res['Clientes_Unicos']).fillna(0)
            
            ws1.write(row_idx-1, 0, p_mes_nome(m_num), fmt_header)
            res.to_excel(writer, sheet_name='Análise 1 - SFA', startrow=row_idx, index=False)
            row_idx += len(res) + 3

        # --- ANÁLISE 2: DIÁRIO (Imagem 2) ---
        df_dia = df_analise[df_analise['mes_num'] == mes_base]
        comp_dia = df_dia.pivot_table(index=df_dia['data_negociacao'].dt.day, columns='ano_num', values='valor_faturado', aggfunc='sum').fillna(0)
        if (ano_base-1) in comp_dia.columns:
            comp_dia['CRES. %'] = (comp_dia[ano_base] / comp_dia[ano_base-1]) - 1
        comp_dia.to_excel(writer, sheet_name='Análise 2 - Diário')

        # --- ANÁLISE 3: GEO (Imagem 3 e 5) ---
        df_geo_atual = df_analise[(df_analise['mes_num'] == mes_base) & (df_analise['ano_num'] == ano_base)]
        geo_res = df_geo_atual.groupby(['regiao', 'estado_cliente']).agg(Faturamento=('valor_faturado', 'sum'), Clientes_Unicos=('codigo_parceiro', 'nunique')).reset_index()
        geo_final = pd.merge(ESTADOS_BR, geo_res, on=['regiao', 'estado_cliente'], how='left').fillna(0)
        geo_final.to_excel(writer, sheet_name='Análise 3 - Geo', index=False)

        # Aplicar Formatação nas colunas R$
        for sheet in writer.sheets.values():
            sheet.set_column('B:D', 15, fmt_money) # Exemplo para colunas de valor

    print(f"\nProcesso concluído! Arquivo: {file_output}")

if __name__ == "__main__":
    run_full_process()