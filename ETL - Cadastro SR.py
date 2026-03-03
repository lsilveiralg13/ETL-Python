import pandas as pd
from sqlalchemy import create_engine, text
import re

def limpar_moedas(valor):
    if pd.isna(valor): return 0.0
    if isinstance(valor, (int, float)): return float(valor)
    s = str(valor).strip()
    s = re.sub(r'[^\d,.-]+', '', s) 
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try: return float(s)
    except: return 0.0

def formatar_moeda_br(valor):
    """Converte o float para string no formato R$ 1.234,56."""
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def run_etl_cadastro_showroom(excel_file_path="CONVIDADOS SHOWROOM ETL.xlsx"):
    engine = create_engine("mysql+pymysql://root:root@localhost:3306/faturamento_multimarcas_dw")
    
    try:
        df_verao_raw = pd.read_excel(excel_file_path, sheet_name="BASE VERÃO 2026")
        df_inverno_raw = pd.read_excel(excel_file_path, sheet_name="BASE INVERNO 2026")
    except Exception as e:
        print(f"Erro ao ler abas: {e}"); return

    # Padronização e Renomeação
    df_verao = df_verao_raw.copy().rename(columns={
        'CODIGO PAR.': 'codigo_parceiro', 'NOME DO PARCEIRO': 'nome_parceiro',
        'DATA': 'data_verao', 'MODALIDADE': 'modalidade_verao'
    })
    df_verao['valor_verao_raw'] = df_verao['VENDIDO VERÃO 2026'].apply(limpar_moedas)

    df_inverno = df_inverno_raw.copy().rename(columns={
        'CODIGO PAR.': 'codigo_parceiro', 'NOME DO PARCEIRO': 'nome_parceiro',
        'DATA': 'data_inverno', 'MODALIDADE': 'modalidade_inverno'
    })
    df_inverno['valor_inverno_raw'] = df_inverno['VENDIDO INVERNO 2026'].apply(limpar_moedas)

    # Consolidação (Merge)
    df_consolidado = pd.merge(
        df_verao[['codigo_parceiro', 'nome_parceiro', 'data_verao', 'modalidade_verao', 'valor_verao_raw']], 
        df_inverno[['codigo_parceiro', 'nome_parceiro', 'data_inverno', 'modalidade_inverno', 'valor_inverno_raw']], 
        on='codigo_parceiro', how='outer', suffixes=('_v', '_i')
    )

    # Tratamento de Nomes e Datas
    df_consolidado['nome_parceiro'] = df_consolidado['nome_parceiro_v'].fillna(df_consolidado['nome_parceiro_i']).fillna('NÃO IDENTIFICADO')
    
    # Formatação de Datas para string dd-mm-yyyy
    for col in ['data_verao', 'data_inverno']:
        df_consolidado[col] = pd.to_datetime(df_consolidado[col], errors='coerce').dt.strftime('%d-%m-%Y').fillna('---')

    # Formatação Monetária 'de_DE'
    df_consolidado['valor_verao'] = df_consolidado['valor_verao_raw'].apply(formatar_moeda_br)
    df_consolidado['valor_inverno'] = df_inverno['valor_inverno_raw'].apply(formatar_moeda_br)

    # Limpeza de colunas temporárias e preenchimento de Modalidade
    df_consolidado['modalidade_verao'] = df_consolidado['modalidade_verao'].fillna('---')
    df_consolidado['modalidade_inverno'] = df_consolidado['modalidade_inverno'].fillna('---')
    df_consolidado = df_consolidado[['codigo_parceiro', 'nome_parceiro', 'data_verao', 'modalidade_verao', 'valor_verao', 'data_inverno', 'modalidade_inverno', 'valor_inverno']]
    
    df_consolidado['data_carga_dw'] = pd.Timestamp.now()

    # Carga
    try:
        df_consolidado.to_sql('staging_cadastro_showroom', con=engine, if_exists='replace', index=False)
        print(f"Sucesso: {len(df_consolidado)} clientes consolidados e formatados enviados ao MySQL.")
    except Exception as e:
        print(f"Erro na carga: {e}")

if __name__ == "__main__":
    run_etl_cadastro_showroom()