import pandas as pd
from sqlalchemy import create_engine
import warnings
import pymysql

warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

# --- CONFIGURAÇÃO (Mantendo suas credenciais) ---
CAMINHO_DO_ARQUIVO_BRUTO = r'C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\Scripts Python\FATURAMENTO ALGORÍTIMO.xlsx' 
MYSQL_CONFIG = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'port': 3306,
    'database': 'dvwarehouse'
}
TABELA_DESTINO_FATURAMENTO = 'fato_faturamento_limpo'
COLUNAS_CRITICAS = {
    'Dt. Neg.': 'data_transacao',
    'Vlr. Nota': 'valor_faturado',
    'Apelido (Vendedor)': 'vendedor_apelido',
    'Parceiro': 'parceiro_id'
}

# --- FUNÇÃO DE LIMPEZA (Sem mudanças) ---
def ler_e_limpar_faturamento(caminho_arquivo):
    try:
        df = pd.read_excel(caminho_arquivo)
    except Exception as e:
        print(f"\n[ERRO FATAL] Erro ao ler a planilha: {e}"); return None
    
    df = df.rename(columns=COLUNAS_CRITICAS)
    colunas_padronizadas = list(COLUNAS_CRITICAS.values())
    df_limpo = df[colunas_padronizadas].copy() 
    
    print(f"[STATUS] Total de linhas antes da limpeza: {len(df_limpo)}")

    df_limpo['data_transacao'] = pd.to_datetime(df_limpo['data_transacao'], errors='coerce')
    df_limpo['valor_faturado'] = pd.to_numeric(df_limpo['valor_faturado'], errors='coerce')
    
    df_limpo.dropna(subset=['data_transacao', 'valor_faturado'], inplace=True)
    df_limpo = df_limpo[df_limpo['valor_faturado'] > 0]
    
    print(f"[STATUS] Total de linhas APÓS a limpeza: {len(df_limpo)}")
    return df_limpo

# --- FUNÇÃO DE INGESTÃO NO MYSQL (CORREÇÃO DE COMPATIBILIDADE) ---

def inserir_no_mysql(df_limpo, config, tabela_destino):
    """
    Usa a conexão do Engine para evitar o erro 'cursor'.
    """
    db_url = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    
    try:
        engine = create_engine(db_url)
        
        # --- SOLUÇÃO DE COMPATIBILIDADE: USAR 'connection' DIRETO ---
        # A nova versão do Pandas (que causa o erro) exige isso.
        with engine.begin() as connection:
            df_limpo.to_sql(tabela_destino, con=connection, if_exists='replace', index=False)
        
        print(f"\n[SUCESSO] Dados inseridos na tabela '{tabela_destino}' no banco '{config['database']}'.")
        
    except Exception as e:
        print(f"\n[ERRO MYSQL] Falha ao conectar ou inserir dados: {e}")
        print("Atenção: A falha é na compatibilidade da biblioteca. Tente executar os comandos 'pip install --upgrade...' para estabilizar o ambiente.")


# --- FLUXO PRINCIPAL DE EXECUÇÃO ---

if __name__ == "__main__":
    
    df_faturamento_limpo = ler_e_limpar_faturamento(CAMINHO_DO_ARQUIVO_BRUTO)
    
    if df_faturamento_limpo is not None and not df_faturamento_limpo.empty:
        inserir_no_mysql(df_faturamento_limpo, MYSQL_CONFIG, TABELA_DESTINO_FATURAMENTO)
        print("\nPronto para a Fase 2: Geração de Features para o Modelo Preditivo.")