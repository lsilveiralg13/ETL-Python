import pandas as pd
from sqlalchemy import create_engine
import re
import os

def limpar_moedas(valor):
    """
    Limpa e converte strings de moedas para float.
    Lida com separadores de milhar (ponto) e decimal (vírgula/ponto).
    """
    if pd.isna(valor):
        return None
    
    # Se o valor já for int ou float, retorna-o diretamente
    if isinstance(valor, (int, float)):
        return float(valor)

    s = str(valor).strip()
    # Remove tudo que não for dígito, vírgula, ponto ou hífen (para números negativos)
    s = re.sub(r'[^\d,.-]+', '', s) 
    
    # Lida com o formato europeu/brasileiro (ex: 1.234,56 -> 1234.56)
    if ',' in s and '.' in s:
        s = s.replace('.', '') # Remove separador de milhar
        s = s.replace(',', '.') # Troca vírgula por ponto decimal
    elif ',' in s: # Lida com formato 123,45 -> 123.45
        s = s.replace(',', '.')

    try:
        return float(s)
    except ValueError:
        print(f"Aviso: Não foi possível converter o valor '{str(valor)}' (limpo: '{s}') para float após a limpeza. Retornando None.")
        return None


def run_etl_cadastro_showroom(excel_file_path="CONVIDADOS SHOWROOM ETL.xlsx"):
    """
    Executa o processo ETL para os dados do cadastro de showroom.
    Extrai dados de um arquivo Excel, transforma-os e carrega-os em uma tabela MySQL.
    """
    print("Iniciando o processo ETL para CADASTRO SHOWROOM...")

    try:
        df_raw = pd.read_excel(excel_file_path, sheet_name=0)
        print(f"Dados extraídos do Excel com sucesso! {df_raw.shape[0]} linhas e {df_raw.shape[1]} colunas.")

        print("\nInformações do DataFrame original (df_raw):")
        df_raw.info()
        print("\nPrimeiras linhas do DataFrame original (df_raw.head()):")
        print(df_raw.head())

    except FileNotFoundError:
        print(f"Erro: Arquivo '{excel_file_path}' não encontrado. Por favor, verifique o caminho e o nome do arquivo.")
        return
    except Exception as e:
        print(f"Erro ao ler o arquivo Excel: {e}")
        return


    print("\nIniciando transformações...")

    df = df_raw.copy()

    # Mapeamento de nomes de colunas do Excel para os nomes EXATOS no SQL.
    # Isso garante que os nomes no DataFrame 'df' já serão os nomes finais para o MySQL.
    colunas_para_etl_internas = {
        "CODIGO PAR.": "codigo_parceiro",
        "NOME DO PARCEIRO": "nome_parceiro",
        "STATUS": "status_cliente",
        "VENDEDORA": "vendedor",
        "CIDADE": "cidade",
        "UF": "uf",
        "CONFIRMADO": "confirmado",
        "MODALIDADE": "modalidade",
        "DATA": "data", # Coluna 'DATA' no Excel e no SQL
        "MEIO DE TRANSPORTE": "meio_transporte",
        "CUSTO - AÉREO": "custo_aereo",
        "CUSTO - HOTEL": "custo_hotel",
        "CUSTO - RODOVIÁRIO": "custo_rodoviario",
        # ATENÇÃO: Mapeia do nome do Excel "VENDIDO VERÃO 2026" para o nome SQL "VENDIDO VERÃO 202"
        "VENDIDO VERÃO 2026": "vendido_VR26", 
    }

    colunas_excel_esperadas = list(colunas_para_etl_internas.keys())
    
    colunas_presentes = [col for col in colunas_excel_esperadas if col in df.columns]
    colunas_faltantes = [col for col in colunas_excel_esperadas if col not in df.columns]

    if colunas_faltantes:
        print(f"Atenção: As seguintes colunas esperadas não foram encontradas no Excel e serão ignoradas: {colunas_faltantes}")
        # Filtra o dicionário para incluir apenas as colunas presentes no DataFrame
        colunas_para_etl_internas = {k: v for k, v in colunas_para_etl_internas.items() if k in colunas_presentes}

    # Seleciona e renomeia as colunas do DataFrame para os nomes exatos do SQL
    df = df[list(colunas_para_etl_internas.keys())].rename(columns=colunas_para_etl_internas)


    # Colunas monetárias agora usam os nomes exatos do SQL
    colunas_monetarias = ['CUSTO - AÉREO', 'CUSTO - HOTEL', 'CUSTO - RODOVIÁRIO', 'VENDIDO VERÃO 202']
    for col in colunas_monetarias:
        if col in df.columns:
            df[col] = df[col].apply(limpar_moedas)
            print(f"Coluna '{col}' limpa e convertida para float.")
            print(f"  -> Nulos em '{col}' após limpeza (antes do fillna): {df[col].isnull().sum()}")
            df[col] = df[col].fillna(0.0) # Preenche valores nulos com 0.0
            print(f"  -> Nulos em '{col}' após fillna(0.0): {df[col].isnull().sum()}")


    # 'DATA' é o nome da coluna no DataFrame e no SQL
    if 'DATA' in df.columns:
        df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')
        print("Coluna 'DATA' convertida para formato de data.")
        linhas_antes_data = df.shape[0]
        df.dropna(subset=['DATA'], inplace=True) # Remove linhas onde a conversão de data falhou
        linhas_depois_data = df.shape[0]
        if linhas_antes_data > linhas_depois_data:
            print(f"Removidas {linhas_antes_data - linhas_depois_data} linhas com 'DATA' vazia ou inválida.")

    df['data_carga_dw'] = pd.Timestamp.now() # Adiciona a coluna com o timestamp da carga
    print("Coluna 'data_carga_dw' adicionada com o timestamp atual.")

    print("Transformações Concluídas com sucesso.")
    print(f"Dataframe transformado tem {df.shape[0]} linhas e {df.shape[1]} colunas.")
    print("\nPrimeiras linhas do DataFrame transformado (df.head()):")
    print(df.head())
    print("\nInformações do DataFrame transformado (df.info()):")
    df.info()

    print("\nIniciando carga para o MySQL...")

    # Configurações do banco de dados (usando variáveis de ambiente)
    db_user = os.getenv('DB_USER', 'root')
    db_password = os.getenv('DB_PASSWORD', 'root') 
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = 'faturamento_multimarcas_dw' 
    staging_table_name = 'staging_cadastro_showroom'

    try:
        engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        
        # AQUI NÃO É MAIS NECESSÁRIO RENOMEAR, POIS OS NOMES JÁ ESTÃO CORRETOS NO DF
        # df_final_load = df.rename(columns={...}) # ESTE BLOCO FOI REMOVIDO
        
        # Define a ordem das colunas para o carregamento no SQL, garantindo que correspondam à tabela de destino.
        # Os nomes aqui DEVEM corresponder exatamente aos nomes na tabela MySQL.
        colunas_finais_ordenadas_sql = [
            'codigo_parceiro', 'nome_parceiro', 'status_cliente', 'vendedor', 'cidade', 'uf',
            'confirmado', # Adicionado para garantir que esta coluna seja carregada
            'modalidade', 'data', 'meio_transporte',
            'custo_aereo', 'custo_hotel', 'custo_rodoviario',
            'vendido_VR26', # Nome da coluna no SQL, conforme seu DESCRIBE
            'data_carga_dw' 
        ]
        
        # Filtra e reordena o DataFrame para corresponder às colunas da tabela SQL
        colunas_para_carregar = [col for col in colunas_finais_ordenadas_sql if col in df.columns]
        df_final_load = df[colunas_para_carregar] # Usa 'df' diretamente
        
        print("\nColunas do DataFrame antes do carregamento SQL:", df_final_load.columns.tolist())
        print("Tipos de dados do DataFrame antes do carregamento SQL:")
        df_final_load.info()

        # Carrega os dados para o MySQL.
        # if_exists='replace' irá DROPAR a tabela e recriá-la a cada execução.
        # Isso é comum para tabelas de staging, mas esteja ciente da implicação de perda de dados anteriores.
        df_final_load.to_sql(staging_table_name, con=engine, if_exists='replace', index=False)
        print(f"Dados carregados com sucesso na tabela de staging '{staging_table_name}' (modo 'replace')!")
    except Exception as e:
        print(f"Erro ao carregar dados para o MySQL: {e}")

# Executa o processo ETL
run_etl_cadastro_showroom()