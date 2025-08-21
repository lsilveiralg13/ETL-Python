import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, BigInteger, String, Numeric, Float, Text # Importa tipos SQLAlchemy para DDL
from sqlalchemy.schema import Table, Column, MetaData, CreateTable # Importações para DDL explícito
import os # Importar para usar variáveis de ambiente
import re # Importar para a função limpar_numero
import warnings
# locale não é estritamente necessário se não formatarmos nomes de meses PT-BR, mas mantido se houver planos futuros.
# import locale 

# Ignora UserWarnings do pandas que podem ocorrer com o to_sql
warnings.filterwarnings('ignore', category=UserWarning)

# --- Seção 1: Configurações ---
# Detalhes do arquivo Excel
EXCEL_FILE = "PRIMEIRO PEDIDO ETL.xlsx"
EXCEL_SHEET_NAME = 0 # Assume a primeira aba, ajuste se necessário

# Detalhes do Banco de Dados MySQL
DB_USER = os.getenv('DB_USER', 'root') # Usa variáveis de ambiente, com fallback para 'root'
DB_PASSWORD = os.getenv('DB_PASSWORD', 'root') 
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '3306')) # Converte a porta para int
DB_NAME = 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_primeiro_pedido' # Nome da tabela para este ETL

# Mapeamento de colunas do Excel para o MySQL e seus tipos de dados SQLAlchemy
# Isso será usado tanto para renomear colunas quanto para criar a tabela no MySQL
COLUMN_MAPPING_AND_TYPES = {
    "Data Faturamento": {"new_name": "data_faturamento", "type": DateTime},
    "Data de Cadastro": {"new_name": "data_cadastro", "type": DateTime},
    "Ano": {"new_name": "chave_ano", "type": Integer},
    "Mês": {"new_name": "chave_mes", "type": String(50)},
    "Nro Pedido": {"new_name": "numero_pedido", "type": String(100)}, # Pode ser alfanumérico
    "Cód. Parceiro": {"new_name": "codigo_parceiro", "type": BigInteger},
    "Nome Parceiro": {"new_name": "nome_parceiro", "type": String(255)},
    "Nome Vendedor": {"new_name": "vendedor", "type": String(100)},
    "Vlr Nota": {"new_name": "valor_faturado", "type": Numeric(15, 2)},
    "Primeira Compra": {"new_name": "primeira_compra", "type": Integer}, # 0 ou 1
    "Qtd Pedidos": {"new_name": "qtd_itens", "type": Integer},
    "UF": {"new_name": "uf", "type": String(2)},
    "Data Negociação": {"new_name": "data_negociacao", "type": DateTime},
    "Nome SDR": {"new_name": "nome_sdr", "type": String(100)},
    "PREMIO": {"new_name": "premio", "type": Numeric(15, 2)}, # Assumindo valor monetário
    # Coluna que será adicionada durante a transformação
    "data_carga_dw": {"new_name": "data_carga_dw", "type": DateTime} 
}

# --- Seção 2: Funções de Transformação e Limpeza ---

def clean_currency_value(value):
    """
    Limpa strings de moeda (R$, pontos, vírgulas) e as converte para float.
    Retorna None para valores inválidos ou NaN.
    """
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    s = s.replace('R$', '').replace(' ', '')
    
    # Lida com o separador decimal: substitui vírgula por ponto, se houver
    if ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'): # Ex: "1.234,56"
            s = s.replace('.', '')
            s = s.replace(',', '.')
    elif ',' in s: # Ex: "1234,56"
        s = s.replace(',', '.')
    
    try:
        return float(s)
    except ValueError:
        print(f"Aviso: Não foi possível converter o valor '{s[:50]}...' para float. Retornando None.")
        return None

def clean_first_purchase(value):
    """
    Limpa valores booleanos/de primeira compra ('Sim', 'Não', 'True', 'False', 1, 0)
    e os converte para 1 (Sim) ou 0 (Não). Retorna None para valores não reconhecidos.
    """
    if pd.isna(value) or value is None:
        return None
    s = str(value).strip().lower()
    if s in ['sim', 'true', '1', 'primeira']:
        return 1
    elif s in ['não', 'nao', 'false', '0']:
        return 0
    else:
        print(f"Aviso: Valor '{value}' não reconhecido para 'Primeira Compra'. Retornando None.")
        return None

# --- Seção 3: Pipeline ETL ---

def run_etl_primeiro_pedido(excel_file_path=EXCEL_FILE):
    """
    Executa o pipeline ETL completo para Primeiro Pedido: Extração, Transformação e Carga.
    """
    df = None 
    connection = None
    cursor = None
    
    try:
        print(f"--- Iniciando ETL de Primeiro Pedido ---")

        # 1. Extração
        print(f"\n1. Extraindo dados do arquivo '{excel_file_path}', aba '{EXCEL_SHEET_NAME}'...")
        try:
            # usecols com as chaves originais para otimizar a leitura
            # Ignora chaves que não existem no arquivo para evitar erros no usecols
            cols_to_read = [col for col in COLUMN_MAPPING_AND_TYPES.keys() if col in pd.read_excel(excel_file_path, sheet_name=EXCEL_SHEET_NAME, nrows=0).columns]
            df_raw = pd.read_excel(excel_file_path, sheet_name=EXCEL_SHEET_NAME, usecols=cols_to_read)
            print(f"Dados extraídos do Excel com sucesso! {df_raw.shape[0]} linhas e {df_raw.shape[1]} colunas.")

            print("\n--- Diagnóstico de Colunas do Excel Original ---")
            print("Colunas encontradas no arquivo Excel:")
            for col in df_raw.columns:
                print(f"- '{col}'")
            print("----------------------------------------------\n")

        except FileNotFoundError:
            print(f"Erro: Arquivo '{excel_file_path}' não encontrado. Por favor, verifique o caminho e o nome do arquivo.")
            return 
        except Exception as e:
            print(f"Erro ao ler o arquivo Excel: {e}")
            return 

        print("\n2. Iniciando transformações para Primeiro Pedido...")

        df = df_raw.copy()

        # 2.1 Seleção e renomeação de colunas
        # Prepara um dicionário com apenas as colunas que realmente existem no df_raw e mapeia para os novos nomes
        columns_to_select_and_rename = {
            excel_col: config['new_name'] 
            for excel_col, config in COLUMN_MAPPING_AND_TYPES.items() 
            if excel_col in df.columns
        }
        
        # Verifica e notifica sobre colunas esperadas que não foram encontradas no Excel
        missing_excel_cols = [col for col in COLUMN_MAPPING_AND_TYPES.keys() if col not in df.columns and col not in ['data_carga_dw']] # Ignora cols derivadas
        if missing_excel_cols:
            print(f"Atenção: As seguintes colunas esperadas NÃO foram encontradas no Excel e serão ignoradas: {missing_excel_cols}")
        
        # Realiza a seleção e renomeação. Garante que só as colunas válidas no mapeamento sejam consideradas.
        existing_cols_in_mapping = [col for col in columns_to_select_and_rename.keys() if col in df.columns]
        df = df[existing_cols_in_mapping].rename(columns=columns_to_select_and_rename)

        print("\n--- Diagnóstico de Colunas Após Seleção e Renomeação ---")
        print("Colunas no DataFrame após seleção e renomeação:")
        for col in df.columns:
            print(f"- '{col}'")
        print("------------------------------------------------------\n")

        # 2.2 Limpeza e Conversão de Colunas Numéricas (monetárias)
        currency_columns = ["valor_faturado", "premio"]
        for col in currency_columns:
            if col in df.columns:
                df[col] = df[col].apply(clean_currency_value)
                df[col] = df[col].fillna(0.0) # Preenche NaN com 0.0 para valores monetários
                print(f"Coluna '{col}' limpa e convertida para float (monetário).")
            else:
                print(f"Aviso: Coluna monetária '{col}' não encontrada no DataFrame para limpeza.")

        # 2.3 Limpeza e Conversão de Colunas Numéricas (inteiros)
        int_columns = ["chave_ano", "qtd_itens"]
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                df[col] = df[col].fillna(0).astype('Int64') # Preenche NaN e converte para Int64
                print(f"Coluna '{col}' convertida para tipo inteiro.")
            else:
                print(f"Aviso: Coluna de inteiro '{col}' não encontrada no DataFrame para limpeza.")
        
        # 2.4 Limpeza e Conversão de Colunas Booleanas (Primeira Compra)
        if 'primeira_compra' in df.columns:
            df['primeira_compra'] = df['primeira_compra'].apply(clean_first_purchase).astype('Int64')
            print("Coluna 'primeira_compra' convertida para 0/1.")
        else:
            print("Aviso: Coluna 'primeira_compra' não encontrada no DataFrame.")

        # 2.5 Tratamento de CNPJ (garantir string, remover não-dígitos, preencher vazios com None)
        if 'cnpj_cliente' in df.columns: # Coluna 'CNPJ' mapeada para 'cnpj_cliente'
            df['cnpj_cliente'] = df['cnpj_cliente'].astype(str).str.replace(r'[^\d]', '', regex=True)
            df['cnpj_cliente'] = df['cnpj_cliente'].replace('', None) # Substitui strings vazias por None
            print("Coluna 'cnpj_cliente' tratada e formatada.")
        else:
            print("Aviso: Coluna 'cnpj_cliente' não encontrada no DataFrame para tratamento.")

        # 2.6 Conversão de datas
        date_columns = ["data_faturamento", "data_cadastro", "data_negociacao"]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].dt.date # Converte para a parte da data (Python date object)
                print(f"Coluna '{col}' convertida para tipo datetime.")
            else:
                print(f"Aviso: Coluna de data '{col}' não encontrada no DataFrame.")

        # 2.7 Tratamento de colunas de texto com valores NaN (para evitar 'nan' string)
        # Percorre as colunas que são mapeadas para String/Text
        for col_excel, col_config in COLUMN_MAPPING_AND_TYPES.items():
            new_name = col_config['new_name']
            sql_type = col_config['type']
            if isinstance(sql_type, (String, Text)) and new_name in df.columns:
                if df[new_name].isnull().any() or df[new_name].dtype == 'object': 
                    df[new_name] = df[new_name].fillna('') # Preenche NaN/None com string vazia
                    df[new_name] = df[new_name].astype(str) # Garante que é tipo string
                    print(f"Coluna de texto '{new_name}' tratada para nulos e convertida para string.")

        # 2.8 Regra: Excluir linhas onde 'nome_sdr' é vazio/nulo
        if 'nome_sdr' in df.columns:
            initial_rows_before_sdr_filter = df.shape[0]
            df = df[df['nome_sdr'].notna() & (df['nome_sdr'].astype(str).str.strip() != '')]
            rows_excluded_sdr = initial_rows_before_sdr_filter - df.shape[0]
            print(f"\n{rows_excluded_sdr} linhas excluídas onde 'nome_sdr' é vazio ou nulo.")
        else:
            print("Aviso: Coluna 'nome_sdr' não encontrada para aplicar regra de exclusão de valores vazios.")
        
        # 2.9 Regra: Excluir linhas onde 'vendedor' é igual a 'nome_sdr'
        if 'vendedor' in df.columns and 'nome_sdr' in df.columns:
            initial_rows_before_sdr_vendedor_filter = df.shape[0]
            df = df[df['vendedor'].astype(str).str.strip().str.lower() != df['nome_sdr'].astype(str).str.strip().str.lower()]
            rows_excluded_sdr_vendedor = initial_rows_before_sdr_vendedor_filter - df.shape[0]
            print(f"{rows_excluded_sdr_vendedor} linhas excluídas onde 'vendedor' é igual a 'nome_sdr'.")
        else:
            print("Aviso: Colunas 'vendedor' ou 'nome_sdr' não encontradas para aplicar regra de exclusão Vendedor == SDR.")
            
        # 2.10 Remover registros sem chaves essenciais
        # Chaves essenciais para este ETL devem ser verificadas no mapeamento COLUMNS_MAP
        chaves_essenciais = ['numero_pedido', 'data_faturamento', 'codigo_parceiro'] 
        chaves_existentes_para_dropna = [col for col in chaves_essenciais if col in df.columns]

        if chaves_existentes_para_dropna:
            initial_rows_dropna = df.shape[0]
            df.dropna(subset=chaves_existentes_para_dropna, inplace=True)
            if df.shape[0] < initial_rows_dropna:
                print(f"Removidas {initial_rows_dropna - df.shape[0]} linhas com chaves essenciais nulas. Total de {df.shape[0]} linhas restantes.")
            else:
                print("Nenhuma linha removida por chaves essenciais nulas.")
        else:
            print("Aviso: Nenhuma chave essencial encontrada no DataFrame para remoção de nulos.")


        # 2.11 Adicionar timestamp de carga
        df['data_carga_dw'] = pd.Timestamp.now()
        print(f"Coluna 'data_carga_dw' adicionada com o timestamp atual: {df['data_carga_dw'].iloc[0]}")

        print("\nTransformações Concluídas para Primeiro Pedido.")
        print(f"DataFrame FINAL para carga tem {df.shape[0]} linhas e {df.shape[1]} colunas.")
        print("\nPrimeiras linhas do DataFrame FINAL:")
        print(df.head())
        print("\nInformações do DataFrame FINAL (df.info()):")
        df.info()

        # 3. Carga para MySQL
        print(f"\n3. Iniciando carga para a tabela de staging '{STAGING_TABLE_NAME}' no MySQL...")
        
        if df.empty:
            print("DataFrame está vazio após as transformações. Nenhuma linha para carregar no MySQL.")
            return

        mysql_connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        print(f"Tentando criar engine para o banco de dados: mysql+pymysql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        engine = create_engine(mysql_connection_string)
        print("Engine criada com sucesso. Tentando obter conexão bruta...")

        connection = engine.raw_connection()
        cursor = connection.cursor() 
        print("Conexão bruta e cursor obtidos.")

        # Passo 3.1: DROP manual da tabela se existir
        print(f"Verificando e droppando a tabela '{STAGING_TABLE_NAME}' se existir...")
        drop_table_sql = f"DROP TABLE IF EXISTS `{STAGING_TABLE_NAME}`;"
        cursor.execute(drop_table_sql)
        connection.commit() 
        print(f"Tabela '{STAGING_TABLE_NAME}' removida (se existia).")

        # Passo 3.2: Geração e execução do CREATE TABLE DDL (para MySQL)
        print(f"Gerando e executando CREATE TABLE para '{STAGING_TABLE_NAME}'...")
        
        metadata = MetaData()
        
        table_columns = []
        for col_name in df.columns: # Itera pelas colunas do DF transformado
            # Encontra o tipo SQLAlchemy para a coluna atual do DataFrame usando o mapeamento
            col_type_obj = next((item['type'] for item in COLUMN_MAPPING_AND_TYPES.values() if item['new_name'] == col_name), None)
            
            if col_type_obj:
                sql_type = col_type_obj
            else: # Fallback para colunas que não estejam no mapeamento inicial (como data_carga_dw)
                if col_name == 'data_carga_dw':
                    sql_type = DateTime
                # Adicionar mais fallbacks se houver outras colunas geradas/não mapeadas
                else: 
                    print(f"Aviso: Tipo para a coluna '{col_name}' não encontrado no mapeamento para DDL. Usando String(255).")
                    sql_type = String(255) 

            table_columns.append(Column(col_name, sql_type, nullable=True)) # 'nullable=True' por padrão

        table_obj = Table(STAGING_TABLE_NAME, metadata,
                          *table_columns,
                          mysql_engine='InnoDB',
                          mysql_charset='utf8mb4'
                         )
        
        create_table_sql = str(CreateTable(table_obj).compile(engine))
        
        cursor.execute(create_table_sql)
        connection.commit() 
        print(f"Tabela '{STAGING_TABLE_NAME}' criada explicitamente no MySQL.")

        # Passo 3.3: Carregamento de dados - AGORA COM INSERÇÃO MANUAL (executemany)
        print("Iniciando carregamento de dados via inserção manual com cursor.executemany...")

        column_order_for_insert = df.columns.tolist() 
        
        data_to_insert = []
        for index, row in df.iterrows():
            processed_row = []
            for col_name in column_order_for_insert:
                val = row[col_name]
                # Trata tipos datetime, NaN e Pandas nullable integers para compatibilidade com MySQL/PyMySQL
                if pd.api.types.is_datetime64_any_dtype(df[col_name]) and pd.notna(val):
                    processed_row.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                elif pd.isna(val) or val is None: 
                    processed_row.append(None) 
                elif isinstance(val, pd.Int64Dtype.type): 
                    processed_row.append(None if pd.isna(val) else int(val)) 
                else:
                    processed_row.append(val)
            data_to_insert.append(tuple(processed_row))

        columns_sql = ", ".join([f"`{col}`" for col in column_order_for_insert]) 
        placeholders = ", ".join(["%s"] * len(column_order_for_insert)) 
        
        insert_sql = f"INSERT INTO `{STAGING_TABLE_NAME}` ({columns_sql}) VALUES ({placeholders})"

        BATCH_SIZE = 1000 

        try:
            for i in range(0, len(data_to_insert), BATCH_SIZE):
                batch = data_to_insert[i:i + BATCH_SIZE]
                if batch: 
                    cursor.executemany(insert_sql, batch)
                    connection.commit() 
                    print(f"Lote {i // BATCH_SIZE + 1} de {len(data_to_insert) // BATCH_SIZE + 1} carregado. ({len(batch)} linhas)")
            
            print(f"Dados carregados com sucesso na tabela de staging '{STAGING_TABLE_NAME}' via inserção manual!")
            print(f"--- ETL Concluído com Sucesso! ---")

        except Exception as insert_e:
            print(f"Erro ao inserir dados via cursor.executemany: {insert_e}")
            connection.rollback() 
            raise 

    except Exception as e:
        print(f"Erro fatal durante o ETL: {e}")
    finally:
        if cursor:
            cursor.close()
            print("Cursor do banco de dados fechado.")
        if connection:
            connection.close()
            print("Conexão com o banco de dados fechada.")

# --- Execução do ETL ---
if __name__ == "__main__":
    run_etl_primeiro_pedido()

    