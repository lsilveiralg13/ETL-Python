import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, BigInteger, String, Numeric, Float, Text # Importa tipos SQLAlchemy para DDL
from sqlalchemy.schema import Table, Column, MetaData, CreateTable # Importações para DDL explícito
import os # Importar para usar variáveis de ambiente
import re # Importar para a função limpar_numero

# --- Seção 1: Configurações ---
# Detalhes do arquivo Excel
EXCEL_FILE = "CADASTRO CLIENTE ETL.xlsx"
EXCEL_SHEET_NAME = 0 # Assume a primeira aba, ajuste se necessário

# Detalhes do Banco de Dados MySQL
DB_USER = os.getenv('DB_USER', 'root') # Usa variáveis de ambiente, com fallback para 'root'
DB_PASSWORD = os.getenv('DB_PASSWORD', 'root') 
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '3306')) # Converte a porta para int
DB_NAME = 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_cadastro_clientes' # Nome da tabela para este ETL

# Mapeamento de colunas do Excel para o MySQL e seus tipos de dados SQLAlchemy
# Isso será usado tanto para renomear colunas quanto para criar a tabela no MySQL
COLUMN_MAPPING_AND_TYPES = {
    'Cód. Parceiro': {'new_name': 'codigo_parceiro', 'type': BigInteger},
    'Nome Parceiro': {'new_name': 'nome_parceiro', 'type': String(255)},
    'STATUS': {'new_name': 'status_cliente', 'type': String(50)},
    'DESCRIÇÃO STATUS': {'new_name': 'motivo_status', 'type': String(255)},
    'Apelido (Vendedor)': {'new_name': 'vendedor', 'type': String(100)},
    'Nome SDR': {'new_name': 'nome_sdr', 'type': String(100)},
    'Nome (Cidade)': {'new_name': 'cidade_cliente', 'type': String(100)},
    'MESORREGIÃO': {'new_name': 'mesorregiao', 'type': String(100)},
    'ESTADO': {'new_name': 'estado', 'type': String(2)}, # UF como String(2)
    'CNPJ': {'new_name': 'cnpj_cliente', 'type': String(20)}, # CNPJ/CPF como string para preservar zeros
    'POPULAÇÃO': {'new_name': 'populacao_cidade', 'type': Integer},
    'BLOCO POPULACIONAL': {'new_name': 'bloco_populacional', 'type': String(50)},
    'CLUSTERIZAÇÃO': {'new_name': 'clusterizacao', 'type': String(100)},
    'Data Cadastro': {'new_name': 'data_cadastro_cliente', 'type': DateTime},
    'CHAVE_MMM': {'new_name': 'chave_mmm', 'type': String(50)},
    'CHAVE_AAA': {'new_name': 'chave_aaa', 'type': Integer},
    # Coluna que será adicionada durante a transformação
    'data_carga_dw': {'new_name': 'data_carga_dw', 'type': DateTime}
}

# --- Seção 2: Funções de Transformação e Limpeza ---

def limpar_numero(valor):
    """
    Limpa strings de números (removendo não-dígitos) e as converte para Int64.
    Retorna None para valores inválidos ou NaN.
    """
    if pd.isna(valor):
        return pd.NA # Retorna Pandas NA para inteiros nulos
    if isinstance(valor, (int, float, pd.Int64Dtype.type)): # Inclui Pandas nullable int
        return pd.NA if pd.isna(valor) else int(valor) # Converte para int nativo se não for NA

    s = str(valor).strip()
    s = re.sub(r'[^\d]', '', s) # Remove tudo que não for dígito
    try:
        if s: # Garante que a string não está vazia após a limpeza
            return int(s)
        else:
            return pd.NA
    except ValueError:
        print(f"Aviso: Não foi possível converter o valor '{str(valor)[:50]}' para número. Retornando NA.")
        return pd.NA

# --- Seção 3: Pipeline ETL ---

def run_etl_cadastro_clientes(excel_file_path=EXCEL_FILE):
    """
    Executa o pipeline ETL completo para Cadastro de Clientes: Extração, Transformação e Carga.
    """
    df = None 
    connection = None
    cursor = None
    
    try:
        print(f"--- Iniciando ETL de Cadastro de Clientes ---")

        # 1. Extração
        print(f"\n1. Extraindo dados do arquivo '{excel_file_path}', aba '{EXCEL_SHEET_NAME}'...")
        try:
            df_raw = pd.read_excel(excel_file_path, sheet_name=EXCEL_SHEET_NAME)
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

        print("\n2. Iniciando transformações para Cadastro de Clientes...")

        df = df_raw.copy()

        # 2.1 Seleção e renomeação de colunas
        columns_to_select_and_rename = {
            excel_col: config['new_name'] 
            for excel_col, config in COLUMN_MAPPING_AND_TYPES.items() 
            if excel_col in df.columns
        }
        
        missing_excel_cols = [col for col in COLUMN_MAPPING_AND_TYPES.keys() if col not in df.columns]
        if missing_excel_cols:
            print(f"Atenção: As seguintes colunas esperadas NÃO foram encontradas no Excel e serão ignoradas: {missing_excel_cols}")
        
        existing_cols_in_mapping = [col for col in columns_to_select_and_rename.keys() if col in df.columns]
        df = df[existing_cols_in_mapping].rename(columns=columns_to_select_and_rename)

        print("\n--- Diagnóstico de Colunas Após Seleção e Renomeação ---")
        print("Colunas no DataFrame após seleção e renomeação:")
        for col in df.columns:
            print(f"- '{col}'")
        print("------------------------------------------------------\n")

        # 2.2 Limpeza e conversão de colunas numéricas
        numeric_cols_to_clean = ['populacao_cidade']
        for col in numeric_cols_to_clean:
            if col in df.columns:
                df[col] = df[col].apply(limpar_numero)
                df[col] = df[col].astype('Int64') # Garante que é um inteiro nullable do Pandas
                print(f"Coluna '{col}' limpa e convertida para Int64.")
            else:
                print(f"Aviso: Coluna numérica '{col}' não encontrada no DataFrame para limpeza.")

        # 2.3 Conversão de datas
        date_cols = ['data_cadastro_cliente']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"Coluna '{col}' convertida para formato de data.")
            else:
                print(f"Aviso: Coluna de data '{col}' não encontrada no DataFrame.")

        # 2.4 Tratamento de CNPJ/CPF (garantir string, remover não-dígitos, preencher vazios com None)
        if 'cnpj_cliente' in df.columns:
            df['cnpj_cliente'] = df['cnpj_cliente'].astype(str).str.replace(r'[^\d]', '', regex=True)
            df['cnpj_cliente'] = df['cnpj_cliente'].replace('', None) # Substitui strings vazias por None
            print("Coluna 'cnpj_cliente' tratada e formatada.")
        else:
            print("Aviso: Coluna 'cnpj_cliente' não encontrada no DataFrame para tratamento.")

        # 2.5 Tratamento de colunas de texto com valores NaN (para evitar que NaN se torne 'nan' string)
        for col_name, col_config in COLUMN_MAPPING_AND_TYPES.items():
            if isinstance(col_config['type'], (String, Text)) and col_config['new_name'] in df.columns:
                # Preenche NaN/None com string vazia antes de garantir o tipo string
                df[col_config['new_name']] = df[col_config['new_name']].fillna('')
                df[col_config['new_name']] = df[col_config['new_name']].astype(str)
                print(f"Coluna de texto '{col_config['new_name']}' tratada para nulos e convertida para string.")

        # 2.6 Remover registros sem chaves essenciais
        chaves_essenciais = ['codigo_parceiro', 'nome_parceiro', 'data_cadastro_cliente']
        chaves_existentes_para_dropna = [col for col in chaves_essenciais if col in df.columns]

        if chaves_existentes_para_dropna:
            initial_rows = df.shape[0]
            df.dropna(subset=chaves_existentes_para_dropna, inplace=True)
            if df.shape[0] < initial_rows:
                print(f"Removidas {initial_rows - df.shape[0]} linhas com chaves essenciais nulas.")
            else:
                print("Nenhuma linha removida por chaves essenciais nulas.")
        else:
            print("Aviso: Nenhuma chave essencial encontrada no DataFrame para aplicar dropna.")

        # 2.7 Adiciona timestamp de carga
        df['data_carga_dw'] = pd.Timestamp.now()
        print(f"Coluna 'data_carga_dw' adicionada com o timestamp atual: {df['data_carga_dw'].iloc[0]}")

        print("\nTransformações Concluídas com sucesso para Cadastro de Clientes.")
        print(f"DataFrame transformado tem {df.shape[0]} linhas e {df.shape[1]} colunas.")
        print("\nPrimeiras linhas do DataFrame transformado (df.head()):")
        print(df.head())
        print("\nInformações do DataFrame transformado (df.info()):")
        df.info()

        # 3. Carga para MySQL
        print(f"\n3. Iniciando carga para a tabela de staging '{STAGING_TABLE_NAME}' no MySQL...")
        mysql_connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        print(f"Tentando criar engine para o banco de dados: mysql+pymysql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        engine = create_engine(mysql_connection_string)
        print("Engine criada com sucesso. Tentando obter conexão bruta...")

        # Obtém uma conexão bruta (DBAPI2) do motor SQLAlchemy
        connection = engine.raw_connection()
        cursor = connection.cursor() # Obtém um cursor da conexão bruta
        print("Conexão bruta e cursor obtidos.")

        # Passo 3.1: DROP manual da tabela se existir
        print(f"Verificando e droppando a tabela '{STAGING_TABLE_NAME}' se existir...")
        drop_table_sql = f"DROP TABLE IF EXISTS `{STAGING_TABLE_NAME}`;"
        cursor.execute(drop_table_sql)
        connection.commit() # Confirma a operação de drop
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
            else: # Fallback para colunas que não estejam no mapeamento inicial (ex: data_carga_dw)
                if col_name == 'data_carga_dw':
                    sql_type = DateTime
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
        connection.commit() # Confirma a criação da tabela
        print(f"Tabela '{STAGING_TABLE_NAME}' criada explicitamente no MySQL.")

        # Passo 3.3: Carregamento de dados - AGORA COM INSERÇÃO MANUAL (executemany)
        print("Iniciando carregamento de dados via inserção manual com cursor.executemany...")

        # A ordem das colunas para o INSERT deve ser a mesma do DataFrame
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
                    processed_row.append(None) # Converte NaN para None para NULL no DB
                elif isinstance(val, pd.Int64Dtype.type): # Lida com Int64 do Pandas
                    processed_row.append(None if pd.isna(val) else int(val)) # Converte para int nativo ou None
                else:
                    processed_row.append(val)
            data_to_insert.append(tuple(processed_row))

        # Constrói a instrução INSERT
        columns_sql = ", ".join([f"`{col}`" for col in column_order_for_insert]) 
        placeholders = ", ".join(["%s"] * len(column_order_for_insert)) 
        
        insert_sql = f"INSERT INTO `{STAGING_TABLE_NAME}` ({columns_sql}) VALUES ({placeholders})"

        BATCH_SIZE = 1000 # Define um tamanho de lote para inserções (pode ajustar para performance)

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
            connection.rollback() # Rollback mudanças se a inserção falhar
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
    run_etl_cadastro_clientes()
