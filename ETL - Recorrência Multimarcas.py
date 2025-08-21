import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, BigInteger, String, Numeric, Float, Text # Importa tipos SQLAlchemy para DDL
from sqlalchemy.schema import Table, Column, MetaData, CreateTable # Importações para DDL explícito
import os # Importar para usar variáveis de ambiente
import re # Importar para a função limpar_numero
import warnings

# Ignora UserWarnings do pandas que podem ocorrer
warnings.filterwarnings('ignore', category=UserWarning)

# --- Seção 1: Configurações ---
# Detalhes do arquivo Excel
EXCEL_FILE = "RECORRÊNCIA CLIENTES ETL.xlsx"
EXCEL_SHEET_NAME = 0 # Ou o nome da aba, ex: "Planilha1"

# Detalhes do Banco de Dados MySQL
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'root')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '3306'))
DB_NAME = 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_recorrencia_clientes' # Nome da tabela para este ETL
SHOWROOM_TABLE_NAME = 'staging_mix_produtos_showroom' # Nome da tabela do showroom para integração

# Mapeamento de colunas do Excel para o MySQL e seus tipos de dados SQLAlchemy
# Isso será usado tanto para renomear colunas quanto para criar a tabela no MySQL
COLUMN_MAPPING_AND_TYPES = {
    'Recorrencia': {'new_name': 'recorrencia', 'type': String(50)},
    'Mês de Apuração': {'new_name': 'mes_apuracao', 'type': String(50)},
    'Status': {'new_name': 'status_cliente', 'type': String(50)},
    'Cód. Parceiro': {'new_name': 'codigo_parceiro', 'type': BigInteger},
    'Nome Parceiro (Parceiro)': {'new_name': 'nome_parceiro', 'type': String(255)},
    'Status Lojista': {'new_name': 'status_lojista', 'type': String(50)},
    'MOTIVO': {'new_name': 'motivo', 'type': String(255)},
    'CNPJ': {'new_name': 'cnpj', 'type': String(20)},
    'Cidade': {'new_name': 'cidade', 'type': String(100)},
    'UF': {'new_name': 'uf', 'type': String(2)},
    'Apelido (Vendedor)': {'new_name': 'apelido_vendedor', 'type': String(100)},
    'Dias sem Compra': {'new_name': 'dias_sem_compra', 'type': Integer},
    'QTDE. PEDIDOS - 90 DIAS': {'new_name': 'qtde_pedidos_90_dias', 'type': Integer},
    'Data de Cadastro': {'new_name': 'data_cadastro', 'type': DateTime},
    'Data Última Compra': {'new_name': 'data_ultima_compra', 'type': DateTime},
    'DIA HOJE': {'new_name': 'dia_hoje', 'type': DateTime},
    'TEMPO DE CASA (Anos)': {'new_name': 'tempo_de_casa_anos', 'type': Integer},
    'COMPRA EM DIAS REAL': {'new_name': 'compra_em_dias_real', 'type': Integer},
    'BLOCOS DE RECORRÊNCIA': {'new_name': 'blocos_recorrencia', 'type': String(100)},
    'CHAVE_MMM': {'new_name': 'chave_mmm', 'type': String(50)},
    'CHAVE_AAA': {'new_name': 'chave_aaa', 'type': Integer},
    'CHAVE DATA ÚLT. COMPRA': {'new_name': 'chave_data_ult_compra', 'type': DateTime},
    # Colunas que serão adicionadas durante a transformação
    'data_carga_dw': {'new_name': 'data_carga_dw', 'type': DateTime},
    'valor_total_showroom_agregado': {'new_name': 'valor_total_showroom_agregado', 'type': Numeric(15, 2)} # De integração
}

# --- Seção 2: Funções de Transformação e Limpeza ---

def limpar_numero_int(valor):
    """
    Limpa strings de números (removendo não-dígitos) e as converte para Int64.
    Retorna pd.NA para valores inválidos ou NaN.
    """
    if pd.isna(valor):
        return pd.NA 
    if isinstance(valor, (int, float, pd.Int64Dtype.type)):
        return pd.NA if pd.isna(valor) else int(valor)

    s = str(valor).strip()
    s = re.sub(r'[^\d]', '', s)
    try:
        if s: 
            return int(s)
        else:
            return pd.NA
    except ValueError:
        print(f"Aviso: Não foi possível converter o valor '{str(valor)[:50]}' para número inteiro. Retornando NA.")
        return pd.NA

# --- Seção 3: Pipeline ETL ---

def run_etl_recorrencia_clientes(excel_file_path=EXCEL_FILE):
    """
    Executa o pipeline ETL completo para Recorrência de Clientes: Extração, Transformação e Carga.
    """
    df = None 
    connection = None
    cursor = None
    
    try:
        print(f"--- Iniciando ETL de Recorrência de Clientes ---")

        # 1. Extração
        print(f"\n1. Extraindo dados do arquivo '{excel_file_path}', aba '{EXCEL_SHEET_NAME}'...")
        try:
            # usecols com as chaves originais para otimizar a leitura
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

        print("\n2. Iniciando transformações para Recorrência de Clientes...")

        df = df_raw.copy()

        # 2.1 Seleção e renomeação de colunas
        columns_to_select_and_rename = {
            excel_col: config['new_name'] 
            for excel_col, config in COLUMN_MAPPING_AND_TYPES.items() 
            if excel_col in df.columns
        }
        
        missing_excel_cols = [col for col in COLUMN_MAPPING_AND_TYPES.keys() if col not in df.columns and col not in ['data_carga_dw', 'valor_total_showroom_agregado']] # Ignora cols derivadas
        if missing_excel_cols:
            print(f"Atenção: As seguintes colunas esperadas NÃO foram encontradas no Excel e serão ignoradas: {missing_excel_cols}")
        
        existing_cols_in_mapping = [col for col in columns_to_select_and_rename.keys() if col in df.columns]
        df = df[existing_cols_in_mapping].rename(columns=columns_to_select_and_rename)

        print("\n--- Diagnóstico de Colunas Após Seleção e Renomeação ---")
        print("Colunas no DataFrame após seleção e renomeação:")
        for col in df.columns:
            print(f"- '{col}'")
        print("------------------------------------------------------\n")

        # 2.2 Limpeza e conversão de colunas numéricas (inteiros)
        int_cols = ['dias_sem_compra', 'qtde_pedidos_90_dias', 'tempo_de_casa_anos', 'compra_em_dias_real', 'chave_aaa']
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].apply(limpar_numero_int)
                df[col] = df[col].astype('Int64') # Garante que é um inteiro nullable do Pandas
                print(f"Coluna '{col}' limpa e convertida para Int64.")
            else:
                print(f"Aviso: Coluna de inteiro '{col}' não encontrada no DataFrame para limpeza.")

        # 2.3 Tratamento de CNPJ (garantir string, remover não-dígitos, preencher vazios com None)
        if 'cnpj' in df.columns: 
            df['cnpj'] = df['cnpj'].astype(str).str.replace(r'[^0-9]', '', regex=True).replace('', None)
            print("Coluna 'cnpj' tratada e formatada.")
        else:
            print("Aviso: Coluna 'cnpj' não encontrada no DataFrame para tratamento.")

        # 2.4 Conversão de datas
        date_cols = ['data_cadastro', 'data_ultima_compra', 'dia_hoje', 'chave_data_ult_compra']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].dt.date # Converte para a parte da data (Python date object)
                print(f"Coluna '{col}' convertida para tipo datetime.")
            else:
                print(f"Aviso: Coluna de data '{col}' não encontrada no DataFrame.")

        # 2.5 Tratamento de colunas de texto com valores NaN (para evitar 'nan' string)
        for col_excel, col_config in COLUMN_MAPPING_AND_TYPES.items():
            new_name = col_config['new_name']
            sql_type = col_config['type']
            # Verifica se a coluna é do tipo String ou Text e se está no DataFrame
            if isinstance(sql_type, (String, Text)) and new_name in df.columns:
                # Verifica se há valores nulos ou se o dtype é 'object' (indicando strings mistas/nulas)
                if df[new_name].isnull().any() or df[new_name].dtype == 'object': 
                    df[new_name] = df[new_name].fillna('') # Preenche NaN/None com string vazia
                    df[new_name] = df[new_name].astype(str) # Garante que é tipo string
                    print(f"Coluna de texto '{new_name}' tratada para nulos e convertida para string.")

        # 2.6 Integração com dados do Showroom
        print("\nIniciando a busca e agregação de dados do Showroom...")
        showroom_connection = None
        showroom_engine = None
        try:
            showroom_connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
            showroom_engine = create_engine(showroom_connection_string)

            # Consulta para somar valor_total_showroom por codigo_parceiro
            query_showroom = f"""
                SELECT
                    codigo_parceiro,
                    SUM(valor_total_showroom) AS valor_total_showroom_agregado
                FROM
                    `{SHOWROOM_TABLE_NAME}`
                GROUP BY
                    codigo_parceiro
            """
            df_showroom = pd.read_sql(query_showroom, showroom_engine)
            print(f"Dados do Showroom carregados: {df_showroom.shape[0]} registros de parceiros com vendas.")

            # Realiza o JOIN (Left Join para manter todos os clientes de recorrencia)
            df = pd.merge(df, df_showroom, left_on='codigo_parceiro', right_on='codigo_parceiro', how='left')
            
            # Preenche valores NaN (clientes sem vendas no showroom) com 0.0 e garante o tipo numérico
            if 'valor_total_showroom_agregado' in df.columns:
                df['valor_total_showroom_agregado'] = pd.to_numeric(df['valor_total_showroom_agregado'], errors='coerce').fillna(0.0)
                print("Coluna 'valor_total_showroom_agregado' adicionada e tratada.")
            else:
                print("AVISO: Coluna 'valor_total_showroom_agregado' não foi criada após o merge. Verifique o nome da coluna de valores no showroom e a chave de join.")

        except Exception as e:
            print(f"ERRO ao integrar dados do Showroom: {e}")
            # Em caso de erro grave na integração do showroom, adicione a coluna com zeros para não falhar a carga
            if 'valor_total_showroom_agregado' not in df.columns:
                df['valor_total_showroom_agregado'] = 0.0 # Garante que a coluna exista para a carga posterior


        # 2.7 Remover registros sem chaves essenciais
        chaves_essenciais = ['codigo_parceiro', 'nome_parceiro', 'data_cadastro'] 
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


        # 2.8 Adicionar timestamp de carga
        df['data_carga_dw'] = pd.Timestamp.now()
        print(f"Coluna 'data_carga_dw' adicionada com o timestamp atual: {df['data_carga_dw'].iloc[0]}")

        print("\nTransformações Concluídas para Recorrência de Clientes.")
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
            else: # Fallback para colunas que não estejam no mapeamento inicial (como data_carga_dw ou valor_total_showroom_agregado)
                if col_name == 'data_carga_dw':
                    sql_type = DateTime
                elif col_name == 'valor_total_showroom_agregado':
                    sql_type = Numeric(15, 2)
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
                    # MySQL espera 'YYYY-MM-DD HH:MM:SS' para DATETIME.
                    # Se o tipo é apenas date (Python date object), a conversão para string deve ser 'YYYY-MM-DD'.
                    # Vamos tratar ambos os casos
                    if isinstance(val, pd.Timestamp): # Se for um timestamp (data e hora)
                        processed_row.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                    elif isinstance(val, (pd.NaT, type(None))): # Handles NaT for datetime as well
                        processed_row.append(None)
                    else: # Se for um objeto date padrão de Python (sem hora)
                         processed_row.append(val.strftime('%Y-%m-%d') if hasattr(val, 'strftime') else str(val))
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
        if connection: # Fecha a conexão bruta antes do cursor, se ainda estiver aberta
            connection.close()
            print("Conexão com o banco de dados fechada.")
        if cursor: # Fecha o cursor (normalmente já fechado com a conexão, mas garante)
            cursor.close()
            print("Cursor do banco de dados fechado.")

# --- Execução do ETL ---
if __name__ == "__main__":
    run_etl_recorrencia_clientes()
