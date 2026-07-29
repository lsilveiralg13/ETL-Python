import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, BigInteger, String, Numeric, Float, Text # Importa tipos específicos do SQLAlchemy para o DDL
from sqlalchemy.schema import Table, Column, MetaData, CreateTable # Importações para criar o DDL explícito

# --- Seção 1: Configurações ---
# Detalhes do arquivo Excel
EXCEL_FILE = "FATURAMENTO ETL.xlsx"
EXCEL_SHEET_NAME = "FAT - TOTAL"

# Detalhes do Banco de Dados MySQL
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_faturamento_multimarcas'

# Mapeamento de colunas do Excel para o MySQL e seus tipos de dados SQLAlchemy
# Isso será usado tanto para renomear colunas quanto para criar a tabela no MySQL
COLUMN_MAPPING_AND_TYPES = {
    'Nro. Nota': {'new_name': 'numero_nota', 'type': Integer},
    'Dt. Neg.': {'new_name': 'data_negociacao', 'type': DateTime},
    'CHAVE_MES': {'new_name': 'chave_mes', 'type': String(50)},
    'CHAVE_ANO': {'new_name': 'chave_ano', 'type': Integer},
    'CHAVE_SEMANA': {'new_name': 'chave_semana', 'type': String(50)},
    'CHAVE_TRI': {'new_name': 'chave_trimestre', 'type': String(50)},
    'DIAUTIL_ANO': {'new_name': 'dia_util', 'type': Integer},
    'TIPO_VENDA': {'new_name': 'tipo_venda', 'type': String(50)},
    'TIPO_SHOWROOM': {'new_name': 'tipo_showroom', 'type': String(50)},
    'Qtd Itens': {'new_name': 'qtd_itens', 'type': Integer},
    'Vlr. Nota': {'new_name': 'valor_faturado', 'type': Numeric(15, 2)}, # DECIMAL para MySQL
    'Desconto total por item': {'new_name': 'desconto', 'type': Numeric(15, 2)}, # DECIMAL para MySQL
    'Descrição (Tipo de Negociação)': {'new_name': 'tipo_negociacao', 'type': String(100)},
    'BOLETO OU CARTÃO': {'new_name': 'forma_pagamento', 'type': String(50)},
    'Status NF-e': {'new_name': 'status_nfe', 'type': String(50)},
    'Descrição (Tipo de Operação)': {'new_name': 'tipo_operacao', 'type': String(100)},
    'Nro. Único': {'new_name': 'numero_unico', 'type': BigInteger},
    'Parceiro': {'new_name': 'codigo_parceiro', 'type': BigInteger},
    'Nome Parceiro (Parceiro)': {'new_name': 'nome_parceiro', 'type': String(255)},
    'CIDADE': {'new_name': 'cidade_cliente', 'type': String(100)},
    'UF': {'new_name': 'estado_cliente', 'type': String(2)},
    'REGIÃO': {'new_name': 'regiao', 'type': String(50)},
    'Dt. do Faturamento': {'new_name': 'data_faturamento', 'type': DateTime},
    'CNPJ/CPF do Parceiro': {'new_name': 'cnpj_parceiro', 'type': String(20)}, # CNPJ/CPF como string para preservar zeros
    'Nome Fantasia (Empresa)': {'new_name': 'empresa_emissora', 'type': String(100)},
    'Apelido (Vendedor)': {'new_name': 'vendedor', 'type': String(100)},
    'Nome Parceiro (Transportadora)': {'new_name': 'transportadora', 'type': String(100)},
    # Coluna que será adicionada durante a transformação
    'data_carga_dw': {'new_name': 'data_carga_dw', 'type': DateTime}
}

# Helper para converter objetos de tipo SQLAlchemy para strings de tipo SQL
def get_sql_type_string(sql_alchemy_type_obj):
    if isinstance(sql_alchemy_type_obj, Integer):
        return 'INT'
    elif isinstance(sql_alchemy_type_obj, DateTime):
        return 'DATETIME'
    elif isinstance(sql_alchemy_type_obj, BigInteger):
        return 'BIGINT'
    elif isinstance(sql_alchemy_type_obj, String):
        # Para String, precisamos do comprimento
        return f'VARCHAR({sql_alchemy_type_obj.length})'
    elif isinstance(sql_alchemy_type_obj, Numeric):
        # Para Numeric, precisamos da precisão e escala
        return f'DECIMAL({sql_alchemy_type_obj.precision}, {sql_alchemy_type_obj.scale})'
    elif isinstance(sql_alchemy_type_obj, Float):
        return 'FLOAT'
    elif isinstance(sql_alchemy_type_obj, Text):
        return 'TEXT'
    # Fallback caso um tipo não seja explicitamente tratado
    return str(sql_alchemy_type_obj)

# Tipos de dados a serem usados pelo df.to_sql para garantir conversão na inserção
# Usa as strings de tipo SQL, conforme o pandas.to_sql espera para o parâmetro dtype
# OBS: Com a inserção manual via executemany, este dicionário não é mais usado por df.to_sql,
# mas é mantido como referência para os tipos de dados esperados no MySQL.
DF_TO_SQL_DTYPES = {
    col_info['new_name']: get_sql_type_string(col_info['type']) 
    for col_info in COLUMN_MAPPING_AND_TYPES.values()
}


# --- Seção 2: Funções de Transformação e Limpeza ---

def limpar_moedas(valor):
    """
    Limpa strings de moeda (ex: 'R$ 1.234,56') e as converte para float.
    Lida com valores NaN e já numéricos.
    """
    if pd.isna(valor):
        return None
    
    # Se o valor já for numérico (int ou float), apenas o converte para float e retorna
    if isinstance(valor, (int, float)):
        return float(valor)

    s = str(valor)
    s = s.replace('R$', '').strip() # Remove "R$" e espaços extras

    # Formato Brasileiro: remove pontos de milhar, substitui vírgula decimal por ponto
    if ',' in s:
        s = s.replace('.', '') 
        s = s.replace(',', '.')
    
    try:
        return float(s)
    except ValueError:
        print(f"Aviso: Não foi possível converter o valor '{str(valor)[:30]}' para float após a limpeza.")
        return None

# --- Seção 3: Pipeline ETL ---

def run_etl():
    """
    Executa o pipeline ETL completo: Extração, Transformação e Carga.
    """
    df = None # Inicializa df para garantir que esteja definido
    connection = None
    cursor = None
    
    try:
        print(f"--- Iniciando ETL de Faturamento Multimarcas ---")

        # 1. Extração
        print(f"\n1. Extraindo dados do arquivo '{EXCEL_FILE}', aba '{EXCEL_SHEET_NAME}'...")
        try:
            df_raw = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
            print(f"Dados extraídos com sucesso! {df_raw.shape[0]} linhas e {df_raw.shape[1]} colunas.")
        except FileNotFoundError:
            print(f"Erro: Arquivo '{EXCEL_FILE}' não encontrado. Verifique o caminho e o nome do arquivo.")
            return # Sai da função se o arquivo não for encontrado
        except Exception as e:
            print(f"Erro ao ler o arquivo Excel: {e}")
            return # Sai da função em caso de erro de leitura
        
        # Cria uma cópia para as transformações
        df = df_raw.copy()
        
        print("\n2. Iniciando transformações...")

        # 2.1 Seleção e renomeação de colunas
        columns_to_select = {excel_col: config['new_name'] for excel_col, config in COLUMN_MAPPING_AND_TYPES.items() if excel_col in df.columns}
        
        missing_excel_cols = [col for col in columns_to_select.keys() if col not in df.columns]
        if missing_excel_cols:
            print(f"Atenção: As seguintes colunas não foram encontradas no Excel e serão ignoradas: {missing_excel_cols}")
            for col in missing_excel_cols:
                del columns_to_select[col] # Remove do mapeamento se não existe

        df = df[list(columns_to_select.keys())].rename(columns=columns_to_select)
        print(f"Colunas selecionadas e renomeadas. DataFrame agora tem {df.shape[1]} colunas.")

        # 2.2 Limpeza de colunas monetárias
        monetary_cols = ['valor_faturado', 'desconto']
        for col in monetary_cols:
            if col in df.columns:
                df[col] = df[col].apply(limpar_moedas)
                print(f"Coluna '{col}' limpa e convertida para numérico.")
        
        # Teste de conversão da moeda
        if 'valor_faturado' in df.columns:
            soma_total = df['valor_faturado'].sum()
            print(f"Faturamento total (após limpeza): R$ {soma_total:,.2f}")
        
        # 2.3 Conversão de datas
        date_cols = ['data_negociacao', 'data_faturamento']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"Coluna '{col}' convertida para datetime.")

        # 2.4 Conversão de inteiros (Qtd Itens)
        if 'qtd_itens' in df.columns:
            df['qtd_itens'] = pd.to_numeric(df['qtd_itens'], errors='coerce')
            print(f"Coluna 'qtd_itens' convertida para numérico.")

        # 2.5 Remove registros sem data de negociação (crítico para integridade)
        if 'data_negociacao' in df.columns:
            initial_rows = df.shape[0]
            df.dropna(subset=['data_negociacao'], inplace=True)
            if df.shape[0] < initial_rows:
                print(f"{initial_rows - df.shape[0]} linhas removidas devido a 'data_negociacao' nula.")
            else:
                print("Nenhuma linha removida por 'data_negociacao' nula.")

        # 2.6 Adiciona timestamp de carga
        df['data_carga_dw'] = pd.Timestamp.now()
        print(f"Coluna 'data_carga_dw' adicionada com o timestamp atual: {df['data_carga_dw'].iloc[0]}")

        print("Transformações Concluídas com sucesso.")
        print(f"DataFrame transformado tem {df.shape[0]} linhas e {df.shape[1]} colunas.")
        print("\nPrimeiras linhas do DataFrame transformado (df.head()):")
        print(df.head())
        print("\nInformações do DataFrame transformado (df.info()):")
        df.info()

        # 3. Carga para MySQL
        print("\n3. Iniciando carga para o MySQL...")
        mysql_connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        print(f"Tentando criar engine para o banco de dados: mysql+pymysql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        engine = create_engine(mysql_connection_string)
        print("Engine criada com sucesso. Tentando obter conexão bruta...")

        # Obtém uma conexão bruta (DBAPI2) do motor SQLAlchemy para garantir compatibilidade
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
        
        # Cria um objeto MetaData para associar a tabela
        metadata = MetaData()
        
        # Constrói o objeto Table do SQLAlchemy usando o DataFrame e o mapeamento de tipos
        table_columns = []
        for col_name in df.columns: # Itera pelas colunas do DF transformado
            # Encontra a configuração de tipo para a coluna renomeada
            original_col_info = next((v for k, v in COLUMN_MAPPING_AND_TYPES.items() if v['new_name'] == col_name), None)
            
            if original_col_info:
                sql_type = original_col_info['type']
            else: # Lida com a coluna 'data_carga_dw' ou outras que não estejam no mapeamento inicial
                if col_name == 'data_carga_dw':
                    sql_type = DateTime
                else:
                    print(f"Aviso: Tipo para a coluna '{col_name}' não encontrado no mapeamento. Usando String(255) como padrão para DDL.")
                    sql_type = String(255) # Tipo padrão de fallback para DDL

            table_columns.append(Column(col_name, sql_type, nullable=True)) # 'nullable=True' por padrão para flexibilidade

        table_obj = Table(STAGING_TABLE_NAME, metadata,
                          *table_columns,
                          mysql_engine='InnoDB', # Motor de armazenamento para MySQL
                          mysql_charset='utf8mb4' # Define o charset para suportar caracteres diversos
                         )
        
        create_table_sql = str(CreateTable(table_obj).compile(engine))
        
        cursor.execute(create_table_sql)
        connection.commit() # Confirma a criação da tabela
        print(f"Tabela '{STAGING_TABLE_NAME}' criada explicitamente no MySQL.")

        # Passo 3.3: Carregamento de dados - AGORA COM INSERÇÃO MANUAL MAIS ROBUSTA (executemany)
        print("Iniciando carregamento de dados via inserção manual com cursor.executemany...")

        # Preparar os dados para inserção no formato que executemany espera (lista de tuplas)
        # As colunas devem estar na mesma ordem que foram definidas no CREATE TABLE
        column_order_for_insert = [col['new_name'] for col_key, col in COLUMN_MAPPING_AND_TYPES.items()]
        
        # Garante que o DataFrame tenha todas as colunas na ordem correta, preenchendo com None se faltar
        # Isso é importante para que o executemany receba as tuplas com a ordem e número de colunas corretos
        missing_df_cols = [col for col in column_order_for_insert if col not in df.columns]
        for m_col in missing_df_cols:
            df[m_col] = None # Adiciona colunas ausentes como None (ou pd.NA, dependendo da preferência)

        df_ordered = df[column_order_for_insert]
        
        # Converte as linhas do DataFrame para uma lista de tuplas
        # Para colunas datetime, assegura que estejam no formato de string que o MySQL espera se o driver não auto-converter
        data_to_insert = []
        for index, row in df_ordered.iterrows():
            processed_row = []
            for col_name in column_order_for_insert:
                val = row[col_name]
                if pd.api.types.is_datetime64_any_dtype(df_ordered[col_name]) and pd.notna(val):
                    processed_row.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                elif pd.isna(val): # Converte NaN para None para que o MySQL entenda como NULL
                    processed_row.append(None)
                else:
                    processed_row.append(val)
            data_to_insert.append(tuple(processed_row))

        # Constrói a instrução INSERT
        columns_sql = ", ".join([f"`{col}`" for col in column_order_for_insert]) # Backticks para nomes de colunas no MySQL
        placeholders = ", ".join(["%s"] * len(column_order_for_insert)) # %s é o placeholder padrão para pymysql
        
        insert_sql = f"INSERT INTO `{STAGING_TABLE_NAME}` ({columns_sql}) VALUES ({placeholders})"

        BATCH_SIZE = 1000 # Define um tamanho de lote para inserções (pode ajustar para performance)

        try:
            for i in range(0, len(data_to_insert), BATCH_SIZE):
                batch = data_to_insert[i:i + BATCH_SIZE]
                if batch: # Garante que o lote não está vazio
                    cursor.executemany(insert_sql, batch)
                    connection.commit() # Comita cada lote
                    print(f"Lote {i // BATCH_SIZE + 1} de {len(data_to_insert) // BATCH_SIZE + 1} carregado. ({len(batch)} linhas)")
            
            print(f"Dados carregados com sucesso na tabela de staging '{STAGING_TABLE_NAME}' via inserção manual!")
        except Exception as insert_e:
            print(f"Erro ao inserir dados via cursor.executemany: {insert_e}")
            connection.rollback() # Rollback mudanças se a inserção falhar
            raise # Re-lança a exceção para ser capturada pelo try-except externo


        print(f"--- ETL Concluído com Sucesso! ---")

    except Exception as e:
        print(f"Erro fatal durante o ETL: {e}")
    finally:
        # Garante que a conexão e o cursor sejam fechados, mesmo que ocorra um erro
        if cursor:
            cursor.close()
            print("Cursor do banco de dados fechado.")
        if connection:
            connection.close()
            print("Conexão com o banco de dados fechada.")

# --- Execução do ETL ---
if __name__ == "__main__":
    run_etl()














    
   
    
    
    
    
    
    
    
    
    
