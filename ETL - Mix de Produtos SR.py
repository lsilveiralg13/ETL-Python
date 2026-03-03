import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, BigInteger, String, Numeric, Float, Text # Importa tipos SQLAlchemy para DDL
from sqlalchemy.schema import Table, Column, MetaData, CreateTable # Importações para DDL explícito

# --- Seção 1: Configurações ---
# Detalhes do arquivo Excel
EXCEL_FILE = "MIX PRODUTOS SHOWROOM ETL.xlsx"
EXCEL_SHEET_NAME = 0 # Ou o nome da aba, ex: "Dados Produtos"

# Detalhes do Banco de Dados MySQL
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306 # Porta do MySQL (padrão é 3306)
DB_NAME = 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_mix_produtos_showroom' # Nome da tabela para este ETL

# Mapeamento de colunas do Excel para o MySQL e seus tipos de dados SQLAlchemy
# Isso será usado tanto para renomear colunas quanto para criar a tabela no MySQL
COLUMN_MAPPING_AND_TYPES = {
    "Num. Único": {"new_name": "numero_unico", "type": BigInteger},
    "Cod. Parceiro": {"new_name": "cod_parceiro", "type": BigInteger},
    "Nome Parceiro": {"new_name": "nome_parceiro", "type": String(255)},
    "Apelido (Vendedor)": {"new_name": "apelido_vendedor", "type": String(100)},
    "Dt. Negociação": {"new_name": "dt_negociacao", "type": DateTime},
    "Cod Tipo Titulo": {"new_name": "cod_tipo_titulo", "type": String(50)},
    "Cod. Produto": {"new_name": "cod_produto", "type": Integer},
    "Referencia": {"new_name": "referencia", "type": String(100)},
    "Descr. Produto": {"new_name": "descr_produto", "type": String(255)},
    "Controle": {"new_name": "controle", "type": String(50)},
    "Quantidade": {"new_name": "quantidade", "type": Integer},
    "VALOR(R$)": {"new_name": "valor_produto", "type": Numeric(15, 2)}, # DECIMAL para MySQL
    "VALOR(R$) TOTAL": {"new_name": "valor_total", "type": Numeric(15, 2)}, # DECIMAL para MySQL
    "Grupo Produto": {"new_name": "grupo_produto", "type": String(100)},
    "CHAVE_MMM": {"new_name": "chave_mmm", "type": String(50)},
    "CHAVE_AAA": {"new_name": "chave_aaa", "type": Integer},
    "ESSENTIAL (S/N)": {"new_name": "essential_y_n", "type": String(50)},
    "TIPO_COLECAO": {"new_name": "tipo_colecao", "type": String(50)},
    "TIPO_EVENTO": {"new_name": "tipo_evento", "type": String(50)},
    # Coluna que será adicionada durante a transformação
    "data_carga_dw": {"new_name": "data_carga_dw", "type": DateTime}
}

# Helper para converter objetos de tipo SQLAlchemy para strings de tipo SQL (para df.to_sql ou referência)
def get_sql_type_string(sql_alchemy_type_obj):
    if isinstance(sql_alchemy_type_obj, Integer):
        return 'INT'
    elif isinstance(sql_alchemy_type_obj, DateTime):
        return 'DATETIME'
    elif isinstance(sql_alchemy_type_obj, BigInteger):
        return 'BIGINT'
    elif isinstance(sql_alchemy_type_obj, String):
        return f'VARCHAR({sql_alchemy_type_obj.length})'
    elif isinstance(sql_alchemy_type_obj, Numeric):
        return f'DECIMAL({sql_alchemy_type_obj.precision}, {sql_alchemy_type_obj.scale})'
    elif isinstance(sql_alchemy_type_obj, Float): # Caso algum float passe, embora Numeric seja preferível para dinheiro
        return 'FLOAT'
    elif isinstance(sql_alchemy_type_obj, Text):
        return 'TEXT'
    return str(sql_alchemy_type_obj)

# Dicionário de tipos para df.to_sql (principalmente para referência agora, dada a inserção manual)
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

def run_etl_mix_produtos_showroom(excel_file_path=EXCEL_FILE):
    """
    Executa o pipeline ETL completo para Mix de Produtos Showroom: Extração, Transformação e Carga.
    """
    df = None # Inicializa df para garantir que esteja definido
    connection = None
    cursor = None
    
    try:
        print(f"--- Iniciando ETL de Mix de Produtos Showroom ---")

        # 1. Extração
        print(f"\n1. Extraindo dados do arquivo '{excel_file_path}', aba '{EXCEL_SHEET_NAME}'...")
        try:
            df_raw = pd.read_excel(excel_file_path, sheet_name=EXCEL_SHEET_NAME)
            print(f"Dados extraídos com sucesso! {df_raw.shape[0]} linhas e {df_raw.shape[1]} colunas.")
        except FileNotFoundError:
            print(f"Erro: Arquivo '{excel_file_path}' não encontrado. Verifique o caminho e o nome do arquivo.")
            return # Sai da função se o arquivo não for encontrado
        except Exception as e:
            print(f"Erro ao ler o arquivo Excel: {e}")
            return # Sai da função em caso de erro de leitura
        
        # Cria uma cópia para as transformações
        df = df_raw.copy()
        
        print("\n2. Iniciando transformações...")

        # 2.1 Seleção e renomeação de colunas
        # Filtra COLUMN_MAPPING_AND_TYPES para incluir apenas as colunas presentes no df_raw
        columns_to_select_and_rename = {
            excel_col: config['new_name'] 
            for excel_col, config in COLUMN_MAPPING_AND_TYPES.items() 
            if excel_col in df.columns
        }
        
        # Verifica colunas ausentes no Excel
        missing_excel_cols = [col for col in COLUMN_MAPPING_AND_TYPES.keys() if col not in df.columns]
        if missing_excel_cols:
            print(f"Atenção: As seguintes colunas esperadas não foram encontradas no Excel e serão ignoradas: {missing_excel_cols}")

        df = df[list(columns_to_select_and_rename.keys())].rename(columns=columns_to_select_and_rename)
        print(f"Colunas selecionadas e renomeadas. DataFrame agora tem {df.shape[1]} colunas.")

        # 2.2 Limpeza de colunas monetárias
        monetary_cols = ['valor_produto', 'valor_total'] # Ajustado para o novo mapeamento
        for col in monetary_cols:
            if col in df.columns:
                df[col] = df[col].apply(limpar_moedas)
                print(f"Coluna '{col}' limpa e convertida para numérico.")
        
        # Teste de conversão da moeda (exemplo com valor_total)
        if 'valor_total' in df.columns:
            soma_total = df['valor_total'].sum()
            print(f"Valor Total do Item (após limpeza): R$ {soma_total:,.2f}")
        
        # 2.3 Conversão de datas
        date_cols = ['dt_negociacao', 'data_faturamento_item'] # 'data_faturamento_item' do ETL anterior está aqui também, caso exista no novo arquivo
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"Coluna '{col}' convertida para datetime.")

        # 2.4 Conversão de inteiros (Qtd Itens e códigos BigInt)
        # Usamos uma lista mais específica para evitar problemas com colunas que devem ser strings
        int_bigint_cols = [
            'codigo_produto', 
            'quantidade', 
            'numero_unico', 
            'cod_parceiro',
            'chave_aaa'
        ]
        for col in int_bigint_cols:
            if col in df.columns:
                # Usa 'Int64' para permitir NaNs em colunas de inteiros
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                print(f"Coluna '{col}' convertida para numérico/Int64.")

        # 2.5 Remove registros sem data de negociação (crítico para a maioria dos ETLs de vendas)
        if 'dt_negociacao' in df.columns:
            initial_rows = df.shape[0]
            df.dropna(subset=['dt_negociacao'], inplace=True)
            if df.shape[0] < initial_rows:
                print(f"Removidas {initial_rows - df.shape[0]} linhas com 'dt_negociacao' vazia.")
            else:
                print("Nenhuma linha removida por 'dt_negociacao' nula.")

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
        print(f"\n3. Iniciando carga para a tabela de staging '{STAGING_TABLE_NAME}' no MySQL...")
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
        
        metadata = MetaData()
        
        table_columns = []
        for col_name in df.columns: # Itera pelas colunas do DF transformado
            # Encontra o tipo SQLAlchemy para a coluna atual do DataFrame
            col_type_obj = next((item['type'] for item in COLUMN_MAPPING_AND_TYPES.values() if item['new_name'] == col_name), None)
            
            if col_type_obj:
                sql_type = col_type_obj
            else: # Fallback para colunas não mapeadas (ex: data_carga_dw que é adicionada depois)
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
                # Trata tipos datetime e NaN para compatibilidade com MySQL/PyMySQL
                if pd.api.types.is_datetime64_any_dtype(df[col_name]) and pd.notna(val):
                    processed_row.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                elif pd.isna(val) or val is None: # Lida com NaN do Pandas e None do Python
                    processed_row.append(None) 
                # Adiciona tratamento para 'Int64' (nullable int do Pandas)
                elif isinstance(val, pd.Int64Dtype.type):
                    processed_row.append(int(val)) # Converte para int nativo se não for NaN
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
    run_etl_mix_produtos_showroom()


