import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, BigInteger, String, Numeric, Float, Text # Importa tipos específicos do SQLAlchemy para o DDL
from sqlalchemy.schema import Table, Column, MetaData, CreateTable # Importações para criar o DDL explícito
import datetime # Para pd.Timestamp.now()
import traceback # Importa para printar o stack trace completo do erro

# --- Seção 1: Configurações ---
# Detalhes do arquivo Excel
# ATENÇÃO: Atualize com o nome correto do seu arquivo Excel e da aba!
EXCEL_FILE = "OPORTUNIDADES ETL.xlsx" # ATUALIZADO: Nome do arquivo Excel para oportunidades
EXCEL_SHEET_NAME = "OPORTUNIDADES" # ATUALIZADO: Nome da aba do Excel para oportunidades

# Detalhes do Banco de Dados MySQL
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'faturamento_multimarcas_dw' # ATENÇÃO: Verifique se este é o nome do seu banco de dados!
STAGING_TABLE_NAME = 'staging_oportunidades' # Nome da sua tabela de staging para oportunidades

# Mapeamento de colunas do Excel para o MySQL e seus tipos de dados SQLAlchemy
# ATENÇÃO: Este mapeamento precisa refletir TODAS as colunas do seu Excel 'OPORTUNIDADES ETL.xlsx'
# Se as colunas 'VALIDAÇÃO - FRAN.', 'VALIDAÇÃO - MM', 'VALIDAÇÃO - FUNIL' existirem no seu Excel,
# você precisará adicioná-las aqui.
COLUMN_MAPPING_AND_TYPES = {
    "Cod. IBGE": {'new_name': 'codigo_ibge', 'type': Integer}, # Alterado para Integer, geralmente é um código numérico inteiro
    "NOME MUNICIPIO": {'new_name': 'nome_municipio', 'type': String(255)}, 
    "UF": {'new_name': 'uf', 'type': String(2)},
    "ESTADO": {'new_name': 'nome_estado', 'type': String(100)},
    "Mesorregião": {'new_name': 'mesorregiao', 'type': String(100)},
    "REGIÃO": {'new_name': 'regiao', 'type': String(50)},
    "POPULAÇÃO": {'new_name': 'populacao', 'type': BigInteger},
    "BLOCO POPULACIONAL": {'new_name': 'bloco_populacional', 'type': String(100)},
    "CLUSTER ALVO FRANQUIA": {'new_name': 'cluster_alvo_franquia', 'type': String(100)},
    "PIB (Per Capita)": {'new_name': 'pib_per_capita', 'type': Numeric(15, 2)},
    "CLUSTER ALVO PIB": {'new_name': 'cluster_alvo_pib', 'type': String(100)},
    "Inside Sales": {'new_name': 'inside_sales', 'type': String(50)}, 
    "TEM FRANQUIA?": {'new_name': 'tem_franquia', 'type': Integer}, # Tipo para Integer (0 ou 1 ou outro número)
    "TEM MULTIMARCAS?": {'new_name': 'tem_multimarcas', 'type': Integer}, # Tipo para Integer (0 ou 1 ou outro número)
    "ESTÁ NO FUNIL?": {'new_name': 'esta_no_funil', 'type': Integer}, # Tipo para Integer (0 ou 1 ou outro número)
    # Exemplo: Se 'VALIDAÇÃO - FRAN.' existir no seu Excel, adicione:
    # "VALIDAÇÃO - FRAN.": {'new_name': 'validacao_franquia', 'type': String(10)}, 
    # E assim por diante para outras colunas.
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
        return f'VARCHAR({sql_alchemy_type_obj.length})'
    elif isinstance(sql_alchemy_type_obj, Numeric):
        return f'DECIMAL({sql_alchemy_type_obj.precision}, {sql_alchemy_type_obj.scale})'
    elif isinstance(sql_alchemy_type_obj, Float):
        return 'FLOAT'
    elif isinstance(sql_alchemy_type_obj, Text):
        return 'TEXT'
    return str(sql_alchemy_type_obj)

DF_TO_SQL_DTYPES = {
    col_info['new_name']: get_sql_type_string(col_info['type']) 
    for col_info in COLUMN_MAPPING_AND_TYPES.values()
}


# --- Seção 2: Funções de Transformação e Limpeza ---

def limpar_moedas(valor):
    """
    Limpa strings de moeda (ex: 'R$ 1.234,56', '1.234,56') e as converte para float.
    Lida com valores NaN e já numéricos.
    """
    if pd.isna(valor):
        return None
    
    if isinstance(valor, (int, float)):
        return float(valor)

    s = str(valor)
    s = s.replace('R$', '').strip()

    if ',' in s and s.count(',') == 1 and s.count('.') >= 1:
        parts = s.split(',')
        integer_part = parts[0].replace('.', '')
        decimal_part = parts[1]
        s = f"{integer_part}.{decimal_part}"
    elif ',' in s and s.count('.') == 0:
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
    df = None 
    connection = None
    cursor = None
    df_ordered = pd.DataFrame() # Inicializa df_ordered para evitar NameError
    data_to_insert = [] # Inicializa data_to_insert para evitar NameError
    
    try:
        print(f"--- Iniciando ETL de Oportunidades ---")

        # 1. Extração
        print(f"\n1. Extraindo dados do arquivo '{EXCEL_FILE}', aba '{EXCEL_SHEET_NAME}'...")
        try:
            df_raw = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
            print(f"Dados extraídos com sucesso! {df_raw.shape[0]} linhas e {df_raw.shape[1]} colunas.")
            print(f"Colunas ENCONTRADAS no Excel: {df_raw.columns.tolist()}")
        except FileNotFoundError:
            print(f"Erro: Arquivo '{EXCEL_FILE}' não encontrado. Verifique o caminho e o nome do arquivo.")
            return
        except Exception as e:
            print(f"Erro ao ler o arquivo Excel: {e}")
            return
        
        df = df_raw.copy()
        
        if df is None or df.empty:
            print("Aviso: DataFrame está vazio ou não pôde ser carregado após a extração. Nenhuma linha será processada.")
            return

        print("\n2. Iniciando transformações...")

        # 2.1 Seleção e renomeação de colunas
        columns_to_select = {}
        for excel_col, config in COLUMN_MAPPING_AND_TYPES.items():
            cleaned_excel_col = excel_col.strip() 
            if cleaned_excel_col in df.columns:
                columns_to_select[cleaned_excel_col] = config['new_name']
            else:
                print(f"ERRO CRÍTICO: A coluna '{excel_col}' (esperada para mapear para '{config['new_name']}') NÃO FOI ENCONTRADA no seu arquivo Excel.")
                print(f"Por favor, verifique se o nome '{excel_col}' no dicionário COLUMN_MAPPING_AND_TYPES corresponde EXATAMENTE a uma das colunas impressas acima (Colunas ENCONTRADAS no Excel).")
        
        if not columns_to_select:
            print("Erro: Nenhuma coluna do mapeamento foi encontrada no Excel. Verifique os nomes das colunas e o arquivo Excel.")
            return

        df = df[list(columns_to_select.keys())].rename(columns=columns_to_select)
        print(f"Colunas selecionadas e renomeadas. DataFrame agora tem {df.shape[1]} colunas.")

        if df.empty:
            print("Aviso: DataFrame está vazio após a seleção e renomeação de colunas. Nenhuma linha será carregada para o MySQL.")
            return

        # 2.2 Limpeza de colunas numéricas (incluindo PIB per Capita)
        numeric_cols_to_clean = ['pib_per_capita', 'populacao', 'codigo_ibge']
        for col in numeric_cols_to_clean:
            if col in df.columns:
                if col == 'pib_per_capita':
                    df[col] = df[col].apply(limpar_moedas)
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                print(f"Coluna '{col}' limpa e convertida para numérico.")
        
        if 'pib_per_capita' in df.columns:
            soma_pib = df['pib_per_capita'].sum()
            print(f"Soma do PIB (após limpeza): R$ {soma_pib:,.2f}")
        
        # 2.3 Conversão de strings booleanas para valores consistentes (0/1 para Integer ou manter outros números)
        boolean_to_int_cols = ['tem_franquia', 'tem_multimarcas', 'esta_no_funil']
        other_boolean_like_cols = ['inside_sales'] 
        
        for col in boolean_to_int_cols + other_boolean_like_cols:
            if col in df.columns:
                # Padroniza todos os valores para string em maiúsculas
                original_upper_str = df[col].astype(str).str.upper()

                # Crie uma nova série para os valores resultantes
                converted_values = pd.Series(index=df.index, dtype='object') # Usar 'object' para permitir misturar int e floats inicialmente

                if col in boolean_to_int_cols:
                    # Aplica as conversões para 1 (Sim)
                    is_sim_like = original_upper_str.isin(['SIM', 'TRUE', '1'])
                    converted_values.loc[is_sim_like] = 1
                    
                    # Aplica as conversões para 0 (Não)
                    is_nao_like = original_upper_str.isin(['NÃO', 'FALSE', '0', '']) # Inclui strings vazias como 'Não'
                    converted_values.loc[is_nao_like] = 0

                    # Para os valores que não foram explicitamente 'SIM'/'NÃO' ou '1'/'0' ou '',
                    # tenta converter para numérico. Isso pegará o '3' e outros números.
                    remaining_indices = converted_values.isna()
                    converted_values.loc[remaining_indices] = pd.to_numeric(original_upper_str.loc[remaining_indices], errors='coerce')
                    
                    # Preenche quaisquer NaNs restantes (que não puderam ser convertidos para numérico) com 0
                    df[col] = converted_values.fillna(0).astype(int)
                    print(f"Coluna '{col}' convertida para 0/1 (Integer) ou manteve valor numérico original.")

                else: # Para other_boolean_like_cols (como 'inside_sales') que permanecem como String
                    # Para 'inside_sales', ainda padronizamos para 'Sim'/'Não' em formato de texto.
                    df[col] = original_upper_str.replace({
                        'SIM': 'Sim', 'TRUE': 'Sim', '1': 'Sim',
                        'NÃO': 'Não', 'FALSE': 'Não', '0': 'Não', '': 'Não',
                    }).fillna('Não')
                    print(f"Coluna '{col}' padronizada para 'Sim'/'Não'.")


        # 2.4 Remove registros com 'codigo_ibge' nulo (crítico para integridade)
        if 'codigo_ibge' in df.columns:
            initial_rows = df.shape[0]
            df.dropna(subset=['codigo_ibge'], inplace=True)
            if df.shape[0] < initial_rows:
                print(f"{initial_rows - df.shape[0]} linhas removidas devido a 'codigo_ibge' nulo.")
            else:
                print("Nenhuma linha removida por 'codigo_ibge' nulo.")

        if df.empty:
            print("Aviso: DataFrame está vazio após a remoção de linhas com 'codigo_ibge' nulo. Nenhuma linha será carregada para o MySQL.")
            return

        # 2.5 Adiciona timestamp de carga
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
        for col_name in df.columns: 
            sql_type_found = None
            for original_excel_col, col_config in COLUMN_MAPPING_AND_TYPES.items():
                if col_config['new_name'] == col_name:
                    sql_type_found = col_config['type']
                    break
            
            if sql_type_found:
                sql_type = sql_type_found
            elif col_name == 'data_carga_dw':
                sql_type = DateTime
            else:
                print(f"Aviso: Tipo para a coluna '{col_name}' não encontrado no mapeamento ou é uma coluna inesperada. Usando String(255) como padrão para DDL.")
                sql_type = String(255)

            table_columns.append(Column(col_name, sql_type, nullable=True))

        table_obj = Table(STAGING_TABLE_NAME, metadata,
                            *table_columns,
                            mysql_engine='InnoDB', 
                            mysql_charset='utf8mb4' 
                           )
        
        create_table_sql = str(CreateTable(table_obj).compile(engine))
        
        cursor.execute(create_table_sql)
        connection.commit() 
        print(f"Tabela '{STAGING_TABLE_NAME}' criada explicitamente no MySQL.")

        # Passo 3.3: Carregamento de dados
        print("Iniciando carregamento de dados via inserção manual com cursor.executemany...")

        final_column_order_for_insert = []
        for col_config in COLUMN_MAPPING_AND_TYPES.values():
            if col_config['new_name'] in df.columns: 
                final_column_order_for_insert.append(col_config['new_name'])
        
        if 'data_carga_dw' in df.columns and 'data_carga_dw' not in final_column_order_for_insert:
            final_column_order_for_insert.append('data_carga_dw')

        if not final_column_order_for_insert:
            print("Aviso: Nenhuma coluna foi determinada para inserção. Verifique o mapeamento e o DataFrame 'df'.")
            return

        print(f"DEBUG: df.empty antes de df_ordered assignment? {df.empty}")
        print(f"DEBUG: Colunas em df: {df.columns.tolist()}")
        print(f"DEBUG: Colunas finais para inserção (final_column_order_for_insert): {final_column_order_for_insert}")

        if df.empty:
            print("Aviso: DataFrame 'df' está vazio antes de tentar criar 'df_ordered'. Nenhuma linha será inserida.")
            return
        
        df_ordered = df[final_column_order_for_insert] 
        
        print(f"DEBUG: df_ordered criado com sucesso. Shape: {df_ordered.shape}")

        if df_ordered.empty:
            print("Aviso: DataFrame final está vazio após transformações e reordenação. Nenhuma linha será inserida.")
            return

        data_to_insert = [] 
        for index, row in df_ordered.iterrows():
            processed_row = []
            for col_name in final_column_order_for_insert:
                val = row[col_name]
                if pd.api.types.is_datetime64_any_dtype(df_ordered[col_name]) and pd.notna(val):
                    processed_row.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                elif pd.isna(val): 
                    processed_row.append(None)
                else:
                    processed_row.append(val)
            data_to_insert.append(tuple(processed_row))

        columns_sql = ", ".join([f"`{col}`" for col in final_column_order_for_insert]) 
        placeholders = ", ".join(["%s"] * len(final_column_order_for_insert)) 
        
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
        except Exception as insert_e:
            print(f"Erro ao inserir dados via cursor.executemany: {insert_e}")
            connection.rollback() 
            raise 


        print(f"--- ETL Concluído com Sucesso! ---")

    except Exception as e:
        print(f"Erro fatal durante o ETL: {e}")
        traceback.print_exc() 
    finally:
        if cursor:
            cursor.close()
            print("Cursor do banco de dados fechado.")
        if connection:
            connection.close()
            print("Conexão com o banco de dados fechada.")

# --- Execução do ETL ---
if __name__ == "__main__":
    run_etl()
