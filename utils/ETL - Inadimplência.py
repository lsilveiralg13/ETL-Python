import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, BigInteger, String, Numeric, Float, Text # Importa tipos específicos do SQLAlchemy para o DDL
from sqlalchemy.schema import Table, Column, MetaData, CreateTable # Importações para criar o DDL explícito
from datetime import datetime

# --- Seção 1: Configurações ---
# Detalhes do arquivo Excel
EXCEL_FILE = "INADIMPLENCIA ETL.xlsx"
EXCEL_SHEET_NAME = "BASE"

# Detalhes do Banco de Dados MySQL
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_inadimplencia_multimarcas' # Nome da tabela ajustado para refletir os dados de parceiros

# Mapeamento de colunas do Excel para o MySQL e seus tipos de dados SQLAlchemy
COLUMN_MAPPING_AND_TYPES = {
    'Descrição (Banco)': {'new_name': 'descricao_banco', 'type': String(255)},
    'Dt. Entrada e Saída': {'new_name': 'data_entrada_saida', 'type': DateTime},
    'Nro Único': {'new_name': 'numero_unico', 'type': String(50)},
    'Parceiro': {'new_name': 'codigo_parceiro', 'type': String(255)},
    'Nome Parceiro (Parceiro)': {'new_name': 'nome_parceiro', 'type': String(255)},
    'CNPJ / CPF (Parceiro)': {'new_name': 'cnpj_cpf', 'type': String(20)},
    'Nro Nota': {'new_name': 'numero_nota', 'type': String(50)},
    'Dt. Neg.': {'new_name': 'data_negociacao', 'type': DateTime},
    'Dt. Venc.': {'new_name': 'data_vencimento', 'type': DateTime},
    'Vlr Bruto': {'new_name': 'valor_bruto', 'type': Numeric(15, 2)},
    'Valor Líq.': {'new_name': 'valor_liquido', 'type': Numeric(15, 2)},
    'Atraso (dias)': {'new_name': 'dias_atraso', 'type': Integer},
    'Histórico': {'new_name': 'historico', 'type': Text},
    'Apelido': {'new_name': 'vendedor', 'type': String(100)},
    # Coluna que será adicionada durante a transformação
    'data_carga_dw': {'new_name': 'data_carga_dw', 'type': DateTime}
}

# Helper para converter objetos de tipo SQLAlchemy para strings de tipo SQL
def get_sql_type_string(sql_alchemy_type_obj):
    """Converte um objeto de tipo SQLAlchemy para a string de tipo SQL correspondente para MySQL."""
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

# --- Seção 2: Pipeline ETL ---

def run_etl():
    """
    Executa o pipeline ETL completo para carregar dados financeiros para o MySQL.
    """
    df = None
    connection = None
    cursor = None

    try:
        print(f"--- Iniciando ETL de Dados Financeiros para o Data Warehouse ---")

        # 1. Extração
        print(f"\n1. Extraindo dados do arquivo '{EXCEL_FILE}', aba '{EXCEL_SHEET_NAME}'...")
        try:
            # Tenta ler o arquivo Excel
            df_raw = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET_NAME)
            print(f"Dados extraídos com sucesso! {df_raw.shape[0]} linhas e {df_raw.shape[1]} colunas.")
        except FileNotFoundError:
            print(f"Erro: Arquivo '{EXCEL_FILE}' não encontrado. Verifique o caminho e o nome do arquivo.")
            return
        except Exception as e:
            print(f"Erro ao ler o arquivo Excel: {e}")
            return

        # Cria uma cópia para as transformações
        df = df_raw.copy()

        print("\n2. Iniciando transformações...")

        # 2.1 Seleção e renomeação de colunas
        columns_to_select = {excel_col: config['new_name'] for excel_col, config in COLUMN_MAPPING_AND_TYPES.items() if excel_col in df.columns}
        
        missing_excel_cols = [col for col in columns_to_select.keys() if col not in df.columns]
        if missing_excel_cols:
            print(f"Atenção: As seguintes colunas não foram encontradas no Excel e serão ignoradas: {missing_excel_cols}")
            for col in missing_excel_cols:
                del columns_to_select[col]

        df = df[list(columns_to_select.keys())].rename(columns=columns_to_select)
        print(f"Colunas selecionadas e renomeadas. DataFrame agora tem {df.shape[1]} colunas.")

        # 2.2 Conversão de tipos de dados
        # Converte as colunas para datetime
        date_cols = ['data_entrada_saida', 'data_negociacao', 'data_vencimento']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                print(f"Coluna '{col}' convertida para datetime.")
        
        # Converte colunas numéricas
        numeric_cols = ['valor_bruto', 'valor_liquido', 'dias_atraso']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                print(f"Coluna '{col}' convertida para numérico.")

        # 2.3 Adiciona timestamp de carga
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
        print(f"Tentando criar engine para o banco de dados: mysql+pymymysql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        engine = create_engine(mysql_connection_string)

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

        # Usar as colunas do DataFrame final para garantir a ordem
        table_columns = []
        for col_name in df.columns:
            # Encontra a configuração de tipo para a coluna renomeada
            original_col_info = next((v for k, v in COLUMN_MAPPING_AND_TYPES.items() if v['new_name'] == col_name), None)
            
            if original_col_info:
                sql_type = original_col_info['type']
            else:
                if col_name == 'data_carga_dw':
                    sql_type = DateTime
                else:
                    print(f"Aviso: Tipo para a coluna '{col_name}' não encontrado no mapeamento. Usando String(255) como padrão para DDL.")
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
        
        # CORREÇÃO: Usar a ordem das colunas do DataFrame final para garantir a correspondência
        column_order_for_insert = list(df.columns)
        
        data_to_insert = []
        for index, row in df.iterrows():
            processed_row = []
            for col_name in column_order_for_insert:
                val = row[col_name]
                if pd.api.types.is_datetime64_any_dtype(df[col_name]) and pd.notna(val):
                    processed_row.append(val.to_pydatetime())
                elif pd.isna(val):
                    processed_row.append(None)
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
        except Exception as insert_e:
            print(f"Erro ao inserir dados via cursor.executemany: {insert_e}")
            connection.rollback()
            raise


        print(f"--- ETL Concluído com Sucesso! ---")

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
    run_etl()
