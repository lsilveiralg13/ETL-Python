import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, BigInteger, String, Numeric, Float, Text # Importa tipos SQLAlchemy para DDL
from sqlalchemy.schema import Table, Column, MetaData, CreateTable # Importações para DDL explícito
import os # Importar para usar variáveis de ambiente
import re # Importar para a função limpar_numero
import warnings
import locale

# Ignora UserWarnings do pandas que podem ocorrer com o to_sql
warnings.filterwarnings('ignore', category=UserWarning)

# --- Seção 1: Configurações ---
# Detalhes do arquivo Excel
EXCEL_FILE = "PEDIDOS DE VENDA ETL.xlsx"
EXCEL_SHEET_NAME = 'Planilha1' # Nome da aba do Excel

# Detalhes do Banco de Dados MySQL
DB_USER = os.getenv('DB_USER', 'root') # Usa variáveis de ambiente, com fallback para 'root'
DB_PASSWORD = os.getenv('DB_PASSWORD', 'root') 
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '3306')) # Converte a porta para int
DB_NAME = 'faturamento_multimarcas_dw'
STAGING_TABLE_NAME = 'staging_pedidos_venda_multimarcas' # Nome da tabela para este ETL

# Mapeamento de colunas do Excel para o MySQL e seus tipos de dados SQLAlchemy
# Isso será usado tanto para renomear colunas quanto para criar a tabela no MySQL
COLUMN_MAPPING_AND_TYPES = {
    "Tipo Operação": {"new_name": "tipo_operacao", "type": String(50)},
    "Confirmada": {"new_name": "confirmado", "type": String(50)},
    "Pendente": {"new_name": "pendente", "type": String(50)},
    "CONFIRMADO_STATUS": {"new_name": "confirmado_status", "type": String(50)}, # Ajustado para snake_case
    "Liberação": {"new_name": "liberacao", "type": String(50)},
    "Situação WMS": {"new_name": "situacao_wms", "type": String(100)},
    "Status WMS": {"new_name": "status_wms", "type": String(100)},
    "Nro. Único": {"new_name": "nro_unico", "type": BigInteger},
    "Dt. Neg.": {"new_name": "data_negociacao", "type": DateTime},
    "CHAVE_MMM": {"new_name": "chave_mmm", "type": String(50)},
    "CHAVE_AAA": {"new_name": "chave_aaa", "type": Integer},
    "Parceiro": {"new_name": "codigo_parceiro", "type": BigInteger},
    "Nome Parceiro (Parceiro)": {"new_name": "nome_parceiro", "type": String(255)},
    "Qtd. Itens": {"new_name": "quantidade_itens", "type": Integer},
    "Vlr. Total": {"new_name": "valor_total", "type": Numeric(15, 2)},
    "Apelido (Vendedor)": {"new_name": "apelido_vendedor", "type": String(100)},
    "Descrição (Tipo de Negociação)": {"new_name": "tipo_negociacao_descricao", "type": String(255)},
    "Cep Parceiro": {"new_name": "cep_parceiro", "type": String(10)}, # CEP pode ter formato específico, manter como string
    "CIDADE": {"new_name": "cidade_parceiro", "type": String(100)},
    "ESTADO": {"new_name": "estado_parceiro", "type": String(2)},
    "Dt. do Movimento": {"new_name": "data_movimento", "type": DateTime},
    "Tipo Negociação": {"new_name": "tipo_negociacao_id", "type": String(50)},
    "Descrição (Tipo de Operação)": {"new_name": "tipo_operacao_descricao", "type": String(255)},
    "Nome Parceiro (Transportadora)": {"new_name": "nome_transportadora", "type": String(255)},
    "Nome Fantasia (Empresa)": {"new_name": "nome_fantasia_empresa", "type": String(255)},
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

def limpar_numero_decimal(valor):
    """
    Limpa strings de números decimais (removendo não-dígitos/pontos, trocando vírgula por ponto)
    e as converte para float. Retorna pd.NA para valores inválidos ou NaN.
    """
    if pd.isna(valor):
        return pd.NA
    if isinstance(valor, (int, float)):
        return float(valor)

    s = str(valor).strip()
    s = s.replace('.', '').replace(',', '.') # Remove separador de milhar e ajusta decimal
    s = re.sub(r'[^\d.]', '', s) # Remove tudo que não for dígito ou ponto decimal
    
    try:
        if s and s.count('.') <= 1: # Garante que não é string vazia e tem no máximo um ponto decimal
            return float(s)
        else:
            return pd.NA
    except ValueError:
        print(f"Aviso: Não foi possível converter o valor '{str(valor)[:50]}' para número decimal. Retornando NA.")
        return pd.NA

# --- Seção 3: Pipeline ETL ---

def run_etl_pedidos_venda(excel_file_path=EXCEL_FILE):
    """
    Executa o pipeline ETL completo para Pedidos de Venda: Extração, Transformação e Carga.
    """
    df = None 
    connection = None
    cursor = None
    
    try:
        print(f"--- Iniciando ETL de Pedidos de Venda ---")

        # 1. Extração
        print(f"\n1. Extraindo dados do arquivo '{excel_file_path}', aba '{EXCEL_SHEET_NAME}'...")
        try:
            # usecols com as chaves originais para otimizar a leitura
            df_raw = pd.read_excel(excel_file_path, sheet_name=EXCEL_SHEET_NAME, usecols=list(COLUMN_MAPPING_AND_TYPES.keys()))
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

        print("\n2. Iniciando transformações para Pedidos de Venda...")

        df = df_raw.copy()

        # 2.1 Seleção e renomeação de colunas
        # Prepara um dicionário com apenas as colunas que realmente existem no df_raw e mapeia para os novos nomes
        columns_to_select_and_rename = {
            excel_col: config['new_name'] 
            for excel_col, config in COLUMN_MAPPING_AND_TYPES.items() 
            if excel_col in df.columns
        }
        
        # Verifica e notifica sobre colunas esperadas que não foram encontradas no Excel
        missing_excel_cols = [col for col in COLUMN_MAPPING_AND_TYPES.keys() if col not in df.columns and col != "mes_negociacao_pt_br" and col != "data_carga_dw"] # Ignora cols derivadas
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

        # 2.2 Limpeza e Conversão de Colunas Numéricas (inteiros)
        int_cols = ['quantidade_itens', 'chave_aaa'] # Adicionado 'chave_aaa'
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].apply(limpar_numero_int)
                # O astype('Int64') é importante para permitir NaNs em colunas de inteiros
                df[col] = df[col].astype('Int64') 
                print(f"Coluna '{col}' limpa e convertida para Int64.")
            else:
                print(f"Aviso: Coluna numérica inteira '{col}' não encontrada no DataFrame para limpeza.")

        # 2.3 Limpeza e Conversão de Colunas Numéricas (decimais)
        decimal_cols = ['valor_total'] # Assumindo 'desconto_total_item' não está no mapeamento
        for col in decimal_cols:
            if col in df.columns:
                df[col] = df[col].apply(limpar_numero_decimal)
                # Preenche NaN com 0.0 para valores monetários se for a regra de negócio
                df[col] = df[col].fillna(0.0) 
                print(f"Coluna '{col}' limpa e convertida para decimal.")
            else:
                print(f"Aviso: Coluna numérica decimal '{col}' não encontrada no DataFrame para limpeza.")

        # 2.4 Conversão de datas
        date_cols = ['data_negociacao', 'data_movimento']
        for col in date_cols:
            if col in df.columns:
                # Converte para datetime primeiro para extração de componentes
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Adiciona coluna 'mes_negociacao_pt_br' AQUI, ENQUANTO É DATETIME
                if col == 'data_negociacao': 
                    try:
                        locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')  # Para sistemas Linux/Mac
                    except locale.Error:
                        try:
                            locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')  # Para sistemas Windows
                        except locale.Error:
                            print("Locale 'pt_BR' or 'Portuguese_Brazil.1252' not found. Month names might be in English.")
                    df['mes_negociacao_pt_br'] = df[col].dt.strftime('%B').str.upper()
                    print("Coluna 'mes_negociacao_pt_br' criada.")

                # Converte para date (apenas a parte da data, sem hora) no DataFrame
                # O MySQL DateTime irá aceitar, mas é importante para a consistência no DataFrame.
                df[col] = df[col].dt.date 
                print(f"Coluna '{col}' convertida para formato de data.")
            else:
                print(f"Aviso: Coluna de data '{col}' não encontrada no DataFrame.")

        # 2.5 Tratamento de IDs (nro_unico, codigo_parceiro - caso não sejam limpos por limpar_numero_int)
        # Assumimos que 'limpar_numero_int' ou a conversão de tipo já os tratou,
        # mas se forem strings com caracteres especiais, podemos limpar novamente.
        id_cols_to_clean = ['nro_unico', 'codigo_parceiro']
        for col in id_cols_to_clean:
            if col in df.columns:
                # Garante que é string, remove não-alfanuméricos, e substitui vazio por None (para NULL no DB)
                df[col] = df[col].astype(str).str.replace(r'[^\w\s-]', '', regex=True).replace('', None)
                print(f"Coluna ID '{col}' tratada para caracteres especiais.")

        # 2.6 Tratamento de colunas de texto com valores NaN (para evitar que NaN se torne 'nan' string)
        # Percorre as colunas que são mapeadas para String/Text
        for col_excel, col_config in COLUMN_MAPPING_AND_TYPES.items():
            new_name = col_config['new_name']
            sql_type = col_config['type']
            if isinstance(sql_type, (String, Text)) and new_name in df.columns:
                if df[new_name].isnull().any() or df[new_name].dtype == 'object': # Verifica se há nulos ou é tipo objeto
                    df[new_name] = df[new_name].fillna('') # Preenche NaN/None com string vazia
                    df[new_name] = df[new_name].astype(str) # Garante que é tipo string
                    print(f"Coluna de texto '{new_name}' tratada para nulos e convertida para string.")

        # 2.7 Remover registros sem chaves essenciais para pedidos
        chaves_essenciais = ['nro_unico', 'data_negociacao', 'codigo_parceiro']
        chaves_existentes_para_dropna = [col for col in chaves_essenciais if col in df.columns]

        if chaves_existentes_para_dropna:
            initial_rows = df.shape[0]
            df.dropna(subset=chaves_existentes_para_dropna, inplace=True)
            if df.shape[0] < initial_rows:
                print(f"Removidas {initial_rows - df.shape[0]} linhas com chaves essenciais nulas. Total de {df.shape[0]} linhas restantes.")
            else:
                print("Nenhuma linha removida por chaves essenciais nulas.")
        else:
            print("Nenhuma chave essencial encontrada no DataFrame para remoção de nulos.")

        # 2.8 Timestamp de carga
        df['data_carga_dw'] = pd.Timestamp.now()
        print(f"Coluna 'data_carga_dw' adicionada com o timestamp atual: {df['data_carga_dw'].iloc[0]}")

        print("\nTransformações Concluídas com sucesso para Pedidos de Venda.")
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
            else: # Fallback para colunas que não estejam no mapeamento inicial (ex: mes_negociacao_pt_br, data_carga_dw)
                if col_name == 'data_carga_dw':
                    sql_type = DateTime
                elif col_name == 'mes_negociacao_pt_br':
                    sql_type = String(20) # Mês em PT-BR
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
    run_etl_pedidos_venda()

    