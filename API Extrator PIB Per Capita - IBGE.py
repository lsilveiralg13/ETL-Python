import pandas as pd
import sidrapy as sidra
import requests

def process_sidra_dataframe_for_pib(raw_df, descriptive_col_mapping, output_col_names):
    """
    Processa um DataFrame bruto retornado pelo sidrapy para dados de PIB,
    definindo a primeira linha como cabeçalho, selecionando e renomeando colunas específicas.

    Args:
        raw_df (pd.DataFrame): O DataFrame bruto retornado por sidra.get_table().
        descriptive_col_mapping (dict): Um dicionário mapeando os nomes descritivos das colunas
                                        (encontrados na primeira linha do raw_df) para os nomes
                                        desejados no DataFrame de saída.
        output_col_names (list): A ordem final das colunas no DataFrame de saída.

    Returns:
        pd.DataFrame: O DataFrame processado com as colunas renomeadas e dados limpos.
    """
    if raw_df.empty:
        print("DataFrame bruto está vazio. Retornando DataFrame vazio.")
        return pd.DataFrame(columns=output_col_names)

    df = raw_df.copy()

    # Define a primeira linha (cabeçalhos descritivos) como as colunas do DataFrame
    df.columns = df.iloc[0]
    # Remove a primeira linha (agora o cabeçalho) do DataFrame de dados
    df = df[1:].copy()

    processed_df_data = {}
    missing_cols_for_processing = []
    for desc_name, output_name in descriptive_col_mapping.items():
        if desc_name in df.columns:
            processed_df_data[output_name] = df[desc_name]
        else:
            missing_cols_for_processing.append(desc_name)
            processed_df_data[output_name] = None 

    if missing_cols_for_processing:
        print(f"Aviso: Colunas descritivas esperadas {missing_cols_for_processing} não encontradas no DataFrame. Colunas disponíveis (após definir cabeçalho): {df.columns.tolist()}")
        if any(col in missing_cols_for_processing for col in ['Município (Código)']):
            print("Erro crítico: Coluna essencial para processamento não encontrada. Retornando DataFrame vazio.")
            return pd.DataFrame(columns=output_col_names)

    processed_df = pd.DataFrame(processed_df_data)

    for col in output_col_names:
        if col not in processed_df.columns:
            processed_df[col] = None 

    return processed_df[output_col_names]


def extract_pib_per_municipio_to_excel(output_filename="pib_municipios.xlsx"):
    """
    Extrai dados de PIB (per capita) por município do IBGE
    e os salva em um arquivo Excel.
    """
    print("Iniciando a extração de dados de PIB por município do IBGE...")

    # --- 1. Obter todos os municípios e seus respectivos estados via API de localidades ---
    municipios_uf_data = []
    mun_api_url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    
    try:
        print(f"Obtendo códigos e siglas dos municípios e seus estados via API de localidades...")
        response_mun = requests.get(mun_api_url)
        response_mun.raise_for_status()
        municipios_raw = response_mun.json()

        for municipio in municipios_raw:
            mun_id = str(municipio.get('id'))
            mun_nome = municipio.get('nome')
            
            microrregiao = municipio.get('microrregiao')
            mesorregiao_info = None
            uf_info = None
            uf_sigla = None

            if microrregiao and isinstance(microrregiao, dict):
                mesorregiao_info = microrregiao.get('mesorregiao')
                if mesorregiao_info and isinstance(mesorregiao_info, dict):
                    uf_info = mesorregiao_info.get('UF') 
                    if uf_info and isinstance(uf_info, dict):
                        uf_sigla = uf_info.get('sigla')

            municipios_uf_data.append({
                'Codigo_Municipio': mun_id,
                'Nome_Municipio_Original': mun_nome,
                'Estado': uf_sigla
            })
        df_municipios_uf = pd.DataFrame(municipios_uf_data)
        df_municipios_uf['Codigo_Municipio'] = df_municipios_uf['Codigo_Municipio'].astype(str)
        print(f"Mapeamento de {len(df_municipios_uf)} municípios para UF obtido com sucesso.")
        print(df_municipios_uf.head())

    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição à API de localidades (municípios): {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Erro inesperado ao processar mapeamento Município-UF: {e}")
        return pd.DataFrame()


    # --- 2. Extrair PIB per Capita em blocos de municípios (usando Tabela 5938) ---
    all_pib_data = []
    municipio_codes = df_municipios_uf['Codigo_Municipio'].tolist()
    chunk_size = 50 # Mantendo o tamanho do bloco pequeno para segurança

    pib_per_capita_variable_code = '6605' # Variável para PIB per capita (da Tabela 5938)
    column_name_for_pib = 'PIB_per_Capita'

    pib_col_mapping = {
        'Município (Código)': 'Codigo_Municipio',
        'Município': 'Nome_Municipio',
        'Valor': 'PIB_per_Capita' # Coluna 'Valor' é o padrão para dados sidra
    }
    pib_output_cols = ['Codigo_Municipio', 'Nome_Municipio', 'PIB_per_Capita']

    print(f"\nExtraindo dados de PIB per Capita (Tabela 5938, variável {pib_per_capita_variable_code}) em blocos de {chunk_size} municípios...")
    
    for i in range(0, len(municipio_codes), chunk_size):
        chunk_of_codes = municipio_codes[i:i + chunk_size]
        codes_str = ','.join(chunk_of_codes)
        
        print(f"Extraindo PIB para o bloco {int(i/chunk_size) + 1} ({len(chunk_of_codes)} municípios)...")
        try:
            raw_df_pib_chunk = sidra.get_table(
                table_code='5938', # Revertido para Tabela 5938
                territorial_level='6', # Nível de Município
                ibge_territorial_code=codes_str, # Passa os códigos específicos do chunk
                variables=pib_per_capita_variable_code, # Variável PIB per capita
                last_result='yes'
            )
            
            df_pib_chunk = process_sidra_dataframe_for_pib(raw_df_pib_chunk, pib_col_mapping, pib_output_cols)

            if not df_pib_chunk.empty:
                all_pib_data.append(df_pib_chunk)
                print(f"Dados de PIB para o bloco extraídos com sucesso.")
            else:
                print(f"DataFrame de PIB vazio para o bloco atual.")
        except Exception as e:
            print(f"Erro ao extrair PIB para o bloco atual: {e}")
            # Não interrompe aqui, tenta todos os blocos mesmo que um falhe, para coletar o máximo possível
    
    if all_pib_data:
        df_pib_consolidated = pd.concat(all_pib_data, ignore_index=True)
        # --- Adição da remoção de duplicatas ---
        df_pib_consolidated = df_pib_consolidated.drop_duplicates(subset=['Codigo_Municipio'], keep='first')
        print("Duplicatas removidas do DataFrame de PIB consolidado.")
        # --- Fim da adição da remoção de duplicatas ---
        print("Todos os dados de PIB por bloco concatenados com sucesso. Primeiras 5 linhas:")
        print(df_pib_consolidated.head())
    else:
        print("Nenhum dado de PIB foi extraído após a iteração por blocos.")
        return pd.DataFrame() # Retorna vazio se não houver dados de PIB


    # --- 3. Combinar PIB e Mapeamento UF ---
    df_final = pd.merge(df_pib_consolidated, df_municipios_uf, on='Codigo_Municipio', how='left')
    print("Dados de PIB e UF combinados com sucesso.")
    print(df_final.head())


    if not df_final.empty:
        try:
            # Converte os valores para tipo numérico antes de aplicar validação
            if column_name_for_pib in df_final.columns:
                df_final[column_name_for_pib] = df_final[column_name_for_pib].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_final[column_name_for_pib] = pd.to_numeric(df_final[column_name_for_pib], errors='coerce')

            # --- Adicionar Validadores ---
            df_final['Status_Validacao'] = 'Dados Válidos'
            if column_name_for_pib in df_final.columns:
                pib_invalid_mask = df_final[column_name_for_pib].isna() | (df_final[column_name_for_pib] < 0)
                df_final.loc[pib_invalid_mask, 'Status_Validacao'] = f'{column_name_for_pib} Inválido'
            # --- Fim Adicionar Validadores ---

            # --- Renomear Cidades Duplicadas Condicionalmente ---
            if 'Nome_Municipio_Original' in df_final.columns and 'Estado' in df_final.columns:
                city_state_counts = df_final.groupby(['Nome_Municipio_Original', 'Estado']).size().reset_index(name='count')
                duplicated_city_names = city_state_counts.groupby('Nome_Municipio_Original')['Estado'].nunique()
                cities_to_rename_with_uf = duplicated_city_names[duplicated_city_names > 1].index.tolist()

                df_final['Nome_Municipio'] = df_final.apply(
                    lambda row: f"{row['Nome_Municipio_Original']} ({row['Estado']})" 
                                if row['Nome_Municipio_Original'] in cities_to_rename_with_uf and pd.notna(row['Estado'])
                                else row['Nome_Municipio_Original'],
                    axis=1
                )
            else:
                df_final['Nome_Municipio'] = df_final.get('Nome_Municipio_Original', df_final['Codigo_Municipio'])

            df_final.drop(columns=['Nome_Municipio_Original'], errors='ignore', inplace=True)
            df_final.drop(columns=['Nome_Municipio_Final'], errors='ignore', inplace=True)
            # --- Fim Renomear Cidades Duplicadas Condicionalmente ---


            # Define a ordem final das colunas para o arquivo Excel
            final_columns_order = [
                'Codigo_Municipio',
                'Nome_Municipio',
                'Estado', 
                column_name_for_pib, # Nome da coluna PIB (será 'PIB_per_Capita')
                'Status_Validacao'
            ]
            final_columns_present = [col for col in final_columns_order if col in df_final.columns]
            df_final = df_final[final_columns_present]

            print("Todos os dados processados e prontos para exportação. Primeiras 5 linhas:")
            print(df_final.head())

            df_final.to_excel(output_filename, index=False)
            print(f"Dados exportados com sucesso para '{output_filename}'")
        except Exception as e:
            print(f"Erro ao exportar dados para Excel: {e}")
    else:
        print("Nenhum dado para exportar para Excel.")

# Chamar a função para executar a extração
extract_pib_per_municipio_to_excel()









