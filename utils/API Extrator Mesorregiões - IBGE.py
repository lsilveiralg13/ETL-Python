import pandas as pd
import requests

def extract_mesoregion_data_to_excel(output_filename="mesorregioes_municipios.xlsx"):
    """
    Extrai dados de Mesorregião (código e nome) por município do IBGE
    utilizando a API direta de localidades e os salva em um arquivo Excel.
    """
    print("Iniciando a extração de dados de Mesorregião por município do IBGE via API direta...")

    mesoregion_data = []
    api_url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

    try:
        print(f"Fazendo requisição à API: {api_url}")
        response = requests.get(api_url)
        response.raise_for_status() # Lança um erro para status de erro HTTP (4xx ou 5xx)
        
        municipios = response.json()
        print(f"Dados brutos de {len(municipios)} municípios recebidos da API.")

        for municipio in municipios:
            mun_id = str(municipio.get('id'))
            mun_nome_completo = municipio.get('nome') # Nome já vem sem o estado anexado, ex: "Cariacica"
            
            # Acessa 'microrregiao' de forma segura
            microrregiao = municipio.get('microrregiao')
            mesorregiao_info = {}
            uf_sigla = None # Inicializa a sigla do estado como None

            if microrregiao and isinstance(microrregiao, dict):
                # Acessa 'mesorregiao' de forma segura dentro de 'microrregiao'
                mesorregiao_info = microrregiao.get('mesorregiao', {})
                
                # Acessa 'uf' e 'sigla' de forma segura
                uf_info = mesorregiao_info.get('UF') # A chave é 'UF' (maiúsculo)
                if uf_info and isinstance(uf_info, dict):
                    uf_sigla = uf_info.get('sigla') # A chave é 'sigla'

            meso_id = str(mesorregiao_info.get('id')) if mesorregiao_info.get('id') else None
            meso_nome = mesorregiao_info.get('nome') if mesorregiao_info.get('nome') else None

            mesoregion_data.append({
                'Codigo_Municipio': mun_id,
                'Nome_Municipio': mun_nome_completo, # Nome do município sem UF
                'Estado': uf_sigla, # Sigla do estado
                'Codigo_Mesorregiao': meso_id,
                'Nome_Mesorregiao': meso_nome
            })
        
        df_mesoregion = pd.DataFrame(mesoregion_data)

        if not df_mesoregion.empty:
            df_mesoregion['Codigo_Municipio'] = df_mesoregion['Codigo_Municipio'].astype(str)
            df_mesoregion = df_mesoregion.drop_duplicates(subset=['Codigo_Municipio'])
            print("Dados de Mesorregião extraídos e processados com sucesso. Primeiras 5 linhas:")
            print(df_mesoregion.head())

            # --- Renomear Cidades Duplicadas Condicionalmente ---
            # 'Nome_Municipio' já contém o nome puro da cidade
            # 'Estado' já contém a sigla do estado

            # Identifica municípios com nomes iguais em estados diferentes
            # Cria uma contagem de cada Nome_Municipio por Estado
            city_state_counts = df_mesoregion.groupby(['Nome_Municipio', 'Estado']).size().reset_index(name='count')
            # Identifica nomes de municípios que aparecem em múltiplos estados
            # Filtra apenas os 'Nome_Municipio' que aparecem em mais de um estado
            duplicated_city_names = city_state_counts.groupby('Nome_Municipio')['Estado'].nunique()
            cities_to_rename_with_uf = duplicated_city_names[duplicated_city_names > 1].index.tolist()

            # Aplica a renomeação condicional
            if 'Nome_Municipio' in df_mesoregion.columns and 'Estado' in df_mesoregion.columns:
                df_mesoregion['Nome_Municipio'] = df_mesoregion.apply(
                    lambda row: f"{row['Nome_Municipio']} ({row['Estado']})" 
                                if row['Nome_Municipio'] in cities_to_rename_with_uf and pd.notna(row['Estado'])
                                else row['Nome_Municipio'],
                    axis=1
                )
            # --- Fim Renomear Cidades Duplicadas Condicionalmente ---

        else:
            print("DataFrame de Mesorregião vazio após processamento. Nenhum dado de mesorregião foi retornado.")
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição à API do IBGE: {e}")
        df_mesoregion = pd.DataFrame(columns=['Codigo_Municipio', 'Nome_Municipio', 'Codigo_Mesorregiao', 'Nome_Mesorregiao', 'Estado'])
    except Exception as e:
        print(f"Erro inesperado ao processar dados de Mesorregião: {e}")
        df_mesoregion = pd.DataFrame(columns=['Codigo_Municipio', 'Nome_Municipio', 'Codigo_Mesorregiao', 'Nome_Mesorregiao', 'Estado'])

    # --- Exportar para Excel ---
    if not df_mesoregion.empty:
        try:
            for col in ['Codigo_Mesorregiao']: 
                if col in df_mesoregion.columns:
                    df_mesoregion[col] = df_mesoregion[col].fillna('').astype(str).str.replace('.0', '', regex=False)
            
            # Define a ordem final das colunas para o arquivo Excel
            final_columns_order = [
                'Codigo_Municipio',
                'Nome_Municipio', # Agora já contém o formato condicional
                'Estado', 
                'Codigo_Mesorregiao',
                'Nome_Mesorregiao'
            ]
            # Filtra e reordena as colunas que realmente existem no DataFrame final
            final_columns_present = [col for col in final_columns_order if col in df_mesoregion.columns]
            df_mesoregion = df_mesoregion[final_columns_present]

            df_mesoregion.to_excel(output_filename, index=False)
            print(f"Dados exportados com sucesso para '{output_filename}'")
        except Exception as e:
            print(f"Erro ao exportar dados para Excel: {e}")
    else:
        print("Nenhum dado para exportar para Excel.")

# Chamar a função para executar a extração
extract_mesoregion_data_to_excel()






