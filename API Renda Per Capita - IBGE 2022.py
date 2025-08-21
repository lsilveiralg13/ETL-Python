import pandas as pd
import sidrapy as sidra

def process_sidra_dataframe(raw_df, descriptive_col_mapping, output_col_names):
    if raw_df.empty:
        print("DataFrame bruto está vazio. Retornando DataFrame vazio.")
        return pd.DataFrame(columns=output_col_names)

    df = raw_df.copy()

    df.columns = df.iloc[0]
    df = df[1:].copy()

    processed_df_data = {}
    for desc_name, output_name in descriptive_col_mapping.items():
        if desc_name in df.columns:
            processed_df_data[output_name] = df[desc_name]
        else:
            if output_name not in ['Codigo_UF', 'Nome_UF', 'Populacao_UF_Ignored']:
                print(f"Aviso: Coluna descritiva esperada '{desc_name}' não encontrada no DataFrame. Esta coluna será ignorada.")
            processed_df_data[output_name] = None

    processed_df = pd.DataFrame(processed_df_data)

    for col in output_col_names:
        if col not in processed_df.columns:
            processed_df[col] = None 

    return processed_df[output_col_names]


def extract_ibge_data_to_excel(output_filename="dados_ibge_municipios.xlsx"):
    print("Iniciando a extração de dados do IBGE...")

    df_populacao = pd.DataFrame(columns=['Codigo_Municipio', 'Nome_Municipio', 'Populacao'])
    try:
        print(f"Extraindo dados de População (Tabela 6579) para o ano mais recente disponível...")
        raw_df_populacao = sidra.get_table(
            table_code='6579',
            territorial_level='6',
            ibge_territorial_code='all',
            variables='9340',
            last_result='yes'
        )
        
        pop_col_mapping = {
            'Município (Código)': 'Codigo_Municipio',
            'Município': 'Nome_Municipio',
            'Valor': 'Populacao'
        }
        pop_output_cols = ['Codigo_Municipio', 'Nome_Municipio', 'Populacao']
        df_populacao = process_sidra_dataframe(raw_df_populacao, pop_col_mapping, pop_output_cols)
        
        if not df_populacao.empty:
            print("Dados de População extraídos e processados com sucesso. Primeiras 5 linhas:")
            print(df_populacao.head())
        else:
            print("DataFrame de População vazio após processamento. Nenhum dado de população foi retornado.")
    except Exception as e:
        print(f"Erro ao extrair dados de População: {e}")

    df_pib = pd.DataFrame(columns=['Codigo_Municipio', 'Nome_Municipio', 'PIB_per_Capita'])
    all_pib_data = []

    try:
        print(f"\n--- Depuração do PIB per Capita ---")
        print(f"Obtendo códigos das Unidades Federativas (Estados) para iteração...")
        
        raw_uf_codes_df = sidra.get_table(
            table_code='6579',
            territorial_level='2',
            ibge_territorial_code='all',
            variables='9340',
            last_result='yes'
        )

        uf_codes = []
        if not raw_uf_codes_df.empty:
            raw_uf_codes_df.columns = raw_uf_codes_df.iloc[0]
            uf_data_rows = raw_uf_codes_df[1:].copy()
            
            if 'Unidade da Federação (Código)' in uf_data_rows.columns:
                uf_codes = uf_data_rows['Unidade da Federação (Código)'].unique().tolist()
            elif 'NC' in uf_data_rows.columns:
                uf_codes = uf_data_rows['NC'].unique().tolist()
            else:
                print(f"Colunas para UF (ex: 'Unidade da Federação (Código)' ou 'NC') não encontradas na tabela de UFs. Colunas disponíveis: {uf_data_rows.columns.tolist()}")

        else:
            print("Não foi possível obter os códigos das UFs a partir da tabela.")
            
        print(f"Total de UFs a serem processadas: {len(uf_codes)}")
            
        pib_col_mapping = {
            'Município (Código)': 'Codigo_Municipio',
            'Município': 'Nome_Municipio',
            'Valor': 'PIB_per_Capita'
        }
        pib_output_cols = ['Codigo_Municipio', 'Nome_Municipio', 'PIB_per_Capita']

        for uf_code in uf_codes:
            print(f"Extraindo PIB per Capita para a UF: {uf_code}...")
            try:
                raw_df_pib_uf = sidra.get_table(
                    table_code='5938',
                    territorial_level='6',
                    ibge_territorial_code=str(uf_code),
                    variables='6605',
                    last_result='yes'
                )
                
                df_pib_uf = process_sidra_dataframe(raw_df_pib_uf, pib_col_mapping, pib_output_cols)

                if not df_pib_uf.empty:
                    all_pib_data.append(df_pib_uf)
                    print(f"Dados de PIB per Capita para UF {uf_code} extraídos com sucesso.")
                else:
                    print(f"DataFrame de PIB per Capita vazio para a UF {uf_code}.")
            except Exception as e:
                print(f"Erro ao extrair PIB per Capita para UF {uf_code}: {e}")
        
        if all_pib_data:
            df_pib = pd.concat(all_pib_data, ignore_index=True)
            print("Todos os dados de PIB per Capita por UF concatenados com sucesso. Primeiras 5 linhas:")
            print(df_pib.head())
        else:
            print("Nenhum dado de PIB per Capita foi extraído após a iteração por UF.")

        print(f"df_pib.empty após processamento total do PIB: {df_pib.empty}")

    except Exception as e:
        print(f"Erro geral ao extrair dados de PIB per Capita ou UFs: {e}")
        df_pib = pd.DataFrame(columns=['Codigo_Municipio', 'Nome_Municipio', 'PIB_per_Capita'])

    if not df_populacao.empty and not df_pib.empty:
        df_populacao['Codigo_Municipio'] = df_populacao['Codigo_Municipio'].astype(str)
        df_pib['Codigo_Municipio'] = df_pib['Codigo_Municipio'].astype(str)
        
        df_final = pd.merge(df_populacao, df_pib, on='Codigo_Municipio', how='outer', suffixes=('_pop', '_pib'))
        
        if 'Nome_Municipio_pop' in df_final.columns and 'Nome_Municipio_pib' in df_final.columns:
            df_final['Nome_Municipio'] = df_final['Nome_Municipio_pop'].fillna(df_final['Nome_Municipio_pib'])
            df_final.drop(columns=['Nome_Municipio_pop', 'Nome_Municipio_pib'], inplace=True)
        elif 'Nome_Municipio_pop' in df_final.columns:
            df_final.rename(columns={'Nome_Municipio_pop': 'Nome_Municipio'}, inplace=True)
        elif 'Nome_Municipio_pib' in df_final.columns:
            df_final.rename(columns={'Nome_Municipio_pib': 'Nome_Municipio'}, inplace=True)

        print("Dados de População e PIB combinados com sucesso.")
    elif not df_populacao.empty:
        df_final = df_populacao
        print("Apenas dados de População foram extraídos.")
    elif not df_pib.empty:
        df_final = df_pib
        print("Apenas dados de PIB per Capita foram extraídos.")
    else:
        df_final = pd.DataFrame()
        print("Nenhum dado foi extraído e/ou nenhum DataFrame foi preenchido.")


    if not df_final.empty:
        try:
            for col in ['Populacao', 'PIB_per_Capita']:
                if col in df_final.columns:
                    df_final[col] = df_final[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
            
            if 'Nome_Municipio' in df_final.columns:
                split_data = df_final['Nome_Municipio'].astype(str).str.split(' - ', n=1, expand=True)
                if len(split_data.columns) > 1:
                    df_final['Nome_Municipio_Base'] = split_data[0]
                    df_final['Estado'] = split_data[1]
                else:
                    df_final['Nome_Municipio_Base'] = df_final['Nome_Municipio']
                    df_final['Estado'] = None
            else:
                df_final['Nome_Municipio_Base'] = None
                df_final['Estado'] = None

            city_state_counts = df_final.groupby(['Nome_Municipio_Base', 'Estado']).size().reset_index(name='count')
            duplicated_city_names = city_state_counts.groupby('Nome_Municipio_Base')['Estado'].nunique()
            cities_to_rename_with_uf = duplicated_city_names[duplicated_city_names > 1].index.tolist()

            if 'Nome_Municipio_Base' in df_final.columns and 'Estado' in df_final.columns:
                df_final['Nome_Municipio_Final'] = df_final.apply(
                    lambda row: f"{row['Nome_Municipio_Base']} ({row['Estado']})" 
                                if row['Nome_Municipio_Base'] in cities_to_rename_with_uf and pd.notna(row['Estado'])
                                else row['Nome_Municipio_Base'],
                    axis=1
                )
                df_final['Nome_Municipio'] = df_final['Nome_Municipio_Final']

            df_final.drop(columns=['Nome_Municipio_Base', 'Nome_Municipio_Final'], errors='ignore', inplace=True)


            df_final['Status_Validacao'] = 'Dados Válidos'
            
            if 'Populacao' in df_final.columns:
                pop_invalid_mask = df_final['Populacao'].isna() | (df_final['Populacao'] <= 0)
                df_final.loc[pop_invalid_mask, 'Status_Validacao'] = 'População Inválida'

            if 'PIB_per_Capita' in df_final.columns:
                pib_invalid_mask = df_final['PIB_per_Capita'].isna() | (df_final['PIB_per_Capita'] < 0)
                
                df_final.loc[pib_invalid_mask, 'Status_Validacao'] = df_final.loc[pib_invalid_mask, 'Status_Validacao'].apply(
                    lambda x: 'População e PIB Inválidos' if x == 'População Inválida' else 'PIB Inválido'
                )

            final_columns_order = [
                'Codigo_Municipio', 'Nome_Municipio', 'Estado'
            ]
            if 'Populacao' in df_final.columns:
                final_columns_order.append('Populacao')
            if 'PIB_per_Capita' in df_final.columns:
                final_columns_order.append('PIB_per_Capita')
            
            final_columns_order.append('Status_Validacao')

            final_columns_present = [col for col in final_columns_order if col in df_final.columns]
            df_final = df_final[final_columns_present]
            
            df_final.to_excel(output_filename, index=False)
            print(f"Dados exportados com sucesso para '{output_filename}'")
        except Exception as e:
            print(f"Erro ao exportar dados para Excel: {e}")
    else:
        print("Nenhum dado para exportar para Excel.")

extract_ibge_data_to_excel()













