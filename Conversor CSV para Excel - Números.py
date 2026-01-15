import pandas as pd
import os # Importar o módulo os para verificar a existência do arquivo
import xlsxwriter # Importar xlsxwriter para formatação avançada
import re # Importar o módulo re para expressões regulares

def limpar_moedas(valor):
    """
    Limpa strings de moeda (R$, pontos, vírgulas) e percentuais (%) e as converte para float.
    Retorna None para valores inválidos ou NaN.
    """
    if pd.isna(valor):
        return None
    
    if isinstance(valor, (int, float)):
        return float(valor)

    s = str(valor).strip()
    # Remove "R$", espaços extras e qualquer texto não numérico que não seja ponto/vírgula/%
    s = re.sub(r'[^\d,.-]+', '', s) 
    
    # Lida com o separador decimal
    if ',' in s and '.' in s: # Ex: "1.234,56"
        s = s.replace('.', '')
        s = s.replace(',', '.')
    elif ',' in s: # Ex: "1234,56"
        s = s.replace(',', '.')

    try:
        return float(s)
    except ValueError:
        # print(f"Aviso: Não foi possível converter o valor '{str(valor)}' para float após a limpeza. Retornando None.")
        return None

def converter_csv_para_xlsx(caminho_arquivo_csv, caminho_arquivo_xlsx_saida):
    """
    Converte um arquivo CSV (assumindo vírgulas como delimitador) para um arquivo XLSX,
    aplicando formatação personalizada (cabeçalhos, totais, condicional).

    Args:
        caminho_arquivo_csv (str): O caminho completo para o arquivo CSV de entrada.
        caminho_arquivo_xlsx_saida (str): O caminho completo para o arquivo XLSX de saída.
    """
    print(f"Iniciando a conversão de '{caminho_arquivo_csv}' para '{caminho_arquivo_xlsx_saida}' com formatação...")

    # 1. Verificar se o arquivo CSV de entrada existe
    if not os.path.exists(caminho_arquivo_csv):
        print(f"Erro: O arquivo CSV de entrada '{caminho_arquivo_csv}' não foi encontrado.")
        return

    try:
        # 2. Ler o arquivo CSV usando pandas
        df = pd.read_csv(caminho_arquivo_csv, delimiter=',', encoding='utf-8')
        print(f"Arquivo CSV '{caminho_arquivo_csv}' lido com sucesso. Total de {df.shape[0]} linhas.")

        if df.empty:
            print("O DataFrame está vazio. Nenhum dado para escrever no Excel.")
            return

        # --- Pré-processamento: Converter colunas para numérico antes de qualquer cálculo ---
        # Colunas que precisam ser numéricas para soma/média
        # Adapte esta lista para as colunas exatas do seu CSV que precisam ser limpas e convertidas
        numeric_cols_for_processing = [
            'Meta', 'Vendido', 'GAP (R$)', 'Itens', 'Conversao', 'VLM', 
            'Atingimento', 'QTD_BOLSAS', 'QTD_SAPATOS', 'QTD_ACESSORIOS', 
            'QTD_SACOLAS', 'TOTAL', '%B_P'
        ]

        # Renomear colunas no DataFrame antes do processamento se os nomes originais forem diferentes
        # Assumindo que 'meta_vendedor' e 'Faturado' são os nomes no CSV de entrada
        if 'meta_vendedor' in df.columns:
            df.rename(columns={'meta_vendedor': 'Meta'}, inplace=True)
        if 'Faturado' in df.columns:
            df.rename(columns={'Faturado': 'Vendido'}, inplace=True)

        for col_name in numeric_cols_for_processing:
            if col_name in df.columns:
                # Limpar strings de moeda e percentual
                if df[col_name].dtype == 'object': # Se for string
                    # Usar a função limpar_moedas para converter
                    df[col_name] = df[col_name].apply(limpar_moedas)
                    
                    # Para colunas de porcentagem, dividir por 100 se o valor já não for um decimal
                    if col_name in ['Atingimento', '%B_P']:
                        df[col_name] = df[col_name].apply(lambda x: x / 100 if pd.notna(x) and x > 1 else x)
                
                # Preencher NaNs com 0 para colunas numéricas que serão somadas
                if col_name in ['Meta', 'Vendido', 'GAP (R$)', 'Itens', 'Conversao', 
                                'QTD_BOLSAS', 'QTD_SAPATOS', 'QTD_ACESSORIOS', 'QTD_SACOLAS', 
                                'TOTAL']:
                    df[col_name] = df[col_name].fillna(0)
        # --- Fim do Pré-processamento ---

        # 3. Preparar o ExcelWriter e o Workbook/Worksheet
        writer = pd.ExcelWriter(caminho_arquivo_xlsx_saida, engine='xlsxwriter')
        workbook = writer.book
        worksheet = workbook.add_worksheet()

        # 4. Definir formatos
        # Formato padrão para células, centralizado
        default_cell_format = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'font_name': 'Calibri', # Adicionado
            'font_size': 9         # Adicionado
        })

        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#000000', # Preto
            'font_color': '#FFFFFF', # Branco
            'align': 'center', # Centralizado
            'valign': 'vcenter',
            'border': 1,
            'font_name': 'Calibri', # Adicionado
            'font_size': 9         # Adicionado
        })

        # Formato base para a linha de total, centralizado
        total_base_format = {
            'bold': True,
            'bg_color': '#000000', # Preto
            'font_color': '#FFFFFF', # Branco
            'align': 'center', # Centralizado
            'valign': 'vcenter',
            'border': 1,
            'font_name': 'Calibri', # Adicionado
            'font_size': 9         # Adicionado
        }
        
        # Formatos combinados para a linha de total
        total_currency_format = workbook.add_format({**total_base_format, 'num_format': 'R$ #,##0.00'})
        total_percentage_format = workbook.add_format({**total_base_format, 'num_format': '0.00%'})
        total_default_format = workbook.add_format(total_base_format) # Para colunas não monetárias/percentuais na linha total

        # Formatos para células de dados, centralizados
        currency_format = workbook.add_format({
            'num_format': 'R$ #,##0.00', 
            'align': 'center', 
            'valign': 'vcenter',
            'font_name': 'Calibri', # Adicionado
            'font_size': 9         # Adicionado
        })
        percentage_format = workbook.add_format({
            'num_format': '0.00%', 
            'align': 'center', 
            'valign': 'vcenter',
            'font_name': 'Calibri', # Adicionado
            'font_size': 9         # Adicionado
        })
        
        # Formatos para formatação condicional (Atingimento)
        red_format = workbook.add_format({
            'bg_color': '#FFC7CE', 
            'font_color': '#9C0006', 
            'align': 'center', 
            'valign': 'vcenter',
            'font_name': 'Calibri', # Adicionado
            'font_size': 9         # Adicionado
        }) # Vermelho claro
        orange_format = workbook.add_format({
            'bg_color': '#FFEB9C', 
            'font_color': '#9C6500', 
            'align': 'center', 
            'valign': 'vcenter',
            'font_name': 'Calibri', # Adicionado
            'font_size': 9         # Adicionado
        }) # Laranja claro
        yellow_format = workbook.add_format({
            'bg_color': '#FFFD00', 
            'font_color': '#9C6500', 
            'align': 'center', 
            'valign': 'vcenter',
            'font_name': 'Calibri', # Adicionado
            'font_size': 9         # Adicionado
        }) # Amarelo
        green_format = workbook.add_format({
            'bg_color': '#C6EFCE', 
            'font_color': '#006100', 
            'align': 'center', 
            'valign': 'vcenter',
            'font_name': 'Calibri', # Adicionado
            'font_size': 9         # Adicionado
        }) # Verde claro

        # --- Calcular e adicionar a linha de TOTAL ---
        total_row_data = {}
        for col in df.columns:
            if col in ['Meta', 'Vendido', 'GAP (R$)', 'Itens', 'Conversao', 
                        'QTD_BOLSAS', 'QTD_SAPATOS', 'QTD_ACESSORIOS', 
                        'QTD_SACOLAS', 'TOTAL']:
                total_row_data[col] = df[col].sum()
            elif col in ['VLM', 'Atingimento', '%B_P']:
                total_row_data[col] = df[col].mean()
            elif col in ['Tipo_Evento', 'vendedor']: # Colunas de identificação para a linha TOTAL
                total_row_data[col] = 'TOTAL' if col == 'Tipo_Evento' else '' # 'TOTAL' na primeira coluna, vazio nas outras
            else: # Para outras colunas, preencher com vazio ou NaN
                total_row_data[col] = '' 
        
        # Adiciona a linha de total ao DataFrame
        df = pd.concat([df, pd.DataFrame([total_row_data])], ignore_index=True)
        total_row_index_in_df = df.index[-1]
        # --- Fim do cálculo e adição da linha de TOTAL ---

        # 5. Escrever cabeçalhos com formatação
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # 6. Escrever dados e aplicar formatação de coluna
        for row_num, row_data in df.iterrows():
            excel_row = row_num + 1 # +1 para pular a linha do cabeçalho
            
            is_total_row = (row_num == total_row_index_in_df)

            for col_num, value in enumerate(row_data):
                col_name = df.columns[col_num]
                
                current_cell_format = default_cell_format # Começa com o formato padrão

                # Aplicar formato de moeda
                if col_name in ['Meta', 'Vendido', 'GAP (R$)', 'VLM', 'TOTAL']: 
                    if pd.isna(value):
                        worksheet.write(excel_row, col_num, '', current_cell_format) # Exibe vazio para NaN
                    else:
                        if is_total_row:
                            worksheet.write(excel_row, col_num, value, total_currency_format)
                        else:
                            worksheet.write(excel_row, col_num, value, currency_format)
                
                # Aplicar formato de porcentagem e condicional para 'Atingimento' ou '%B_P'
                elif col_name in ['Atingimento', '%B_P']:
                    if pd.isna(value):
                        worksheet.write(excel_row, col_num, '', current_cell_format) # Exibe vazio para NaN
                    else:
                        if is_total_row:
                            worksheet.write(excel_row, col_num, value, total_percentage_format)
                        else:
                            worksheet.write(excel_row, col_num, value, percentage_format)
                            # Aplicar formatação condicional (apenas se não for a linha de total)
                            if col_name == 'Atingimento' and not is_total_row:
                                if value < 0.50:
                                    worksheet.conditional_format(excel_row, col_num, excel_row, col_num,
                                        {'type': 'cell', 'criteria': '<', 'value': 0.50, 'format': red_format})
                                elif value < 0.70:
                                    worksheet.conditional_format(excel_row, col_num, excel_row, col_num,
                                        {'type': 'cell', 'criteria': '<', 'value': 0.70, 'format': orange_format})
                                elif value < 0.90:
                                    worksheet.conditional_format(excel_row, col_num, excel_row, col_num,
                                        {'type': 'cell', 'criteria': '<', 'value': 0.90, 'format': yellow_format})
                                else: # >= 0.90
                                    worksheet.conditional_format(excel_row, col_num, excel_row, col_num,
                                        {'type': 'cell', 'criteria': '>=', 'value': 0.90, 'format': green_format})
                
                # Para outras colunas, escrever valor normalmente
                else:
                    if is_total_row:
                        worksheet.write(excel_row, col_num, value, total_default_format)
                    else:
                        worksheet.write(excel_row, col_num, value, default_cell_format)
                
        # 7. Ajustar largura das colunas
        for col_num, col_name in enumerate(df.columns):
            # Ajuste para lidar com colunas que podem ter valores vazios na linha total
            col_values_str = df[col_name].astype(str).replace('nan', '') 
            max_len = max(col_values_str.map(len).max(), len(col_name))
            worksheet.set_column(col_num, col_num, max_len + 2) # +2 para um pouco de padding

        # 8. Fechar o ExcelWriter para salvar o arquivo
        writer.close()
        print(f"Arquivo XLSX salvo com sucesso em '{caminho_arquivo_xlsx_saida}' com formatação.")

    except FileNotFoundError:
        print(f"Erro: O arquivo '{caminho_arquivo_csv}' não foi encontrado.")
    except UnicodeDecodeError:
        print(f"Erro de codificação ao ler o CSV. Tente alterar 'encoding=' para 'latin1' ou 'cp1252' na função pd.read_csv.")
    except Exception as e:
        print(f"Ocorreu um erro durante a conversão e formatação: {e}")

# --- Exemplo de como usar a função ---
# Certifique-se de que o arquivo 'lista.csv' exista no mesmo diretório do script,
# ou forneça o caminho completo para ele.
# O arquivo de saída será 'saida_formatada.xlsx'.
converter_csv_para_xlsx("Mix de Produtos MM.csv", "Mix_Produtos_MM_Saida.xlsx")
 


