import pandas as pd
import os
import xlsxwriter
import re
import numpy as np
import locale

# Tentar configurar locale para PT-BR
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')
    except:
        print("Aviso: Não foi possível configurar locale para PT-BR. Meses podem vir em inglês.")

def limpar_moedas(valor):
    if pd.isna(valor):
        return None
    
    if isinstance(valor, (int, float)):
        return float(valor)

    s = str(valor).strip()
    s = re.sub(r'[^\d,.-]+', '', s) 
    
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')

    try:
        return float(s)
    except ValueError:
        return None

def converter_csv_para_xlsx(caminho_arquivo_csv, caminho_arquivo_xlsx_saida):
    print(f"Iniciando a conversão de '{caminho_arquivo_csv}' para '{caminho_arquivo_xlsx_saida}' com formatação...")

    if not os.path.exists(caminho_arquivo_csv):
        print(f"Erro: O arquivo CSV de entrada '{caminho_arquivo_csv}' não foi encontrado.")
        return

    try:
        df = pd.read_csv(caminho_arquivo_csv, sep=',', quotechar='"', encoding='utf-8', on_bad_lines='warn')
        
        df = df.replace([np.inf, -np.inf], np.nan)

        print(f"Arquivo CSV '{caminho_arquivo_csv}' lido com sucesso. Total de {df.shape[0]} linhas.")

        if df.empty:
            print("O DataFrame está vazio.")
            return

        # =============================
        # 🔥 TRATAMENTO DE DATA
        # =============================
        if 'DataExpedicao' in df.columns:

            def converter_data(valor):
                if pd.isna(valor):
                    return pd.NaT
                
                valor = str(valor)

                try:
                    if '-' in valor:
                        return pd.to_datetime(valor, format='%Y-%m-%d', errors='coerce')
                    elif '/' in valor:
                        return pd.to_datetime(valor, format='%d/%m/%Y', errors='coerce')
                    else:
                        return pd.to_datetime(valor, errors='coerce')
                except:
                    return pd.NaT

            df['DATA_CONVERTIDA'] = df['DataExpedicao'].apply(converter_data)

            df['CHAVE_MMM'] = df['DATA_CONVERTIDA'].dt.strftime('%B').str.upper()
            df['CHAVE_AAA'] = df['DATA_CONVERTIDA'].dt.strftime('%Y').str.upper()

        # =============================
        # 🔢 TRATAMENTO NUMÉRICO
        # =============================
        numeric_cols_for_processing = [
            'Meta', 'Vendido', 'GAP (R$)', 'Itens', 'Conversao', 'VLM', 
            'Atingimento', 'QTD_BOLSAS', 'QTD_SAPATOS', 'QTD_ACESSORIOS', 
            'QTD_SACOLAS', 'TOTAL', '%B_P'
        ]

        if 'meta_vendedor' in df.columns:
            df.rename(columns={'meta_vendedor': 'Meta'}, inplace=True)
        if 'Faturado' in df.columns:
            df.rename(columns={'Faturado': 'Vendido'}, inplace=True)

        for col_name in numeric_cols_for_processing:
            if col_name in df.columns:
                if df[col_name].dtype == 'object':
                    df[col_name] = df[col_name].apply(limpar_moedas)
                    
                    if col_name in ['Atingimento', '%B_P']:
                        df[col_name] = df[col_name].apply(lambda x: x / 100 if pd.notna(x) and x > 1 else x)
                
                df[col_name] = df[col_name].fillna(0)

        # =============================
        # 📊 TOTAL
        # =============================
        total_row_data = {}
        for col in df.columns:
            if col in ['Meta', 'Vendido', 'GAP (R$)', 'Itens', 'Conversao', 'QTD_BOLSAS', 'QTD_SAPATOS', 'QTD_ACESSORIOS', 'QTD_SACOLAS', 'TOTAL']:
                total_row_data[col] = df[col].sum()
            elif col in ['VLM', 'Atingimento', '%B_P']:
                total_row_data[col] = df[col].mean()
            elif col in ['Tipo_Evento', 'vendedor']:
                total_row_data[col] = 'TOTAL' if col == 'Tipo_Evento' else ''
            else:
                total_row_data[col] = ''

        df = pd.concat([df, pd.DataFrame([total_row_data])], ignore_index=True)
        total_row_index_in_df = df.index[-1]

        # =============================
        # 📄 EXCEL
        # =============================
        writer = pd.ExcelWriter(
            caminho_arquivo_xlsx_saida, 
            engine='xlsxwriter', 
            engine_kwargs={'options': {'nan_inf_to_errors': True}}
        )
        
        workbook = writer.book
        worksheet = workbook.add_worksheet()

        fmt_base = {'align': 'center', 'valign': 'vcenter', 'font_name': 'Calibri', 'font_size': 9}
        
        default_cell_format = workbook.add_format(fmt_base)
        header_format = workbook.add_format({**fmt_base, 'bold': True, 'bg_color': '#000000', 'font_color': '#FFFFFF', 'border': 1})
        
        total_base = {**fmt_base, 'bold': True, 'bg_color': '#000000', 'font_color': '#FFFFFF', 'border': 1}
        total_currency_format = workbook.add_format({**total_base, 'num_format': 'R$ #,##0.00'})
        total_percentage_format = workbook.add_format({**total_base, 'num_format': '0.00%'})
        total_default_format = workbook.add_format(total_base)

        currency_format = workbook.add_format({**fmt_base, 'num_format': 'R$ #,##0.00'})
        percentage_format = workbook.add_format({**fmt_base, 'num_format': '0.00%'})

        # Cabeçalho
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # Dados
        for row_num, row_data in df.iterrows():
            excel_row = row_num + 1
            is_total_row = (row_num == total_row_index_in_df)

            for col_num, value in enumerate(row_data):
                col_name = df.columns[col_num]

                if pd.isna(value):
                    worksheet.write(excel_row, col_num, '', default_cell_format)
                    continue

                if col_name in ['Meta', 'Vendido', 'GAP (R$)', 'VLM', 'TOTAL']: 
                    fmt = total_currency_format if is_total_row else currency_format
                    worksheet.write(excel_row, col_num, value, fmt)
                
                elif col_name in ['Atingimento', '%B_P']:
                    fmt = total_percentage_format if is_total_row else percentage_format
                    worksheet.write(excel_row, col_num, value, fmt)
                
                else:
                    fmt = total_default_format if is_total_row else default_cell_format
                    worksheet.write(excel_row, col_num, value, fmt)

        # Ajustar largura
        for col_num, col_name in enumerate(df.columns):
            max_len = max(df[col_name].astype(str).map(len).max(), len(col_name))
            worksheet.set_column(col_num, col_num, max_len + 2)

        writer.close()
        print(f"Arquivo XLSX salvo com sucesso em '{caminho_arquivo_xlsx_saida}'.")

    except Exception as e:
        print(f"Erro: {e}")


# EXECUÇÃO
converter_csv_para_xlsx("ResultadoSQL.csv", "ResultadoSQL.xlsx")