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
        return 0.0
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
        return 0.0

def converter_csv_para_xlsx(caminho_arquivo_csv, caminho_arquivo_xlsx_saida):
    print(f"Iniciando a conversão de '{caminho_arquivo_csv}' para '{caminho_arquivo_xlsx_saida}'...")

    if not os.path.exists(caminho_arquivo_csv):
        print(f"Erro: O arquivo CSV de entrada '{caminho_arquivo_csv}' não foi encontrado.")
        return

    try:
        # Lendo o CSV
        df = pd.read_csv(caminho_arquivo_csv, sep=',', quotechar='"', encoding='utf-8', on_bad_lines='warn')
        
        # 🔥 SUBSTITUIÇÃO GLOBAL DE NaN/INF POR 0 ANTES DE TUDO
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.fillna(0)

        if df.empty:
            print("O DataFrame está vazio.")
            return

        # ==========================================
        # 🔥 TRATAMENTO DE DATA E HORA
        # ==========================================
        col_data = 'DataExpedicaoCompleta'
        
        if col_data in df.columns:
            df[col_data] = pd.to_datetime(df[col_data], errors='coerce')
            df['DATA_CONVERTIDA'] = df[col_data].dt.date
            df['CHAVE_MMM'] = df[col_data].dt.strftime('%B').str.upper()
            df['CHAVE_AAA'] = df[col_data].dt.strftime('%Y').str.upper()
            df['HoraExpedicao'] = df[col_data].dt.strftime('%H:%M:%S')
            
            # Garante que nulos gerados pela conversão de data não quebrem o Excel
            df['DATA_CONVERTIDA'] = df['DATA_CONVERTIDA'].fillna(0)
            df['HoraExpedicao'] = df['HoraExpedicao'].fillna('00:00:00')

        # =============================
        # 🔢 TRATAMENTO NUMÉRICO (Incluso ValorTotal)
        # =============================
        numeric_cols = [
            'Meta', 'Vendido', 'GAP (R$)', 'Itens', 'Conversao', 'VLM', 
            'Atingimento', 'QTD_BOLSAS', 'QTD_SAPATOS', 'QTD_ACESSORIOS', 
            'QTD_SACOLAS', 'TOTAL', '%B_P', 'Quantidade', 'ValorTotal'
        ]

        # Padronização de nomes
        if 'meta_vendedor' in df.columns: df.rename(columns={'meta_vendedor': 'Meta'}, inplace=True)
        if 'Faturado' in df.columns: df.rename(columns={'Faturado': 'Vendido'}, inplace=True)

        for col in numeric_cols:
            if col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(limpar_moedas)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # =============================
        # 📊 LINHA DE TOTAL
        # =============================
        total_row = {}
        for col in df.columns:
            if col in ['Meta', 'Vendido', 'GAP (R$)', 'Itens', 'TOTAL', 'Quantidade', 'ValorTotal']:
                total_row[col] = df[col].sum()
            elif col in ['VLM', 'Atingimento', '%B_P']:
                total_row[col] = df[col].mean()
            elif col == df.columns[0]:
                total_row[col] = 'TOTAL GERAL'
            else:
                total_row[col] = ''

        df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True).fillna(0)
        idx_total = df.index[-1]

        # =============================
        # 📄 EXCEL (FORMATADO)
        # =============================
        writer = pd.ExcelWriter(caminho_arquivo_xlsx_saida, engine='xlsxwriter')
        workbook = writer.book
        worksheet = workbook.add_worksheet('Relatorio')

        # Estilos
        fmt_base = {'align': 'center', 'valign': 'vcenter', 'font_name': 'Calibri', 'font_size': 9}
        header_fmt = workbook.add_format({**fmt_base, 'bold': True, 'bg_color': '#000000', 'font_color': '#FFFFFF', 'border': 1})
        
        curr_fmt = workbook.add_format({**fmt_base, 'num_format': 'R$ #,##0.00'})
        perc_fmt = workbook.add_format({**fmt_base, 'num_format': '0.00%'})
        time_fmt = workbook.add_format({**fmt_base, 'num_format': 'HH:MM:SS'})
        date_fmt = workbook.add_format({**fmt_base, 'num_format': 'DD/MM/YYYY'})
        text_fmt = workbook.add_format(fmt_base)

        total_base = {**fmt_base, 'bold': True, 'bg_color': '#000000', 'font_color': '#FFFFFF', 'border': 1}
        total_curr_fmt = workbook.add_format({**total_base, 'num_format': 'R$ #,##0.00'})
        total_text_fmt = workbook.add_format(total_base)

        # Escrever Cabeçalho
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_fmt)

        # Escrever Dados
        for row_num, row_data in df.iterrows():
            excel_row = row_num + 1
            is_total = (row_num == idx_total)

            for col_num, value in enumerate(row_data):
                col_name = df.columns[col_num]

                if is_total:
                    fmt = total_curr_fmt if col_name in ['Meta', 'Vendido', 'GAP (R$)', 'TOTAL', 'ValorTotal'] else total_text_fmt
                    worksheet.write(excel_row, col_num, value, fmt)
                else:
                    if col_name in ['Meta', 'Vendido', 'GAP (R$)', 'VLM', 'TOTAL', 'ValorTotal']:
                        worksheet.write(excel_row, col_num, value, curr_fmt)
                    elif col_name in ['Atingimento', '%B_P']:
                        worksheet.write(excel_row, col_num, value, perc_fmt)
                    elif col_name == 'HoraExpedicao':
                        worksheet.write(excel_row, col_num, value, time_fmt)
                    elif col_name == 'DATA_CONVERTIDA':
                        worksheet.write(excel_row, col_num, value, date_fmt)
                    else:
                        worksheet.write(excel_row, col_num, value, text_fmt)

        # Ajustar largura
        for col_num, col_name in enumerate(df.columns):
            worksheet.set_column(col_num, col_num, 18)

        writer.close()
        print(f"Sucesso! Arquivo gerado: {caminho_arquivo_xlsx_saida}")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")

# EXECUÇÃO
converter_csv_para_xlsx("ResultadoSQL.csv", "ResultadoSQL.xlsx")