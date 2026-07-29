import pandas as pd
import os

# --- Configurações ---
# 1. Defina o nome do seu arquivo CSV de entrada
arquivo_csv = 'CNAE.csv' 

# 2. Defina o nome do arquivo Excel de saída
arquivo_excel = 'CNAE.xlsx'

# 3. Defina o nome da aba no Excel
nome_da_aba = 'CNAE'

# 4. Parâmetros do CSV (Importante para CSVs brasileiros)
# Se seu arquivo usa ponto-e-vírgula, mude para ';'. Se usa vírgula, use ','.
separador_csv = ';' 

# 5. *** NOVO PARÂMETRO: ENCODING ***
# 'latin-1' ou 'ISO-8859-1' costumam resolver problemas com caracteres acentuados em CSVs brasileiros.
encoding_csv = 'latin-1' 


# --- Função de Conversão Ajustada ---
def converter_csv_para_excel(csv_path, excel_path, sheet_name, sep, encoding):
    """
    Lê um arquivo CSV (usando o encoding especificado) e salva em Excel.
    """
    if not os.path.exists(csv_path):
        print(f"ERRO: Arquivo CSV não encontrado: {csv_path}")
        return

    try:
        # 1. Ler o arquivo CSV e criar um DataFrame
        print(f"Lendo o arquivo CSV: {csv_path}...")
        
        # *** MUDANÇA AQUI: Adicionando o parâmetro 'encoding' ***
        df = pd.read_csv(csv_path, sep=sep, encoding=encoding)
        
        print(f"Dados lidos. Total de linhas: {len(df)}")

        # 2. Escrever o DataFrame no arquivo Excel (.xlsx)
        print(f"Escrevendo dados no Excel: {excel_path}...")
        
        df.to_excel(
            excel_path,
            sheet_name=sheet_name,
            index=False,
            header=True,
            engine='xlsxwriter'
        )

        print(f"\nSucesso! O arquivo foi salvo como: {excel_path}")
        
    except Exception as e:
        print(f"Ocorreu um erro durante a conversão: {e}")

# --- Execução Ajustada ---
converter_csv_para_excel(arquivo_csv, arquivo_excel, nome_da_aba, separador_csv, encoding_csv)