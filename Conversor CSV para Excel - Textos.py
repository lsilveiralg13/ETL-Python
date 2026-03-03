import pandas as pd
import os # Importar o módulo os para verificar a existência do arquivo

def converter_csv_para_xlsx_simples(caminho_arquivo_csv, caminho_arquivo_xlsx_saida):
    """
    Converte um arquivo CSV para um arquivo XLSX de forma simples,
    sem formatação avançada.

    Args:
        caminho_arquivo_csv (str): O caminho completo para o arquivo CSV de entrada.
        caminho_arquivo_xlsx_saida (str): O caminho completo para o arquivo XLSX de saída.
    """
    print(f"Iniciando a conversão simples de '{caminho_arquivo_csv}' para '{caminho_arquivo_xlsx_saida}'...")

    # 1. Verificar se o arquivo CSV de entrada existe
    if not os.path.exists(caminho_arquivo_csv):
        print(f"Erro: O arquivo CSV de entrada '{caminho_arquivo_csv}' não foi encontrado.")
        return

    try:
        # 2. Ler o arquivo CSV usando pandas
        # Tenta inferir o delimitador e a codificação, ou use padrões comuns.
        # Para arquivos de texto simples, 'utf-8' e ',' (vírgula) são bons pontos de partida.
        df = pd.read_csv(caminho_arquivo_csv, encoding='utf-8') 
        print(f"Arquivo CSV '{caminho_arquivo_csv}' lido com sucesso. Total de {df.shape[0]} linhas.")

        if df.empty:
            print("O DataFrame está vazio. Nenhum dado para escrever no Excel.")
            return

        # 3. Escrever o DataFrame para um arquivo XLSX
        # index=False evita que o índice do DataFrame seja salvo como uma coluna no Excel
        df.to_excel(caminho_arquivo_xlsx_saida, index=False)
        print(f"Arquivo XLSX salvo com sucesso em '{caminho_arquivo_xlsx_saida}'.")

    except FileNotFoundError:
        print(f"Erro: O arquivo '{caminho_arquivo_csv}' não foi encontrado.")
    except UnicodeDecodeError:
        print(f"Erro de codificação ao ler o CSV. Tente alterar 'encoding=' para 'latin1' ou 'cp1252' na função pd.read_csv.")
    except Exception as e:
        print(f"Ocorreu um erro durante a conversão: {e}")

# --- Exemplo de como usar a função ---
# Substitua 'seu_arquivo_simples.csv' pelo nome do seu arquivo CSV de texto.
# O arquivo de saída será 'saida_simples.xlsx'.
converter_csv_para_xlsx_simples("Texto.csv", "Formatado_Simples.xlsx")

