import pandas as pd
import os
import subprocess
import sys

# --- FUNÇÕES DE AJUDA ---

def instalar_bibliotecas(pacotes):
    """Verifica e instala pacotes Python ausentes."""
    for pacote in pacotes:
        try:
            # O pyarrow é importado como tal, mas a chamada é feita para o pacote
            if pacote == 'pyarrow':
                __import__('pyarrow')
            else:
                __import__(pacote)
        except ImportError:
            print(f"Biblioteca '{pacote}' não encontrada. Instalando...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", pacote])
            except Exception as e:
                print(f"ERRO: Não foi possível instalar '{pacote}'. Verifique sua conexão ou permissões. Erro: {e}")
                sys.exit(1)
            print(f"'{pacote}' instalado com sucesso.")

# Para Parquet para CSV, precisamos apenas de pandas e pyarrow
instalar_bibliotecas(['pandas', 'pyarrow'])


# --- CONFIGURAÇÕES DE CAMINHO E FORMATO ---
# *** IMPORTANTE: SUBSTITUA O VALOR ABAIXO PELO CAMINHO COMPLETO DA SUA PASTA! ***
CAMINHO_DO_DIRETORIO = os.getcwd() 

NOME_ARQUIVO_PARQUET = 'ESTABELECIMENTO.parquet'
# 1. ARQUIVO DE SAÍDA AGORA É CSV
NOME_ARQUIVO_CSV = 'Estabelecimento_Convertido.csv' 

# 2. CONFIGURAÇÕES ESPECÍFICAS DO CSV
# Use ';' (ponto e vírgula) para compatibilidade com o Excel brasileiro ou ',' (vírgula)
SEPARADOR_CSV = ';' 
# Padrão UTF-8. Use 'latin-1' ou 'cp1252' se tiver problemas com acentuação.
ENCODING_CSV = 'utf-8'


# Cria os caminhos absolutos
CAMINHO_PARQUET = os.path.join(CAMINHO_DO_DIRETORIO, NOME_ARQUIVO_PARQUET)
CAMINHO_CSV = os.path.join(CAMINHO_DO_DIRETORIO, NOME_ARQUIVO_CSV)


# --- FUNÇÃO DE CONVERSÃO PRINCIPAL ---

def converter_parquet_para_csv_robusto(parquet_path, csv_path):
    
    # 1. VERIFICAÇÃO INICIAL DO ARQUIVO PARQUET
    print(f"Tentando acessar o arquivo Parquet em: {parquet_path}")
    if not os.path.exists(parquet_path):
        print("="*50)
        print(f"ERRO DE ARQUIVO: O arquivo '{NOME_ARQUIVO_PARQUET}' NÃO foi encontrado neste caminho.")
        print("Verifique se o 'CAMINHO_DO_DIRETORIO' está correto ou se o arquivo está na pasta.")
        print("="*50)
        return

    tamanho_mb = os.path.getsize(parquet_path) / (1024 * 1024)
    print(f"Arquivo encontrado. Tamanho: {tamanho_mb:.2f} MB.")


    # 2. TENTATIVA DE LEITURA
    try:
        print("\nINICIANDO LEITURA... (Isso pode levar tempo para arquivos grandes)")
        df = pd.read_parquet(parquet_path, engine='pyarrow')
        
        print(f"SUCESSO NA LEITURA! Total de linhas: {len(df)}")
        print("Colunas lidas: ", df.columns.tolist())

    except Exception as e:
        print("="*50)
        print("ERRO NA LEITURA DO PARQUET (pd.read_parquet falhou):")
        print(f"Detalhe do erro: {e}")
        print("="*50)
        return


    # 3. TENTATIVA DE ESCRITA NO CSV
    try:
        print(f"\nINICIANDO ESCRITA no CSV: {csv_path}")
        
        # *** MUDANÇA PRINCIPAL: to_csv ***
        df.to_csv(
            csv_path,
            sep=SEPARADOR_CSV,
            encoding=ENCODING_CSV,
            index=False, # Não inclui o índice do DataFrame como coluna
        )

        print("="*50)
        print(f"SUCESSO TOTAL! O arquivo foi salvo como: {csv_path}")
        print("="*50)
        
    except Exception as e:
        print("="*50)
        print("ERRO DESCONHECIDO NA ESCRITA DO CSV:")
        print(f"Detalhe do erro: {e}")
        print("="*50)


# --- EXECUÇÃO ---
# Chama a função de conversão, passando os novos caminhos
converter_parquet_para_csv_robusto(CAMINHO_PARQUET, CAMINHO_CSV)