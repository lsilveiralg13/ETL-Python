import pandas as pd

# --- CONFIGURAÇÃO ---
ARQUIVO_ALVO_PATH = r"C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\HUB MULTIMARCAS\Controle de Agendamentos - Showroom Inverno 2026.xlsx"
ARQUIVO_MATRIZ_PATH = "Modelo de Motor de Crédito 6.xlsx" 
ARQUIVO_SAIDA_PATH = ARQUIVO_ALVO_PATH.replace(".xlsx", "_PROCESSADO.xlsx") # Novo nome

ABAS_ALVO = ['ERIKHA', 'GLENDA', 'ISABELLA', 'JOSIANE', 'LUCIANA', 'MARCELA', 'NELIANE']

# Nomes exatos dos cabeçalhos
COLUNA_CHAVE_ALVO = 'COD. PAR.'
COLUNA_CHAVE_MATRIZ = 'Cód Parceiro'
COLUNA_RETORNO_MATRIZ = 'Limite Sugerido'

NOME_COLUNA_DESTINO = 'LIMITE DISP. AJUSTADO' 

# --- LÓGICA DE EXECUÇÃO ---

# 1. Carregar a Matriz de Dados (Caminho e Colunas OK)
print(f"1. Carregando Matriz de Dados de: {ARQUIVO_MATRIZ_PATH}")

df_matriz = pd.read_excel(
    ARQUIVO_MATRIZ_PATH,
    usecols=[COLUNA_CHAVE_MATRIZ, COLUNA_RETORNO_MATRIZ]
)
df_matriz.columns = [COLUNA_CHAVE_ALVO, NOME_COLUNA_DESTINO] # Renomeia para o merge

# 2. Processar e Agrupar todas as abas
print("2. Iniciando PROCX/Merge nas abas alvo...")

# Dicionário para armazenar todas as abas processadas
abas_processadas = {}

for sheet_name in ABAS_ALVO:
    try:
        print(f"   -> Processando aba: {sheet_name}")
        
        # Leitura da Aba Alvo usando Pandas (mais estável)
        df_aba = pd.read_excel(
            ARQUIVO_ALVO_PATH,
            sheet_name=sheet_name,
            header=0
        )
        
        # Validação de Chave
        if COLUNA_CHAVE_ALVO not in df_aba.columns:
            print(f"   -> ERRO: Chave '{COLUNA_CHAVE_ALVO}' não encontrada em {sheet_name}. Pulando.")
            abas_processadas[sheet_name] = df_aba # Salva a aba sem modificação
            continue
            
        # O PROCX (Merge)
        df_aba_merged = pd.merge(
            df_aba, 
            df_matriz, 
            on=COLUNA_CHAVE_ALVO, 
            how='left'
        )
        
        # Se a coluna de destino existir no original, o Pandas criará uma cópia (Ex: LIMITE DISP. AJUSTADO_x)
        # Vamos garantir que apenas a coluna nova fique com o nome final
        colunas_a_deletar = [col for col in df_aba_merged.columns if col.endswith('_x')]
        df_aba_merged = df_aba_merged.drop(columns=colunas_a_deletar, errors='ignore')
        
        # Armazena o resultado
        abas_processadas[sheet_name] = df_aba_merged
        
    except Exception as e:
        print(f"   -> ERRO INESPERADO ao processar {sheet_name}: {e}. Pulando.")


# 3. Salvar todas as abas processadas em um NOVO ARQUIVO
print("\n3. Salvando resultados em NOVO ARQUIVO...")

try:
    with pd.ExcelWriter(ARQUIVO_SAIDA_PATH, engine='xlsxwriter') as writer:
        for sheet_name, df in abas_processadas.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # ATENÇÃO: A formatação de moeda deve ser feita manualmente no novo arquivo.
            # O ExcelWriter não tem controle sobre a formatação da célula como openpyxl.

    print("\nPROCESSO CONCLUÍDO COM SUCESSO!")
    print(f"O novo arquivo '{ARQUIVO_SAIDA_PATH}' foi criado.")
    print("\nAVISO: A formatação de Moeda deve ser aplicada manualmente na Coluna 'LIMITE DISP. AJUSTADO'.")

except Exception as e:
    print(f"\nERRO ao salvar o arquivo: {e}")
    print("Verifique se o caminho de destino está correto.")