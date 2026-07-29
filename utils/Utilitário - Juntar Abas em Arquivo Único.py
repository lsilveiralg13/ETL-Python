import os
import pandas as pd

# 1. Definição dos caminhos e nomes dos arquivos
diretorio = r"C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python"
arquivo_entrada = os.path.join(diretorio, "RELATÓRIO ATENDIMENTO AO CLIENTE - VENTTOS.xlsx")
arquivo_saida = os.path.join(diretorio, "RELATÓRIO ATENDIMENTO AO CLIENTE FORMATADO.xlsx")

print("Carregando o arquivo Excel...")
# 2. Carrega o arquivo Excel para ler os nomes de todas as abas
excel_file = pd.ExcelFile(arquivo_entrada)
todas_as_abas = excel_file.sheet_names

# Lista para armazenar os DataFrames de cada dia
dataframes_a_juntar = []

print("Varrendo as abas para consolidação...")
# 3. Loop pelas abas procurando os padrões informados (Ignorando maiúsculas/minúsculas)
for aba in todas_as_abas:
    aba_upper = aba.upper().strip()
    
    # Verifica se começa com ATENDIMENTO, ATENDIMENTOS ou ATEND.
    if aba_upper.startswith("ATENDIMENTO") or aba_upper.startswith("ATENDIMENTOS") or aba_upper.startswith("ATEND."):
        print(f"-> Lendo aba ativa: {aba}")
        
        # Lê os dados da aba atual
        df_aba = pd.read_excel(arquivo_entrada, sheet_name=aba)
        
        # Opcional: Adiciona uma coluna para saber de qual aba veio o dado (caso precise validar a data depois)
        df_aba['Aba_Origem'] = aba
        
        dataframes_a_juntar.append(df_aba)

# 4. Junta tudo se houver abas encontradas
if dataframes_a_juntar:
    print("\nEmpilhando os dados na tabela única...")
    # concat junta tudo por colunas idênticas automaticamente
    df_consolidado = pd.concat(dataframes_a_juntar, ignore_index=True)
    
    print(f"Salvando o novo arquivo em: {arquivo_saida}")
    # 5. Salva na nova planilha criando a aba "BASE"
    with pd.ExcelWriter(arquivo_saida, engine='openpyxl') as writer:
        df_consolidado.to_excel(writer, sheet_name="BASE", index=False)
        
    print("Processo concluído com sucesso! A aba BASE está pronta.")
else:
    print("\n[ERRO] Nenhuma aba correspondente aos padrões foi encontrada. Verifique os nomes.")