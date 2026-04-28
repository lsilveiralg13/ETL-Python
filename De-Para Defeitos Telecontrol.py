import pandas as pd
import os

# =================================================================
# CONFIGURAÇÃO DE CAMINHOS
# =================================================================
caminho_pasta = r'C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python'

arquivo_bruto = os.path.join(caminho_pasta, 'Classificação.xlsx')
arquivo_dicionario = os.path.join(caminho_pasta, 'defeitos_padronizados.xlsx')
arquivo_saida = os.path.join(caminho_pasta, 'Classificacao_Unicos_Padronizada.xlsx')

def processar_de_para_direto():
    try:
        # Carrega os arquivos (ajuste os nomes das abas se necessário)
        df_classificacao = pd.read_excel(arquivo_bruto)
        df_padrao = pd.read_excel(arquivo_dicionario)
        print("Arquivos carregados com sucesso!")
    except Exception as e:
        print(f"Erro ao carregar arquivos: {e}")
        return

    # Padronizamos o dicionário para evitar erro de caixa alta/baixa
    df_padrao['Defeito Padronizado'] = df_padrao['Defeito Padronizado'].astype(str).str.strip().str.upper()
    df_padrao['Categoria'] = df_padrao['Categoria'].astype(str).str.strip().str.upper()

    def realizar_mapeamento(linha):
        # Unimos as colunas de busca em uma string só para a varredura
        texto_original = f"{str(linha.get('DEFEITO CONSTATADO', ''))} {str(linha.get('DEFEITO RECLAMADO', ''))}".upper()
        
        # O De/Para: percorre o dicionário e verifica se o termo padrão está contido no texto
        for _, row in df_padrao.iterrows():
            termo_chave = row['Defeito Padronizado']
            
            # Se encontrar o termo EXATO do dicionário dentro do texto sujo
            if termo_chave in texto_original:
                return termo_chave, row['Categoria']
        
        return None, None

    print(f"Processando {len(df_classificacao)} motivos únicos...")

    # Aplica o De/Para
    mapeamento = df_classificacao.apply(realizar_mapeamento, axis=1)
    
    # Criamos as colunas de resultado
    df_classificacao['DEFEITO_CONSTATADO_PADRAO'] = [x[0] for x in mapeamento]
    df_classificacao['CATEGORIA_DEFEITO_NOVA'] = [x[1] for x in mapeamento]

    # Preenchemos a coluna CATEGORIA_DEFEITO original com o resultado do "PROCX"
    df_classificacao['CATEGORIA_DEFEITO'] = df_classificacao['CATEGORIA_DEFEITO_NOVA'].fillna(df_classificacao.get('CATEGORIA_DEFEITO', ''))
    
    # Atualiza o DEFEITO CONSTATADO para o termo padronizado
    df_classificacao['DEFEITO CONSTATADO'] = df_classificacao['DEFEITO_CONSTATADO_PADRAO'].fillna(df_classificacao['DEFEITO CONSTATADO'])

    # Limpeza de colunas auxiliares
    df_classificacao.drop(columns=['DEFEITO_CONSTATADO_PADRAO', 'CATEGORIA_DEFEITO_NOVA'], inplace=True)

    # Salva o resultado final
    df_classificacao.to_excel(arquivo_saida, index=False)
    print(f"Sucesso! Arquivo de motivos únicos salvo em: {arquivo_saida}")

if __name__ == "__main__":
    processar_de_para_direto()