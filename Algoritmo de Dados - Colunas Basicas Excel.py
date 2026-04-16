import pandas as pd
import os

# --- CONFIGURAÇÕES ---
CAMINHO_ARQUIVO = r'C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python\Expedidos - Histórico.xlsx'
NOME_ABA = 'EXPEDIDOS'

def consolidar_v2():
    try:
        if not os.path.exists(CAMINHO_ARQUIVO):
            raise FileNotFoundError(f"Arquivo não localizado: {CAMINHO_ARQUIVO}")
            
        print(f"📂 Lendo: {os.path.basename(CAMINHO_ARQUIVO)}...")

        df_raw = pd.read_excel(CAMINHO_ARQUIVO, sheet_name=NOME_ABA, header=None, engine='openpyxl')

        lista_final = []

        # Itera sobre as colunas de 2 em 2
        for col_idx in range(0, df_raw.shape[1], 2):
            
            # Captura a data/mês
            referencia_data = df_raw.iloc[0, col_idx]
            
            if pd.isna(referencia_data):
                continue

            # Percorre as linhas de dados (inicia na 3ª linha do Excel / Índice 2 do DF)
            for row_idx in range(2, len(df_raw)):
                cod_prod = df_raw.iloc[row_idx, col_idx]
                
                # Proteção caso a planilha termine em uma coluna ímpar (evita o index out of range)
                try:
                    qtd = df_raw.iloc[row_idx, col_idx + 1]
                except IndexError:
                    qtd = 0

                if pd.notna(cod_prod):
                    lista_final.append({
                        'DATA': referencia_data,
                        'CODPROD': int(cod_prod) if isinstance(cod_prod, (int, float)) else cod_prod,
                        'QUANTIDADE': qtd if pd.notna(qtd) else 0
                    })

        df_final = pd.DataFrame(lista_final)

        # --- AJUSTE AQUI: Salvar ANTES do return ---
        if not df_final.empty:
            arquivo_saida = 'Expedidos - Resultado.xlsx'
            df_final.to_excel(arquivo_saida, index=False, engine='openpyxl')
            print(f"✅ Consolidação concluída! Total de registros: {len(df_final)}")
            print(f"📤 Resultado salvo em: {os.path.abspath(arquivo_saida)}")
        else:
            print("⚠️ Nenhum dado encontrado para processar.")

        return df_final # Agora o return é a última coisa

    except Exception as e:
        erro_msg = str(e)
        if "does not support file format" in erro_msg:
            print("❌ ERRO: Arquivo corrompido ou temporário (~$). Feche o Excel.")
        elif "index out of range" in erro_msg:
            print("❌ ERRO: A estrutura de colunas da planilha mudou.")
        else:
            print(f"⚠️ Erro inesperado: {e}")
        return None

if __name__ == "__main__":
    df = consolidar_v2()
    if df is not None and not df.empty:
        print("\n--- Primeiras 10 linhas do resultado ---")
        print(df.head(10))