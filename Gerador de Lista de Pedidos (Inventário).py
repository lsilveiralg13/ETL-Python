import pandas as pd
import warnings
import os

# Silenciar avisos
warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')

ARQUIVO_ENTRADA = 'PEDIDOS INVENTARIO 2026.xlsx'
ABAS_ALVO = ['ERIKHA', 'OPEN', 'ISABELLA', 'JOSIANE', 'LUCIANA', 'MARCELA', 'ROBERTA']
COLUNAS_DESEJADAS = [
    'Data Pedido', 'Vendedor', 'Nome Parceiro', 'Cód.Produto', 
    'Desc. Produto', 'GRADE DISPONÍVEL', 'QTDE', 'Total'
]

def localizar_e_filtrar(df, cod_busca, vend_busca):
    # 1. Tentar encontrar a linha de cabeçalho correta se as colunas não estiverem no topo
    # Se 'Cód. Parceiro' não estiver nas colunas atuais, procuramos nas primeiras linhas
    if 'Cód. Parceiro' not in df.columns:
        for i, row in df.head(10).iterrows():
            if 'Cód. Parceiro' in row.values:
                df.columns = row
                df = df.iloc[i+1:].reset_index(drop=True)
                break
    
    # Garantir que as colunas de filtro existem após a tentativa de correção
    if 'Cód. Parceiro' in df.columns and 'Vendedor' in df.columns:
        # Normalização rigorosa
        df['Cód. Parceiro'] = df['Cód. Parceiro'].astype(str).str.strip().str.replace('.0', '', regex=False)
        df['Vendedor'] = df['Vendedor'].astype(str).str.strip().str.upper()
        
        filtro = (df['Cód. Parceiro'] == str(cod_busca).strip()) & \
                 (df['Vendedor'] == str(vend_busca).strip().upper())
        
        # Filtrar apenas as colunas que realmente existem para evitar erro de KeyError
        cols_existentes = [c for c in COLUNAS_DESEJADAS if c in df.columns]
        return df.loc[filtro, cols_existentes]
    
    return pd.DataFrame()

def gerar_relatorio_robusto():
    if not os.path.exists(ARQUIVO_ENTRADA):
        print(f"Erro: Arquivo {ARQUIVO_ENTRADA} não encontrado no diretório.")
        return

    p_busca = input("Digite o Cód. Parceiro: ")
    v_busca = input("Digite o nome do Vendedor: ")
    
    lista_resultados = []

    print("\n🔍 Vasculhando abas...")
    for aba in ABAS_ALVO:
        try:
            # Lemos a aba sem definir cabeçalho inicialmente para podermos tratar deslocamentos
            df_aba = pd.read_excel(ARQUIVO_ENTRADA, sheet_name=aba, header=None if "Sankhya" in aba else 0)
            
            # Se lemos sem header, a primeira linha de dados vira a coluna. Ajustamos:
            if isinstance(df_aba.columns[0], int): 
                # Tenta achar onde está escrito 'Cód. Parceiro'
                for i in range(len(df_aba)):
                    if 'Cód. Parceiro' in df_aba.iloc[i].values:
                        df_aba.columns = df_aba.iloc[i]
                        df_aba = df_aba.iloc[i+1:].reset_index(drop=True)
                        break
            
            res = localizar_e_filtrar(df_aba, p_busca, v_busca)
            if not res.empty:
                lista_resultados.append(res)
                print(f"✅ Dados encontrados na aba: {aba}")
        except Exception as e:
            print(f"⚠️ Erro ao ler aba {aba}: {e}")

    if lista_resultados:
        df_final = pd.concat(lista_resultados, ignore_index=True)
        
        # Conversão de tipos para cálculo
        df_final['Total'] = pd.to_numeric(df_final['Total'], errors='coerce').fillna(0)
        df_final['QTDE'] = pd.to_numeric(df_final['QTDE'], errors='coerce').fillna(0)
        
        total_valor = df_final['Total'].sum()
        total_qtd = df_final['QTDE'].sum()

        print("\n" + "=".center(50, "="))
        print("RELATÓRIO SINTÉTICO".center(50))
        print(f"PARCEIRO: {p_busca} | VENDEDOR: {v_busca.upper()}")
        print(f"TOTAL DE ITENS: {int(total_qtd)}")
        print(f"VALOR TOTAL: R$ {total_valor:,.2f}")
        print("=".center(50, "="))

        arquivo_saida = f"RELATORIO_{p_busca}_{v_busca.upper()}.xlsx"
        df_final.to_excel(arquivo_saida, index=False)
        print(f"\n✨ Sucesso! Relatório gerado: {arquivo_saida}")
    else:
        print(f"\n❌ Fim da busca. Nenhum dado localizado para {p_busca} / {v_busca}.")

if __name__ == "__main__":
    gerar_relatorio_robusto()