import pandas as pd
import warnings
import os

# Silenciar avisos de validação do Excel
warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')

ARQUIVO_ENTRADA = 'PEDIDOS INVENTARIO 2026.xlsx'
ABAS_ALVO = ['ERIKHA', 'OPEN', 'ISABELLA', 'JOSIANE', 'LUCIANA', 'MARCELA', 'ROBERTA']

# Colunas na ordem correta, incluindo agora 'Tamanho'
COLUNAS_DESEJADAS = [
    'Data Pedido', 'Vendedor', 'Nome Parceiro', 'Cód.Produto', 
    'Desc. Produto', 'Tamanho', 'GRADE DISPONÍVEL', 'QTDE', 'Total'
]

def gerar_relatorio_final():
    if not os.path.exists(ARQUIVO_ENTRADA):
        print(f"Erro: O arquivo '{ARQUIVO_ENTRADA}' não foi encontrado.")
        return

    p_busca = input("Digite o Cód. Parceiro: ").strip()
    v_busca = input("Digite o nome do Vendedor: ").strip().upper()
    
    lista_frames = []

    print(f"\n🔍 Buscando dados para {p_busca} - {v_busca}...")

    for aba in ABAS_ALVO:
        try:
            df = pd.read_excel(ARQUIVO_ENTRADA, sheet_name=aba)
            
            # Normalização de tipos para a busca
            df['Cód. Parceiro'] = df['Cód. Parceiro'].astype(str).str.replace('.0', '', regex=False).str.strip()
            df['Vendedor'] = df['Vendedor'].astype(str).str.strip().str.upper()
            
            filtro = (df['Cód. Parceiro'] == p_busca) & (df['Vendedor'] == v_busca)
            
            # Filtramos garantindo que 'Tamanho' e as outras colunas existam
            cols_atuais = [c for c in COLUNAS_DESEJADAS if c in df.columns]
            df_filtrado = df.loc[filtro, cols_atuais].copy()
            
            if not df_filtrado.empty:
                lista_frames.append(df_filtrado)
                print(f"✅ Dados extraídos da aba: {aba}")

        except Exception as e:
            print(f"⚠️ Erro na aba {aba}: {e}")

    if lista_frames:
        df_consolidado = pd.concat(lista_frames, ignore_index=True)

        # Garantir cálculos numéricos para o rodapé
        df_consolidado['QTDE'] = pd.to_numeric(df_consolidado['QTDE'], errors='coerce').fillna(0)
        df_consolidado['Total'] = pd.to_numeric(df_consolidado['Total'], errors='coerce').fillna(0)

        # Linha de Rodapé com as somas
        soma_qtde = df_consolidado['QTDE'].sum()
        soma_total = df_consolidado['Total'].sum()

        linha_total = pd.DataFrame({
            'Data Pedido': ['TOTALIZADORES:'],
            'Vendedor': [''],
            'Nome Parceiro': [''],
            'Cód.Produto': [''],
            'Desc. Produto': [''],
            'Tamanho': [''], # Coluna Tamanho vazia no rodapé
            'GRADE DISPONÍVEL': [''],
            'QTDE': [soma_qtde],
            'Total': [soma_total]
        })

        df_final = pd.concat([df_consolidado, linha_total], ignore_index=True)

        # Resumo visual no console
        print("\n" + "="*50)
        print(f"SOMA QTDE: {int(soma_qtde)}")
        print(f"SOMA TOTAL: R$ {soma_total:,.2f}")
        print("="*50)

        nome_arquivo = f"Relatorio_{p_busca}_{v_busca}.xlsx"
        df_final.to_excel(nome_arquivo, index=False)
        print(f"\n✨ Arquivo pronto: {nome_arquivo}")
    else:
        print(f"\n❌ Nenhum registro encontrado para {p_busca} / {v_busca}.")

if __name__ == "__main__":
    gerar_relatorio_final()