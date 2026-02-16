import pandas as pd
import os
from openpyxl import load_workbook
from openpyxl.drawing.image import Image as OpenpyxlImage
from openpyxl.styles import PatternFill, Font, Alignment
import time

# ==========================================================
# 🔧 CONFIGURAÇÕES E COLUNAS CHAVE
# ==========================================================

COLUNAS_CHAVE = {
    'COD_PRODUTO': 'Cód. Produto',
    'REFERENCIA': 'Referência',
    'DESCRICAO': 'Descrição Produto',
    'CATEGORIA': 'GRUPO',
    'SUBGRUPO': 'SUBGRUPO',
    'COD_UF': 'COD. UF',
    'CHAVE_MMM': 'CHAVE_MMM',
    'QUANTIDADE': 'Quantidade',
    'VALOR': 'Faturado',
    'ESTOQUE': 'Estoque',

    # NOVAS COLUNAS PARA PESQUISA (nomes exatos do cabeçalho no Excel)
    'ESTADO_IS': 'ESTADO/IS',
    'REGIAO': 'REGIÃO',
}

DIRETORIO_BASE = os.path.abspath(r'C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\Scripts Python')
ARQUIVO_BASE_SANKHYA = os.path.join(DIRETORIO_BASE, 'Base de Produtos Mais Vendidos.xlsx')
DIRETORIO_IMAGENS = os.path.join(DIRETORIO_BASE, 'IMAGENS')

SUBGRUPOS_EXCLUIDOS = ['CHINELO', 'SACOLAS', 'REVENDA', 'MARY KAY']

# ==========================================================
# 🚀 FUNÇÃO PRINCIPAL
# ==========================================================

def consolidar_relatorio(filtros: dict):

    # 1) Ler a base
    try:
        df_bruto = pd.read_excel(ARQUIVO_BASE_SANKHYA, sheet_name='BASE PRODUTOS')
        print(f"Base Sankhya lida com sucesso. Total de {len(df_bruto)} linhas.")
    except PermissionError as e:
        print(f"❌ ERRO de permissão: {e}\nFeche o arquivo no Excel/OneDrive e rode novamente.")
        return
    except FileNotFoundError:
        print(f"❌ ERRO: Arquivo não encontrado em:\n{ARQUIVO_BASE_SANKHYA}")
        return
    except Exception as e:
        print(f"❌ ERRO ao ler planilha: {e}")
        return

    df_bruto.columns = df_bruto.columns.str.strip()

    # 2) Renomear colunas para nomes internos
    mapa_renomeacao = {
        COLUNAS_CHAVE['COD_PRODUTO']: 'CODIGO_SANKHYA',
        COLUNAS_CHAVE['REFERENCIA']: 'REFERENCIA_PRODUTO',
        COLUNAS_CHAVE['DESCRICAO']: 'DESCRICAO_PRODUTO',
        COLUNAS_CHAVE['CATEGORIA']: 'CATEGORIA',
        COLUNAS_CHAVE['SUBGRUPO']: 'SUBGRUPO',
        COLUNAS_CHAVE['COD_UF']: 'COD_UF',
        COLUNAS_CHAVE['CHAVE_MMM']: 'CHAVE_MMM',
        COLUNAS_CHAVE['QUANTIDADE']: 'QTD_VENDIDA',
        COLUNAS_CHAVE['VALOR']: 'VALOR_TOTAL_VENDA',
        COLUNAS_CHAVE['ESTADO_IS']: 'INSIDE_SALES',
        COLUNAS_CHAVE['REGIAO']: 'REGIAO',
        COLUNAS_CHAVE['ESTOQUE']: 'ESTOQUE'
    }
    df_bruto.rename(columns=mapa_renomeacao, inplace=True)

    # 2.1) Fallback de posição
    if 'REGIAO' not in df_bruto.columns and len(df_bruto.columns) >= 20:
        df_bruto.rename(columns={df_bruto.columns[19]: 'REGIAO'}, inplace=True)
    if 'INSIDE_SALES' not in df_bruto.columns and len(df_bruto.columns) >= 21:
        df_bruto.rename(columns={df_bruto.columns[20]: 'INSIDE_SALES'}, inplace=True)

    # 3) Garantias de tipos/padronização
    if 'CODIGO_SANKHYA' in df_bruto.columns:
        df_bruto['CODIGO_SANKHYA'] = df_bruto['CODIGO_SANKHYA'].astype(str).str.strip()

    for col in ['QTD_VENDIDA', 'VALOR_TOTAL_VENDA']:
        if col in df_bruto.columns:
            df_bruto[col] = pd.to_numeric(df_bruto[col], errors='coerce').fillna(0)

    # AJUSTE: Padronização rigorosa de todas as colunas de texto antes de filtrar
    for col in ['CATEGORIA', 'SUBGRUPO', 'COD_UF', 'REGIAO', 'INSIDE_SALES', 'CHAVE_MMM']:
        if col in df_bruto.columns:
            df_bruto[col] = df_bruto[col].astype(str).str.upper().str.strip()

    # 4) Exclusão de subgrupos (AJUSTADO PARA SER INFALÍVEL)
    df_filtrado = df_bruto.copy()
    if SUBGRUPOS_EXCLUIDOS:
        # Prepara a lista de exclusão
        excluir = [str(s).upper().strip() for s in SUBGRUPOS_EXCLUIDOS]
        if 'SUBGRUPO' in df_filtrado.columns:
            antes = len(df_filtrado)
            # Remove se o nome for idêntico ou se contiver o termo (ex: "CHINELO FEMININO")
            for termo in excluir:
                df_filtrado = df_filtrado[~df_filtrado['SUBGRUPO'].str.contains(termo, na=False)]
            print(f"→ Exclusão aplicada: {antes - len(df_filtrado)} linhas removidas ({', '.join(excluir)}).")

    # 5) Filtros interativos
    for coluna, valor in filtros.items():
        if valor and coluna in df_filtrado.columns:
            v = valor.upper().strip()
            print(f"→ Filtro: {coluna} = {v}")
            df_filtrado = df_filtrado[df_filtrado[coluna] == v]

    # 5.1) Filtro por estoque
    opcao_estoque = input("\nDeseja excluir produtos sem estoque? (S/N): ").strip().upper()
    if opcao_estoque == "S" and 'ESTOQUE' in df_filtrado.columns:
        antes = len(df_filtrado)
        df_filtrado = df_filtrado[df_filtrado['ESTOQUE'] > 0]
        print(f"→ Estoque aplicado: {antes - len(df_filtrado)} produtos removidos.")
        
    # 6) Consolidação e ranking
    df_agrup = df_filtrado.groupby(
        ['REFERENCIA_PRODUTO', 'CODIGO_SANKHYA', 'DESCRICAO_PRODUTO', 'CATEGORIA']
    ).agg(VENDAS_TOTAIS=('QTD_VENDIDA', 'sum')).reset_index()

    df_agrup['RANK_CATEGORIA'] = df_agrup.groupby('CATEGORIA')['VENDAS_TOTAIS'].rank(
        method='dense', ascending=False
    ).astype(int)

    df_top10 = (
        df_agrup[df_agrup['RANK_CATEGORIA'] <= 10]
        .sort_values(by=['CATEGORIA', 'RANK_CATEGORIA', 'VENDAS_TOTAIS'], ascending=[True, True, False])
        .groupby('CATEGORIA')
        .head(10)
        .reset_index(drop=True)
    )

    if 'CODIGO_SANKHYA' in df_top10.columns:
        df_top10['CODIGO_SANKHYA'] = df_top10['CODIGO_SANKHYA'].astype(str).str.strip()

    print(f"✅ Relatório final pronto. Total de {len(df_top10)} produtos (Top 10 por categoria).")

    exportar_e_formatar(df_top10, filtros)

def exportar_e_formatar(df_dados, filtros):
    uf = filtros.get('COD_UF', '') or filtros.get('REGIAO', '') or filtros.get('INSIDE_SALES', 'GERAL')
    cat = filtros.get('CATEGORIA', 'TODAS')
    nome_arquivo = f"Relatorio_Top10_{uf}_{cat}_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
    caminho_saida = os.path.join(DIRETORIO_BASE, nome_arquivo)

    df_export = df_dados.copy()
    df_export['Rótulos de Linha'] = df_export['REFERENCIA_PRODUTO']
    df_export['Cod. Sankhya']    = df_export['CODIGO_SANKHYA']
    df_export['Descrição']       = df_export['DESCRICAO_PRODUTO']
    df_export['Ranking']         = df_export['RANK_CATEGORIA']
    df_export['VENDAS_TOTAIS']   = df_export['VENDAS_TOTAIS']

    colunas_finais = ['Rótulos de Linha', 'Cod. Sankhya', 'Descrição', 'Ranking', 'VENDAS_TOTAIS']
    df_export = df_export[colunas_finais]

    with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
        df_export.to_excel(writer, sheet_name='Top_10_Consolidado', index=False, startrow=1)

    try:
        wb = load_workbook(caminho_saida)
        ws = wb['Top_10_Consolidado']
        ws.views.sheetView[0].showGridlines = False

        style_center = Alignment(horizontal='center', vertical='center', wrap_text=True)
        font_titulo = Font(color="FFFFFFFF", bold=True, size=14)
        cor_preta   = '00000000'

        ws.insert_cols(4)
        ws.cell(row=2, column=4, value="Foto").alignment = style_center

        titulo = f"TOP 10 - {cat.upper()} MAIS VENDIDOS - {uf.upper()}"
        ws.merge_cells('A1:F1')
        titulo_cell = ws['A1']
        titulo_cell.value = titulo
        titulo_cell.fill = PatternFill(start_color=cor_preta, end_color=cor_preta, fill_type="solid")
        titulo_cell.font = font_titulo
        titulo_cell.alignment = Alignment(horizontal='center', vertical='center')

        larguras = {'A': 15.09, 'B': 15.09, 'C': 42.09, 'D': 20, 'E': 15.09, 'F': 15.09}
        for col, width in larguras.items():
            ws.column_dimensions[col].width = width

        ws.row_dimensions[1].height = 19
        ws.row_dimensions[2].height = 19
        
        # AJUSTE: Altura das linhas para todas as linhas de dados do Top 10
        for i in range(3, len(df_dados) + 3):
            ws.row_dimensions[i].height = 80

        print("\n[SUCESSO] Inserindo imagens...")
        for index, row in df_dados.iterrows():
            codigo = str(row['CODIGO_SANKHYA']).strip()
            row_excel = index + 3

            img_path = None
            for ext in ['.jpg', '.png', '.jpeg']:
                p = os.path.join(DIRETORIO_IMAGENS, f"{codigo}{ext}")
                if os.path.exists(p):
                    img_path = p
                    break

            if img_path:
                try:
                    img = OpenpyxlImage(img_path)
                    img.width, img.height = 100, 100
                    ws.add_image(img, f'D{row_excel}')
                except Exception as img_e:
                    ws.cell(row=row_excel, column=4, value="ERRO").alignment = style_center
            else:
                ws.cell(row=row_excel, column=4, value="N/A").alignment = style_center

            for col_num in range(1, 7):
                ws.cell(row=row_excel, column=col_num).alignment = style_center

        wb.save(caminho_saida)
        print(f"[SUCESSO] Relatório: {nome_arquivo}")

    except Exception as e:
        print(f"❌ Erro ao formatar: {e}")

if __name__ == "__main__":
    print("\n--- TIPO DE PESQUISA ---")
    print("1 - Por Estado (UF) | 2 - Por Região | 3 - Por Inside Sales")
    opcao = input("Selecione: ").strip()
    filtros = {}
    if opcao == "1":
        filtros = {'COD_UF': input("UF: ").strip(), 'CATEGORIA': input("GRUPO: ").strip()}
    elif opcao == "2":
        filtros = {'REGIAO': input("Região: ").strip(), 'CATEGORIA': input("GRUPO: ").strip()}
    elif opcao == "3":
        filtros = {'INSIDE_SALES': input("Vendedora: ").strip(), 'CATEGORIA': input("GRUPO: ").strip()}
    
    consolidar_relatorio(filtros)