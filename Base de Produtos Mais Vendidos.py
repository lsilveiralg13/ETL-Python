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

    # NOVAS COLUNAS PARA PESQUISA (nomes exatos do cabeçalho no Excel)
    'ESTADO_IS': 'ESTADO/IS',   # Coluna que contém o Inside Sales vinculado ao Estado
    'REGIAO': 'REGIÃO'          # Coluna de Região (NORTE, SUL, etc.)
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
        COLUNAS_CHAVE['REGIAO']: 'REGIAO'
    }
    df_bruto.rename(columns=mapa_renomeacao, inplace=True)

    # 2.1) Fallback de posição (T=20, U=21) se, por alguma razão, REGIAO/INSIDE_SALES não vierem com o nome esperado
    if 'REGIAO' not in df_bruto.columns and len(df_bruto.columns) >= 20:
        df_bruto.rename(columns={df_bruto.columns[19]: 'REGIAO'}, inplace=True)   # T = índice 19 (base 0)
    if 'INSIDE_SALES' not in df_bruto.columns and len(df_bruto.columns) >= 21:
        df_bruto.rename(columns={df_bruto.columns[20]: 'INSIDE_SALES'}, inplace=True)  # U = índice 20 (base 0)

    # 3) Garantias de tipos/padronização
    if 'CODIGO_SANKHYA' in df_bruto.columns:
        df_bruto['CODIGO_SANKHYA'] = df_bruto['CODIGO_SANKHYA'].astype(str).str.strip()

    for col in ['QTD_VENDIDA', 'VALOR_TOTAL_VENDA']:
        if col in df_bruto.columns:
            df_bruto[col] = pd.to_numeric(df_bruto[col], errors='coerce').fillna(0)

    for col in ['CATEGORIA', 'SUBGRUPO', 'COD_UF', 'REGIAO', 'INSIDE_SALES', 'CHAVE_MMM']:
        if col in df_bruto.columns:
            df_bruto[col] = df_bruto[col].astype(str).str.upper().str.strip()

    # 4) Exclusão de subgrupos
    df_filtrado = df_bruto.copy()
    if SUBGRUPOS_EXCLUIDOS:
        excluir = [s.upper().strip() for s in SUBGRUPOS_EXCLUIDOS]
        if 'SUBGRUPO' in df_filtrado.columns:
            antes = len(df_filtrado)
            df_filtrado = df_filtrado[~df_filtrado['SUBGRUPO'].isin(excluir)]
            print(f"→ Exclusão aplicada: {antes - len(df_filtrado)} linhas removidas ({', '.join(SUBGRUPOS_EXCLUIDOS)}).")

    # 5) Filtros interativos (UF, REGIAO, INSIDE_SALES, CATEGORIA)
    for coluna, valor in filtros.items():
        if valor and coluna in df_filtrado.columns:
            v = valor.upper().strip()
            print(f"→ Filtro: {coluna} = {v}")
            df_filtrado = df_filtrado[df_filtrado[coluna] == v]

    print(f"Base após filtros: {len(df_filtrado)} linhas.")
    if df_filtrado.empty:
        print("⚠️ Nenhum dado encontrado após aplicar filtros.")
        return

    # 6) Consolidação e ranking Top 10 por categoria
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

# ==========================================================
# 🎨 EXPORTAÇÃO E FORMATAÇÃO (com imagens)
# ==========================================================

def exportar_e_formatar(df_dados, filtros):
    # Construção de nome do arquivo (usa UF, REGIAO ou INSIDE_SALES para dar contexto)
    uf = filtros.get('COD_UF', '') or filtros.get('REGIAO', '') or filtros.get('INSIDE_SALES', 'GERAL')
    cat = filtros.get('CATEGORIA', 'TODAS')
    nome_arquivo = f"Relatorio_Top10_{uf}_{cat}_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
    caminho_saida = os.path.join(DIRETORIO_BASE, nome_arquivo)

    # Prepara DataFrame de exportação (mantendo a estrutura que você usa)
    df_export = df_dados.copy()
    df_export['Rótulos de Linha'] = df_export['REFERENCIA_PRODUTO']
    df_export['Cod. Sankhya']    = df_export['CODIGO_SANKHYA']
    df_export['Descrição']       = df_export['DESCRICAO_PRODUTO']
    df_export['Ranking']         = df_export['RANK_CATEGORIA']
    df_export['VENDAS_TOTAIS']   = df_export['VENDAS_TOTAIS']

    colunas_finais = ['Rótulos de Linha', 'Cod. Sankhya', 'Descrição', 'Ranking', 'VENDAS_TOTAIS']
    df_export = df_export[colunas_finais]

    # Exporta a partir da linha 2 (linha 1 será título)
    with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
        df_export.to_excel(writer, sheet_name='Top_10_Consolidado', index=False, startrow=1)

    try:
        wb = load_workbook(caminho_saida)
        ws = wb['Top_10_Consolidado']

        # Remover gridlines
        try:
            ws.views.sheetView[0].showGridlines = False
        except Exception:
            pass

        style_center = Alignment(horizontal='center', vertical='center', wrap_text=True)
        font_titulo = Font(color="FFFFFFFF", bold=True, size=14)
        cor_preta   = '00000000'

        # Inserir coluna Foto na posição D
        ws.insert_cols(4)
        ws.cell(row=2, column=4, value="Foto").alignment = style_center

        # Título principal na linha 1
        titulo = f"TOP 10 - {cat.upper()} MAIS VENDIDOS - {uf.upper()}"
        ws.merge_cells('A1:F1')
        titulo_cell = ws['A1']
        titulo_cell.value = titulo
        titulo_cell.fill = PatternFill(start_color=cor_preta, end_color=cor_preta, fill_type="solid")
        titulo_cell.font = font_titulo
        titulo_cell.alignment = Alignment(horizontal='center', vertical='center')

        # Ajuste de larguras
        larguras = {'A': 15.09, 'B': 15.09, 'C': 42.09, 'D': 20, 'E': 15.09, 'F': 15.09}
        for col, width in larguras.items():
            ws.column_dimensions[col].width = width

        # Ajuste de alturas
        ws.row_dimensions[1].height = 19
        ws.row_dimensions[2].height = 19
        # Linhas dos 10 itens (3 a 12)
        for row_num in range(3, 13):
            ws.row_dimensions[row_num].height = 50

        # Centralização de cabeçalhos e dados
        for col_num in range(1, 7):  # A..F
            ws.cell(row=2, column=col_num).alignment = style_center
        for row_num in range(3, 13):
            for col_num in range(1, 7):
                ws.cell(row=row_num, column=col_num).alignment = style_center

        # Inserção de imagens (procura por .jpg/.png/.jpeg)
        print("\n[SUCESSO] Relatório tabular exportado. Inserindo imagens...")
        for index, row in df_dados.iterrows():
            codigo = str(row['CODIGO_SANKHYA']).strip()

            # Caminhos prováveis
            p_jpg  = os.path.join(DIRETORIO_IMAGENS, f"{codigo}.jpg")
            p_png  = os.path.join(DIRETORIO_IMAGENS, f"{codigo}.png")
            p_jpeg = os.path.join(DIRETORIO_IMAGENS, f"{codigo}.jpeg")

            img_path = None
            if os.path.exists(p_jpg):
                img_path = p_jpg
            elif os.path.exists(p_png):
                img_path = p_png
            elif os.path.exists(p_jpeg):
                img_path = p_jpeg

            # Diagnóstico parcial
            if index < 5:
                status = "ENCONTRADA" if img_path else "NÃO ENCONTRADA"
                print(f"  DIAGNÓSTICO ({codigo}): Imagem {status} | jpg/png/jpeg")

            row_excel = index + 3  # Dados iniciam na linha 3
            if img_path:
                try:
                    img = OpenpyxlImage(img_path)
                    img.width = 80
                    img.height = 80
                    ws.add_image(img, f'D{row_excel}')
                except Exception as img_e:
                    ws.cell(row=row_excel, column=4, value="ERRO INSERÇÃO").alignment = style_center
                    print(f"  ERRO ao inserir imagem {codigo}: {img_e}")
            else:
                ws.cell(row=row_excel, column=4, value="Img. não encontrada").alignment = style_center

        wb.save(caminho_saida)
        print(f"[SUCESSO] Exportação final concluída: {caminho_saida}")

    except PermissionError as e:
        print(f"❌ ERRO: Não foi possível salvar. Arquivo aberto? {e}")
    except Exception as e:
        print(f"❌ Erro ao manipular Excel: {e}")

# ==========================================================
# 🧭 EXECUÇÃO INTERATIVA
# ==========================================================

if __name__ == "__main__":
    os.makedirs(DIRETORIO_IMAGENS, exist_ok=True)
    print("\n--- TIPO DE PESQUISA ---")
    print("1 - Por Estado (UF)")
    print("2 - Por Região")
    print("3 - Por Inside Sales")
    opcao = input("Selecione (1, 2 ou 3): ").strip()

    filtros = {}
    if opcao == "1":
        uf = input("Informe o Código UF (Ex: SP, RJ). Deixe vazio p/ todas: ").strip()
        cat = input("Informe o GRUPO (Ex: CALÇADOS, ACESSÓRIOS). Deixe vazio p/ todos: ").strip()
        filtros = {'COD_UF': uf, 'CATEGORIA': cat}
    elif opcao == "2":
        regiao = input("Informe a REGIÃO (NORTE, NORDESTE, CENTRO-OESTE, SUDESTE, SUL). Deixe vazio p/ todas: ").strip()
        cat = input("Informe o GRUPO (Ex: CALÇADOS, ACESSÓRIOS). Deixe vazio p/ todos: ").strip()
        filtros = {'REGIAO': regiao, 'CATEGORIA': cat}
    elif opcao == "3":
        isales = input("Informe o nome do Inside Sales (Ex: MARCELAVAZ, JOSIANEVIEIRA). Deixe vazio p/ todos: ").strip()
        cat = input("Informe o GRUPO (Ex: CALÇADOS, ACESSÓRIOS). Deixe vazio p/ todos: ").strip()
        filtros = {'INSIDE_SALES': isales, 'CATEGORIA': cat}
    else:
        print("❌ Opção inválida.")
        raise SystemExit

    print(f"\n→ Aplicando exclusão automática dos subgrupos: {', '.join(SUBGRUPOS_EXCLUIDOS)}")
    consolidar_relatorio(filtros)
