import pandas as pd
import os
from openpyxl import load_workbook
from openpyxl.drawing.image import Image as OpenpyxlImage
from openpyxl.utils import get_column_letter
from openpyxl.styles import PatternFill, Font, Alignment, Side, Border
import time

# --- CONFIGURAÇÕES E COLUNAS CHAVE ---

# O Python usará a chave (esquerda) no código e o valor (direita) para buscar na sua planilha.
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
}

# Diretórios e Arquivos
# CORREÇÃO FINAL PARA AMBIENTES ONEDRIVE/REDE: 
DIRETORIO_BASE = os.path.abspath(r'C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\Scripts Python')

ARQUIVO_BASE_SANKHYA = os.path.join(DIRETORIO_BASE, 'Base de Produtos Mais Vendidos.xlsx') 
DIRETORIO_IMAGENS = os.path.join(DIRETORIO_BASE, 'IMAGENS')

# CRITÉRIOS DE EXCLUSÃO (SUBGRUPOS)
SUBGRUPOS_EXCLUIDOS = ['CHINELO', 'SACOLAS', 'REVENDA']

# ------------------------------------

def consolidar_relatorio(filtros: dict):
    
    # 1. LER A BASE BRUTA DO SANKHYA
    try:
        # Lendo a aba específica 'BASE PRODUTOS'
        df_bruto = pd.read_excel(ARQUIVO_BASE_SANKHYA, sheet_name='BASE PRODUTOS')
        print(f"Base Sankhya lida com sucesso. Total de {len(df_bruto)} linhas da aba 'BASE PRODUTOS'.")
    except FileNotFoundError:
        print(f"ERRO: Arquivo base Sankhya não encontrado em {ARQUIVO_BASE_SANKHYA}.")
        return
    except ValueError as e:
        if "Worksheet named 'BASE PRODUTOS' not found" in str(e):
            print(f"ERRO: A aba 'BASE PRODUTOS' não foi encontrada no arquivo Excel. Verifique o nome EXATO.")
        else:
            print(f"ERRO ao ler a planilha: {e}")
        return
    except Exception as e:
        print(f"ERRO ao ler a planilha: {e}")
        return

    # CORREÇÃO CRÍTICA: Remover espaços de todos os cabeçalhos
    df_bruto.columns = df_bruto.columns.str.strip() 

    # 2. RENOMEAR E GARANTIR TIPOS
    
    # Cria o mapa de renomeação: {Nome Antigo da Planilha: Novo Nome Interno}
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
    }
    
    # Executa a renomeação
    df_bruto = df_bruto.rename(columns=mapa_renomeacao)

    # 3. VERIFICAÇÃO FINAL DE COLUNAS CRÍTICAS
    colunas_validas = ['QTD_VENDIDA', 'VALOR_TOTAL_VENDA', 'SUBGRUPO', 'COD_UF', 'CATEGORIA', 'CODIGO_SANKHYA']
    for col in colunas_validas:
        if col not in df_bruto.columns:
            nome_original_falha = next((k for k, v in mapa_renomeacao.items() if v == col), 'NÃO ENCONTRADO')
            
            print(f"\nERRO FATAL: Coluna '{col}' não encontrada após renomeação.")
            print(f"O nome da coluna de entrada no Excel que o Pandas tentou buscar era: '{nome_original_falha}'")
            print("Verifique se o nome original está escrito EXATAMENTE no dicionário COLUNAS_CHAVE e na planilha.")
            return

    # Garante que a coluna CODIGO_SANKHYA seja string para o nome do arquivo
    df_bruto['CODIGO_SANKHYA'] = df_bruto['CODIGO_SANKHYA'].astype(str).str.strip()

    # Garantias numéricas
    df_bruto['QTD_VENDIDA'] = pd.to_numeric(df_bruto['QTD_VENDIDA'], errors='coerce').fillna(0)
    df_bruto['VALOR_TOTAL_VENDA'] = pd.to_numeric(df_bruto['VALOR_TOTAL_VENDA'], errors='coerce').fillna(0)
    
    # Converte colunas de filtro para string e remove espaços, padroniza para maiúsculas
    for col in ['COD_UF', 'CATEGORIA', 'SUBGRUPO', 'CHAVE_MMM']:
         if col in df_bruto.columns:
             df_bruto[col] = df_bruto[col].astype(str).str.upper().str.strip()

    # 4. APLICAÇÃO DOS FILTROS DE EXCLUSÃO (Subgrupos)
    
    df_filtrado = df_bruto.copy()
    
    if SUBGRUPOS_EXCLUIDOS:
        subgrupos_upper = [s.upper().strip() for s in SUBGRUPOS_EXCLUIDOS]
        df_filtrado = df_filtrado[~df_filtrado['SUBGRUPO'].isin(subgrupos_upper)]
        print(f"-> Filtro de Exclusão aplicado: {len(df_bruto) - len(df_filtrado)} linhas removidas (Subgrupos: {', '.join(SUBGRUPOS_EXCLUIDOS)}).")

    # 5. APLICAÇÃO DOS FILTROS INTERATIVOS (UF e GRUPO)
    
    for coluna_filtro, valor_filtro in filtros.items():
        if valor_filtro:
            valor_filtro_upper = valor_filtro.upper().strip()
            
            print(f"-> Aplicando filtro Interativo: {coluna_filtro} = '{valor_filtro_upper}'")
            df_filtrado = df_filtrado[df_filtrado[coluna_filtro] == valor_filtro_upper]

    print(f"Base após todos os filtros (Exclusão e Interativos): {len(df_filtrado)} linhas restantes.")
    
    if df_filtrado.empty:
        print("AVISO: Nenhum dado encontrado após aplicar os filtros.")
        return
        
    # 6. CONSOLIDAÇÃO, AGRUPAMENTO E RANKING (Lógica de Desdobramento)
    
    # NOTA: RECEITA_TOTAL e VALOR_TOTAL_VENDA foram removidos do agrupamento final
    df_agrupado = df_filtrado.groupby(['REFERENCIA_PRODUTO', 'CODIGO_SANKHYA', 'DESCRICAO_PRODUTO', 'CATEGORIA']).agg(
        VENDAS_TOTAIS=('QTD_VENDIDA', 'sum'),
        # RECEITA_TOTAL=('VALOR_TOTAL_VENDA', 'sum') <-- REMOVIDO
    ).reset_index()
    
    # REMOVIDO o cálculo de PRECO_MEDIO_ITEM
    
    df_agrupado['RANK_CATEGORIA'] = df_agrupado.groupby('CATEGORIA')['VENDAS_TOTAIS'].rank(
        method='dense', 
        ascending=False
    ).astype(int)
    
    # Filtra para o Top 10 e ordena
    df_relatorio = df_agrupado[df_agrupado['RANK_CATEGORIA'] <= 10].sort_values(
        by=['CATEGORIA', 'RANK_CATEGORIA', 'VENDAS_TOTAIS'], # Adicionando VENDAS_TOTAIS como desempate secundário
        ascending=[True, True, False]
    ).reset_index(drop=True)
    
    # GARANTIA: Limita estritamente a 10 linhas por CATEGORIA
    df_relatorio = df_relatorio.groupby('CATEGORIA').head(10).reset_index(drop=True)

    # Garante que o CODIGO_SANKHYA na agregação ainda seja string para o nome do arquivo
    df_relatorio['CODIGO_SANKHYA'] = df_relatorio['CODIGO_SANKHYA'].astype(str).str.strip()
    
    print(f"Relatório final pronto. Total de {len(df_relatorio)} produtos no Top 10.")

    # 7. EXPORTAR E FORMATAR O EXCEL
    exportar_e_formatar(df_relatorio, filtros)


def exportar_e_formatar(df_dados, filtros):
    
    # Gera um nome de arquivo mais amigável, ignorando filtros vazios
    uf = filtros.get('COD_UF', 'GERAL') or 'GERAL'
    cat = filtros.get('CATEGORIA', 'TODAS') or 'TODAS'
    
    nome_filtro = f"{uf}_{cat}"
    
    ARQUIVO_SAIDA = os.path.join(DIRETORIO_BASE, f'Relatorio_Top10_{nome_filtro}_{time.strftime("%Y%m%d_%H%M%S")}.xlsx')
    
    df_export = df_dados.copy()
    
    df_export['Rótulos de Linha'] = df_export['REFERENCIA_PRODUTO']
    df_export['Cod. Sankhya'] = df_export['CODIGO_SANKHYA']
    df_export['Descrição'] = df_export['DESCRICAO_PRODUTO']
    df_export['Ranking'] = df_export['RANK_CATEGORIA']
    df_export['VENDAS_TOTAIS'] = df_export['VENDAS_TOTAIS'] # Mantém apenas esta coluna de métrica

    # Colunas finais ATUALIZADAS (sem RECEITA_TOTAL e PRECO_MEDIO_ITEM)
    colunas_finais = ['Rótulos de Linha', 'Cod. Sankhya', 'Descrição', 'Ranking', 'VENDAS_TOTAIS']
    df_export = df_export[colunas_finais]

    writer = pd.ExcelWriter(ARQUIVO_SAIDA, engine='openpyxl')
    # Exporta os dados a partir da Linha 2, deixando a Linha 1 livre para o Cabeçalho principal
    df_export.to_excel(writer, sheet_name='Top_10_Consolidado', index=False, startrow=1) 
    writer.close()

    try:
        wb = load_workbook(ARQUIVO_SAIDA)
        ws = wb['Top_10_Consolidado']
        
        # --- DEFINIÇÃO DE ESTILOS ---
        
        # Estilo para Centralização (Vertical e Horizontal) e Quebra de Linha
        style_center = Alignment(horizontal='center', vertical='center', wrap_text=True)
        
        # Cor preta sólida para o cabeçalho principal
        cor_preta = '00000000' 
        
        # Estilo de fonte para o cabeçalho principal
        font_titulo = Font(color="FFFFFFFF", bold=True, size=14)
        
        # 1. REMOVER LINHAS DE GRADE
        ws.views.sheetView[0].showGridlines = False

        # 2. INSERÇÃO DA COLUNA FOTO E AJUSTE DE CABEÇALHO (que agora começa na linha 2)
        
        # A nova coluna Foto será a 4ª coluna (D)
        ws.insert_cols(4) 
        foto_header_cell = ws.cell(row=2, column=4, value="Foto") 
        foto_header_cell.alignment = style_center # Centraliza o cabeçalho "Foto"
        
        # 3. CABEÇALHO PRINCIPAL (TOP 10)
        
        # Define o título dinâmico
        titulo = f"TOP 10 - {cat.upper()} MAIS VENDIDOS - {uf.upper()}"
        
        # A última coluna do relatório é a F (já que as colunas G e H foram removidas)
        ws.merge_cells('A1:F1')
        titulo_cell = ws['A1']
        titulo_cell.value = titulo
        
        # Aplica o estilo do título
        titulo_cell.fill = PatternFill(start_color=cor_preta, end_color=cor_preta, fill_type="solid")
        titulo_cell.font = font_titulo 
        titulo_cell.alignment = Alignment(horizontal='center', vertical='center')
        
        # 4. AJUSTE DE LARGURA DAS COLUNAS (A, B, C, D, E, F)
        
        larguras = {
            'A': 15.09, 'B': 15.09, 'C': 42.09, 'D': 20, 
            'E': 15.09, 'F': 15.09
        }
        
        for col, width in larguras.items():
            ws.column_dimensions[col].width = width
            
        # 5. AJUSTE DE ALTURA DAS LINHAS
        
        # Linhas 1 e 2 (Cabeçalhos)
        ws.row_dimensions[1].height = 19
        ws.row_dimensions[2].height = 19
        
        # Linhas 3 a 12 (Dados dos 10 itens)
        for row_num in range(3, 13): 
            ws.row_dimensions[row_num].height = 50
        
        # 6. CENTRALIZAÇÃO DE TODO O CONTEÚDO DA TABELA
        
        # Aplica centralização aos cabeçalhos (Linha 2)
        for col_num in range(1, 7): # A até F
            cell = ws.cell(row=2, column=col_num)
            cell.alignment = style_center

        # Aplica centralização aos dados (Linhas 3 a 12)
        for row_num in range(3, 13):
            for col_num in range(1, 7): # A até F
                cell = ws.cell(row=row_num, column=col_num)
                cell.alignment = style_center
                
        # 7. INSERÇÃO DAS IMAGENS (com ajuste para a Linha 3 em diante)
        
        print(f"\n[SUCESSO] Relatório de tabela exportado. Adicionando imagens...")
        
        # O loop começa na Linha 3 do Excel (índice 2 do openpyxl)
        for index, row in df_dados.iterrows():
            codigo = str(row['CODIGO_SANKHYA']).strip() 
            
            # ... (Lógica de busca de caminho_img_final inalterada) ...
            caminho_img_jpg = os.path.join(DIRETORIO_IMAGENS, f"{codigo}.jpg")
            caminho_img_png = os.path.join(DIRETORIO_IMAGENS, f"{codigo}.png")
            caminho_img_jpeg = os.path.join(DIRETORIO_IMAGENS, f"{codigo}.jpeg")
            
            caminho_img_final = None
            
            if os.path.exists(caminho_img_jpg):
                caminho_img_final = caminho_img_jpg
            elif os.path.exists(caminho_img_png):
                caminho_img_final = caminho_img_png
            elif os.path.exists(caminho_img_jpeg):
                caminho_img_final = caminho_img_jpeg

            # DIAGNÓSTICO (FORÇADA)
            if index < 5: 
                 status = "ENCONTRADA" if caminho_img_final else "NÃO ENCONTRADA"
                 print(f"  DIAGNÓSTICO ({codigo}): Imagem {status}. Caminho de Busca: {caminho_img_png} | Caminho Final: {caminho_img_final}")
                 
            # Note que row_excel = index + 3 (1 do cabeçalho principal + 1 do cabeçalho da tabela + índice 0-base)
            row_excel = index + 3
            
            if caminho_img_final:
                try:
                    img = OpenpyxlImage(caminho_img_final)
                    img.width = 80
                    img.height = 80
                    
                    cell_ref = f'D{row_excel}' 
                    ws.add_image(img, cell_ref)
                except Exception as img_e:
                    ws.cell(row=row_excel, column=4, value="ERRO INSERÇÃO").alignment = style_center
                    print(f"  ERRO CRÍTICO NA INSERÇÃO da imagem {codigo}: {img_e}")
            else:
                ws.cell(row=row_excel, column=4, value="Img. não encontrada").alignment = style_center
                
        wb.save(ARQUIVO_SAIDA)
        print(f"[SUCESSO] Inserção de imagens e exportação final concluída: {ARQUIVO_SAIDA}")
        
    except Exception as e:
        print(f"Erro ao manipular o Excel para imagens: {e}")

# --- EXECUÇÃO DO SCRIPT COM FILTROS ---
if __name__ == "__main__":
    os.makedirs(DIRETORIO_IMAGENS, exist_ok=True) 
    print(f"Iniciando a automação de relatório. Diretório base: {DIRETORIO_BASE}")

    # COLETANDO INPUTS INTERATIVOS DO USUÁRIO
    
    print("\n--- INFORME OS FILTROS DE BUSCA ---")
    
    uf_input = input("Informe o Código UF (Ex: SP, RJ). Deixe vazio para buscar TODAS as UFs: ").strip()
    
    categoria_input = input("Informe o GRUPO de produtos (Ex: CALÇADOS, ACESSÓRIOS). Deixe vazio para buscar TODOS os Grupos: ").strip()
    
    print(f"\n---> Aplicando exclusão automática dos Subgrupos: {', '.join(SUBGRUPOS_EXCLUIDOS)}")

    FILTROS_BUSCA = {
        'COD_UF': uf_input,
        'CATEGORIA': categoria_input,
    }
    
    consolidar_relatorio(FILTROS_BUSCA)