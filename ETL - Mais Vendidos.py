import mysql.connector
import os
import pandas as pd
import xlsxwriter # Importando a biblioteca xlsxwriter

# --- Configurações do Banco de Dados ---
# ATENÇÃO: Substitua com suas credenciais e detalhes do banco de dados
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'faturamento_multimarcas_dw',
    'port': 3306
}

# --- Configuração da Pasta de Imagens ---
# ATENÇÃO: Substitua pelo caminho REAL da sua pasta de imagens
# Exemplo: 'C:/Users/SeuUsuario/ImagensProdutos' ou '/home/seu_usuario/imagens_produtos'
IMAGENS_FOLDER = r'C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\FOTOS PRODUTOS'
IMAGEM_EXTENSAO = '.png' # Confirme a extensão das suas imagens (ex: .png, .jpeg)

def get_top_products_from_sp(mes: str, ano: int):
    """
    Conecta ao MySQL, chama a Stored Procedure e retorna os 10 produtos mais vendidos.
    """
    products_data = []
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True) # dictionary=True retorna resultados como dicionários

        # Chama a Stored Procedure
        sp_call = f"CALL `Ranking dos Mais Vendidos - SAPATOS`('{mes}', {ano})"
        print(f"Executando SP: {sp_call}")
        cursor.execute(sp_call)

        # Recupera os resultados
        for row in cursor:
            products_data.append(row)

    except mysql.connector.Error as err:
        print(f"Erro ao conectar ou executar a SP: {err}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return products_data

def get_image_path(codigo_produto: str) -> str:
    """
    Constrói o caminho completo para a imagem de um produto.
    Assume que as imagens estão nomeadas como 'codigo_produto.extensao'.
    """
    # Convertendo o código do produto e a extensão para minúsculas para padronização
    image_filename_lower = f"{str(codigo_produto).lower()}{IMAGEM_EXTENSAO.lower()}"
    full_image_path_lower = os.path.join(IMAGENS_FOLDER, image_filename_lower)
    
    # --- NOVAS MENSAGENS DE DEPURAÇÃO ---
    print(f"DEBUG: IMAGENS_FOLDER configurado como: '{IMAGENS_FOLDER}'")
    print(f"DEBUG: Tentando construir nome do arquivo (lower): '{image_filename_lower}'")
    print(f"DEBUG: Caminho completo que será verificado: '{full_image_path_lower}'")

    if os.path.exists(full_image_path_lower):
        print(f"DEBUG: Imagem ENCONTRADA: '{full_image_path_lower}'")
        return full_image_path_lower
    else:
        print(f"DEBUG: Imagem NÃO ENCONTRADA: '{full_image_path_lower}'")
        
        # Tenta listar arquivos na pasta para verificar se o nome existe com outra capitalização
        try:
            files_in_folder = os.listdir(IMAGENS_FOLDER)
            found_alternative = False
            for f_name in files_in_folder:
                if f_name.lower() == image_filename_lower:
                    print(f"DEBUG: Encontrado arquivo com capitalização diferente: '{f_name}'")
                    # Se encontrar, retorna o caminho com a capitalização real do arquivo
                    return os.path.join(IMAGENS_FOLDER, f_name)
            if not found_alternative:
                print(f"DEBUG: Nenhum arquivo correspondente encontrado na pasta '{IMAGENS_FOLDER}'.")
        except FileNotFoundError:
            print(f"DEBUG: Erro: A pasta de imagens '{IMAGENS_FOLDER}' não foi encontrada pelo Python.")
        except Exception as e:
            print(f"DEBUG: Erro ao listar arquivos na pasta de imagens: {e}")

        return "" # Retorna string vazia se a imagem não for encontrada

def generate_product_catalog(mes: str, ano: int, output_excel_filename: str = 'catalogo_produtos.xlsx'):
    """
    Gera o catálogo dos produtos mais vendidos e o exporta para um arquivo Excel,
    incorporando as imagens dos produtos com redimensionamento automático.
    """
    print(f"\n--- Catálogo dos 10 Mais Vendidos (SAPATOS) - {mes.capitalize()}/{ano} ---")
    
    top_products = get_top_products_from_sp(mes, ano)

    if not top_products:
        print("Nenhum produto encontrado para o período e critérios especificados.")
        return

    # Adiciona o caminho local da imagem aos dados
    for product in top_products:
        product['Caminho da Imagem'] = get_image_path(product['codigo_produto'])
        if 'FOTO' in product:
            del product['FOTO']

    # Cria um DataFrame Pandas para facilitar a manipulação dos dados
    df = pd.DataFrame(top_products)

    # Define a ordem das colunas para o Excel (excluindo 'Caminho da Imagem' que será usada para inserir a imagem)
    desired_columns_order = [
        'ranking',
        'referencia_produto',
        'descricao_produto',
        'codigo_produto',
        'volume_vendido_total'
    ]
    df = df[[col for col in desired_columns_order if col in df.columns]]

    # --- Exporta para Excel usando XlsxWriter para incorporar imagens ---
    try:
        # Cria um novo workbook e worksheet
        workbook = xlsxwriter.Workbook(output_excel_filename)
        worksheet = workbook.add_worksheet()

        # Define o formato padrão para o texto (Calibri, tamanho 9)
        default_text_format = workbook.add_format({'font_name': 'Calibri', 'font_size': 9, 'align': 'center', 'valign': 'vcenter', 'border': 1})

        # Define o formato do cabeçalho (negrito, centralizado, Calibri, tamanho 9)
        header_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'font_name': 'Calibri', 'font_size': 9})
        
        # Escreve os cabeçalhos das colunas
        headers = df.columns.tolist()
        headers.append('Imagem do Produto') # Adiciona o cabeçalho para a coluna de imagem

        for col_num, header in enumerate(headers):
            worksheet.write(0, col_num, header, header_format)
        
        # Define a largura exata das colunas (22 unidades de caracteres)
        for col_idx in range(len(headers)): # Aplica a largura a TODAS as colunas, incluindo a de imagem
            worksheet.set_column(col_idx, col_idx, 22)

        # Itera sobre os dados e escreve no Excel
        for row_num, row_data in df.iterrows():
            # Escreve os dados das colunas existentes com o formato padrão
            for col_num, value in enumerate(row_data):
                worksheet.write(row_num + 1, col_num, value, default_text_format) # +1 para pular a linha do cabeçalho

            # Define a altura exata da linha (46.0 pontos)
            worksheet.set_row(row_num + 1, 46.0) # Altura da linha definida para 46.0 pontos

            # Insere a imagem na coluna 'Imagem do Produto'
            image_path = top_products[row_num]['Caminho da Imagem'] # Pega o caminho da imagem da lista original

            if image_path and os.path.exists(image_path): # Verifica novamente se o caminho é válido
                try:
                    # Insere a imagem com a propriedade 'object_position': 2 (Move and size with cells)
                    # A imagem será redimensionada automaticamente para caber na célula
                    # devido ao 'object_position': 2 e ao tamanho da célula (22 de largura, 46 de altura).
                    worksheet.insert_image(
                        row_num + 1, # Linha (1-indexed)
                        len(headers) - 1, # Coluna (0-indexed) da 'Imagem do Produto'
                        image_path,
                        {'x_offset': 0, 'y_offset': 0, # Sem offset para ficar colado à célula
                         'object_position': 2 # ESSA É A PROPRIEDADE CHAVE: Move and size with cells
                        }
                    )
                except Exception as img_err:
                    print(f"DEBUG: Erro ao inserir imagem para {row_data['codigo_produto']}: {img_err}")
                    worksheet.write(row_num + 1, len(headers) - 1, "Erro na Imagem", default_text_format)
            else:
                worksheet.write(row_num + 1, len(headers) - 1, "Imagem não encontrada", default_text_format)


        # Fecha o workbook
        workbook.close()

        print(f"\nCatálogo exportado com sucesso para '{output_excel_filename}', com imagens incorporadas e redimensionamento automático.")
    except Exception as e:
        print(f"Erro ao exportar para Excel: {e}")

# --- Execução Principal ---
if __name__ == "__main__":
    mes_consulta = 'julho'
    ano_consulta = 2025
    
    # Define o nome do arquivo Excel de saída
    output_file = 'CatalogoTop10MaisVendidos.xlsx'

    generate_product_catalog(mes_consulta, ano_consulta, output_file)


