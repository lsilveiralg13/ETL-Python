import pandas as pd
import os
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill

def formatar_excel(caminho_arquivo):
    wb = load_workbook(caminho_arquivo)
    ws = wb.active
    ws.sheet_view.showGridLines = False

    fonte_corpo = Font(name='Calibri', size=9)
    fonte_cabecalho = Font(name='Calibri', size=9, bold=True, color="FFFFFF")
    alinhamento_central = Alignment(horizontal='center', vertical='center')
    preenchimento_preto = PatternFill(start_color='000000', end_color='000000', fill_type='solid')

    for row in ws.iter_rows():
        for cell in row:
            cell.font = fonte_corpo
            cell.alignment = alinhamento_central

    for cell in ws[1]:
        cell.font = fonte_cabecalho
        cell.fill = preenchimento_preto
        cell.alignment = alinhamento_central
    
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except: pass
        ws.column_dimensions[column].width = max_length + 2

    wb.save(caminho_arquivo)

def ler_csv_com_varredura(caminho):
    """Tenta ler o CSV testando os encodings sugeridos pelo Lucas."""
    encodings = [
        'utf-8-sig',    # UTF-8 com BOM
        'utf-8',        # UTF-8 padrão
        'cp1252',       # ANSI / Windows-1252
        'iso-8859-1',   # Latin-1
        'utf-16',       # UTF-16 (Tenta detectar LE/BE automaticamente)
        'ascii'
    ]
    
    for encoding in encodings:
        try:
            # engine='python' com sep=None ajuda a detectar se é , ou ;
            df = pd.read_csv(caminho, encoding=encoding, sep=None, engine='python')
            print(f"✔️ Sucesso ao ler com encoding: {encoding}")
            return df
        except (UnicodeDecodeError, Exception):
            continue
    
    raise Exception("Não foi possível converter o arquivo com nenhum dos encodings da lista.")

def converter_arquivos():
    diretorio = r'C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python'
    
    if not os.path.exists(diretorio):
        print(f"Erro: O caminho não foi encontrado.")
        return

    arquivos_disponiveis = [f for f in os.listdir(diretorio) if os.path.isfile(os.path.join(diretorio, f))]
    
    print(f"\n--- Arquivos em: {diretorio} ---")
    for i, arq in enumerate(arquivos_disponiveis):
        print(f"[{i}] {arq}")

    try:
        idx = int(input("\nDigite o número do arquivo de ORIGEM: "))
        nome_arquivo_entrada = arquivos_disponiveis[idx]
    except:
        print("Seleção inválida.")
        return

    ext_saida = input("Qual a extensão de SAÍDA desejada (.xlsx, .parquet, .csv): ").strip().lower()
    if not ext_saida.startswith('.'): ext_saida = '.' + ext_saida

    caminho_entrada = os.path.join(diretorio, nome_arquivo_entrada)
    nome_base = os.path.splitext(nome_arquivo_entrada)[0]
    caminho_saida = os.path.join(diretorio, nome_base + ext_saida)

    print(f"\nProcessando: {nome_arquivo_entrada}...")
    
    try:
        if nome_arquivo_entrada.lower().endswith('.csv'):
            df = ler_csv_com_varredura(caminho_entrada)
        elif nome_arquivo_entrada.lower().endswith('.parquet'):
            df = pd.read_parquet(caminho_entrada)
        elif nome_arquivo_entrada.lower().endswith(('.xlsx', '.xlsm')):
            df = pd.read_excel(caminho_entrada)
        else:
            print("Extensão não suportada.")
            return

        # Exportação garantindo saída em UTF-8 com BOM para o Excel não quebrar depois
        if ext_saida in ['.xlsx', '.xlsm']:
            df.to_excel(caminho_saida, index=False)
            formatar_excel(caminho_saida)
        elif ext_saida == '.parquet':
            df.to_parquet(caminho_saida)
        elif ext_saida == '.csv':
            df.to_csv(caminho_saida, index=False, encoding='utf-8-sig')
        
        print(f"\n✅ SUCESSO! Salvo como: {os.path.basename(caminho_saida)}")

    except Exception as e:
        print(f"\n❌ Erro crítico: {e}")

if __name__ == "__main__":
    converter_arquivos()