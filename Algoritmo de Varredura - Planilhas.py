import pandas as pd
import numpy as np
import unicodedata

def limpar_valor_seguro(valor):
    """
    Normaliza UM ÚNICO valor. 
    Garante que nunca tente aplicar .upper() em um objeto Series.
    """
    try:
        if pd.isna(valor) or valor is None:
            return ""
        # Força a conversão para string de forma atômica (elemento por elemento)
        string_pura = str(valor).strip().upper()
        
        # Remove acentos
        nfkd = unicodedata.normalize('NFKD', string_pura)
        return "".join([c for c in nfkd if not unicodedata.combining(c)])
    except:
        return ""

# Dicionário (Mantido conforme o seu padrão)
dicionario_bruto = {
    "DESCRICAO": ["descricao", "descrição", "detalhe", "detalhamento", "especificacao", "especificação", "informacao", "informação", "resumo", "apresentacao", "apresentação", "caracteristica", "característica", "definicao", "definição", "conteudo", "conteúdo", "texto", "observacao", "observação", "nota", "anotacao", "anotação", "relato", "explicacao", "explicação", "documentacao", "documentação", "ficha tecnica", "ficha técnica", "descricao tecnica", "descricao do produto"],
    "SKU": ["sku", "codigo", "código", "codigo produto", "codigo do produto", "id produto", "identificador", "identificacao", "identificação", "referencia", "referência", "cod item", "codigo item", "codigo interno", "codigo de barras", "ean", "gtin", "numero do produto", "numero item", "id", "chave", "chave primaria", "chave primária", "registro", "indexador"],
    "QTDE": ["qtde", "quantidade", "qtd", "volume", "numero", "número", "total", "contagem", "quantia", "montante", "unidades", "qtd total", "quantidade total", "numero de itens", "itens", "qtd itens", "qtd produtos", "quantidade de produtos", "saldo", "estoque", "disponivel", "disponível", "nivel", "nível", "carga", "lote"],
    "DEPOSITO": ["deposito", "depósito", "armazem", "armazém", "estoque", "local", "localidade", "unidade", "filial", "centro de distribuicao", "centro de distribuição", "cd", "warehouse", "almoxarifado", "galpao", "galpão", "ponto de estoque", "ponto", "base", "instalacao", "instalação", "estrutura", "local de armazenamento", "area de estoque", "area", "setor", "endereco", "endereço"]
}

MAPA_NORMALIZADO = {k: [limpar_valor_seguro(v) for v in lista] for k, lista in dicionario_bruto.items()}

def extrair_dados_aba(xls, nome_aba):
    # Carrega as primeiras 50 linhas para busca
    df_busca = pd.read_excel(xls, sheet_name=nome_aba, header=None, nrows=50)
    
    linha_mestre = None
    mapeamento = {}

    # Percorre cada linha
    for idx_linha, row in df_busca.iterrows():
        temp_map = {}
        achados = set()
        
        # Percorre cada célula da linha de forma isolada
        for idx_col, celula in enumerate(row):
            valor_limpo = limpar_valor_seguro(celula)
            
            for categoria, sinonimos in MAPA_NORMALIZADO.items():
                if valor_limpo in sinonimos and categoria not in achados:
                    temp_map[idx_col] = categoria
                    achados.add(categoria)
        
        # Critério de sucesso: achou SKU e QTDE
        if "SKU" in achados and "QTDE" in achados:
            linha_mestre = idx_linha
            mapeamento = temp_map
            break
            
    if linha_mestre is None:
        return None

    # Re-leitura focada na linha correta
    df_real = pd.read_excel(xls, sheet_name=nome_aba, skiprows=linha_mestre)
    
    # Faz o de-para das colunas reais para as oficiais
    colunas_originais = df_real.columns
    renomear_dict = {}
    for pos, cat in mapeamento.items():
        if pos < len(colunas_originais):
            renomear_dict[colunas_originais[pos]] = cat

    # Seleção segura
    df_limpo = df_real[list(renomear_dict.keys())].copy()
    df_limpo = df_limpo.rename(columns=renomear_dict)

    # Garantia de colunas obrigatórias
    if 'DEPOSITO' not in df_limpo.columns: df_limpo['DEPOSITO'] = nome_aba
    if 'DESCRICAO' not in df_limpo.columns: df_limpo['DESCRICAO'] = "NÃO INFORMADO"
    
    return df_limpo[['SKU', 'DESCRICAO', 'DEPOSITO', 'QTDE']]

def executar_consolidacao():
    arquivo = 'INVENTARIO CD ESMERALDAS ETL.xlsx'
    print(f"--- Iniciando Consolidação Blindada (V6) ---")
    
    try:
        xls = pd.ExcelFile(arquivo)
        final_list = []

        for aba in xls.sheet_names:
            try:
                resultado_aba = extrair_dados_aba(xls, aba)
                if resultado_aba is not None:
                    resultado_aba['ORIGEM_ABA'] = aba
                    final_list.append(resultado_aba)
                    print(f"✅ Aba '{aba}': Extraída.")
                else:
                    print(f"⚠️ Aba '{aba}': SKU/QTDE não localizados.")
            except Exception as e_aba:
                print(f"❌ Falha técnica na aba '{aba}': {e_aba}")

        if final_list:
            df_full = pd.concat(final_list, ignore_index=True)
            
            # Limpeza final de valores de SKU (Lidando com .0 do Excel)
            df_full['SKU'] = df_full['SKU'].apply(lambda x: str(x).replace('.0', '').strip().upper() if pd.notna(x) else "")
            df_full['QTDE'] = pd.to_numeric(df_full['QTDE'], errors='coerce').fillna(0)
            
            df_full = df_full[df_full['SKU'] != ""]
            df_full = df_full.rename(columns={'DESCRICAO': 'DESCRIÇÃO', 'DEPOSITO': 'DEPÓSITO'})
            
            # Ordenação e ID
            df_full = df_full.sort_values(by=['DEPÓSITO', 'SKU']).reset_index(drop=True)
            df_full.insert(0, 'ID', df_full.index + 1)
            
            df_full.to_excel('INVENTARIO_CONSOLIDADO_ORION_V6.xlsx', index=False)
            print(f"\n🚀 SUCESSO! {len(df_full)} registros processados.")
        else:
            print("\nNenhum dado capturado.")

    except Exception as e_geral:
        print(f"\nERRO CRÍTICO NO ARQUIVO: {e_geral}")

if __name__ == "__main__":
    executar_consolidacao()