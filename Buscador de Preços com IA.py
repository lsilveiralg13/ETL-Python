import requests
from bs4 import BeautifulSoup
import pandas as pd
import re 
import time

# --- 1. CONFIGURAÇÕES E HEADERS ---

# URL de busca do BUSCAPÉ
BUSCAPE_URL = "https://www.buscape.com.br/search?q="
NOME_LOJA = "Buscapé (Agregador)"

# Headers para simular um navegador real e evitar bloqueios
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Connection': 'keep-alive'
}

# Estrutura de Categorias de Busca
CATEGORIAS_GAMING = {
    "Placa de Vídeo": ["RTX 4070", "RX 7800 XT", "RTX 4060 Ti"],
    "Processador": ["Ryzen 7 7700X", "Core i5 14400F"],
    "Console": ["Playstation 5", "Xbox Series X"]
}

# --- 2. CLASSE DE WEB SCRAPING NO BUSCAPÉ (Ponto Crítico de Ajuste) ---

class BuscapeScraper:
    
    # ----------------------------------------------------------------------
    # !!! ATENÇÃO !!!
    # ESTES SÃO SELETORES CSS BASEADOS NA ESTRUTURA ATUAL DO BUSCAPÉ.
    # SE O CÓDIGO NÃO FUNCIONAR, VOCÊ DEVE INSPECIONAR O BUSCAPÉ E ATUALIZAR ESTES 3 SELETORES.
    # ----------------------------------------------------------------------
    
    # Seletores do Buscapé (Versão Tentaiva)
    SELETOR_CARD = 'div[data-testid="product-card"]'       # O bloco que contém todo o produto
    SELETOR_NOME = 'h2[data-testid="product-card-title"]'  # O nome/descrição do produto
    SELETOR_PRECO = 'p[data-testid="price"]'               # O valor final do preço
    
    def limpar_preco(self, preco_str):
        if not preco_str:
            return None
        # Remove R$, ponto de milhar e converte vírgula decimal para ponto
        preco_limpo = re.sub(r'[^\d,\.]', '', preco_str)
        # O Buscapé usa o formato BRL (1.000,00 -> 1000.00)
        preco_limpo = preco_limpo.replace('.', '').replace(',', '.') 
        try:
            return float(preco_limpo)
        except ValueError:
            return None

    def buscar(self, termo_busca):
        termo_formatado = termo_busca.replace(' ', '%20') # Formato de URL para busca
        url_completa = BUSCAPE_URL + termo_formatado
        resultados = []

        print(f"  -> Buscando em {NOME_LOJA} por: '{termo_busca}'")

        try:
            response = requests.get(url_completa, headers=HEADERS, timeout=15)
            response.raise_for_status() 
        except requests.exceptions.RequestException as e:
            print(f"  -> ERRO ao acessar {NOME_LOJA}: {e}")
            return resultados

        soup = BeautifulSoup(response.text, 'html.parser')
        
        cards = soup.select(self.SELETOR_CARD)

        for card in cards:
            try:
                # 1. Extrai Nome (o link geralmente está no H2/nome)
                nome_elemento = card.select_one(self.SELETOR_NOME)
                nome = nome_elemento.text.strip() if nome_elemento else 'N/A'
                
                # 2. Extrai Preço
                preco_elemento = card.select_one(self.SELETOR_PRECO)
                preco_str = preco_elemento.text.strip() if preco_elemento else 'R$ 0,00'
                preco = self.limpar_preco(preco_str)

                # 3. Extrai Link (A tag 'a' geralmente é a primeira dentro do card)
                # Como o Buscapé usa links relativos, precisamos adicionar o domínio
                link_tag = card.select_one('a') 
                link_relativo = link_tag.get('href') if link_tag and link_tag.get('href') else url_completa
                link = "https://www.buscape.com.br" + link_relativo if link_relativo.startswith('/') else link_relativo
                
                
                if preco and nome != 'N/A' and preco > 0:
                    resultados.append({
                        'Produto': nome,
                        'Preço (R$)': preco,
                        'Loja': NOME_LOJA,
                        'Link': link
                    })
            except Exception:
                # Ignora cards malformados
                continue
        
        return resultados

# --- 3. FUNÇÃO PRINCIPAL DE EXECUÇÃO E ANÁLISE ---

def buscador_precos_comparativo(categoria_ou_termo):
    
    if categoria_ou_termo in CATEGORIAS_GAMING:
        termos_busca = CATEGORIAS_GAMING[categoria_ou_termo]
    else:
        termos_busca = [categoria_ou_termo]

    todos_os_resultados = []
    scraper = BuscapeScraper() # Instancia o scraper

    for termo in termos_busca:
        print(f"\n======================================")
        print(f"INICIANDO BUSCA GERAL POR: {termo}")
        print(f"======================================")
        
        # Chama a função de busca
        resultados_buscape = scraper.buscar(termo)
        
        for res in resultados_buscape:
            res['Categoria'] = categoria_ou_termo
        
        todos_os_resultados.extend(resultados_buscape)
        time.sleep(1) # Pausa entre as buscas

    if not todos_os_resultados:
        print("\nNenhum resultado consolidado encontrado. Verifique os seletores CSS do Buscapé.")
        return
        
    # Análise de Dados (Pandas)
    df = pd.DataFrame(todos_os_resultados)
    df_ordenado = df.sort_values(by='Preço (R$)', ascending=True).reset_index(drop=True)
    
    # Exibe e Exporta
    print("\n\n--- RELATÓRIO DE PREÇOS NO BUSCAPÉ (TOP 5 OFERTAS) ---\n")
    df_exibicao = df_ordenado.copy()
    df_exibicao['Preço (R$)'] = df_exibicao['Preço (R$)'].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    
    print(df_exibicao[['Produto', 'Preço (R$)', 'Loja', 'Link']].head(5).to_string(index=True))
    
    nome_arquivo = f'buscape_precos_{categoria_ou_termo.replace(" ", "_")}.xlsx'
    df_ordenado[['Produto', 'Preço (R$)', 'Loja', 'Link', 'Categoria']].to_excel(nome_arquivo, index=False)
    print(f"\n[SUCESSO] Dados completos exportados para '{nome_arquivo}'")

# --- EXECUÇÃO DO SCRIPT ---

if __name__ == "__main__":
    # EXECUÇÃO: BUSCAR CONSOLES
    buscador_precos_comparativo("Console")