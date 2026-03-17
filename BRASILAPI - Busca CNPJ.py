import requests
import json
import time

def buscar_empresa_por_nome(termo):
    """
    Tenta localizar o CNPJ pelo nome usando um serviço de busca 
    e retorna os detalhes completos da empresa.
    """
    print(f"\n🔍 Iniciando varredura para: '{termo}'...")
    
    # Passo 1: Buscar o CNPJ pelo nome (Usando API de busca pública)
    # A 'CNPJ.ws' ou 'Minha Receita' são ótimas alternativas
    search_url = f"https://minhareceita.org/{termo}" # Exemplo de buscador simplificado
    
    # Como buscadores de nome variam, usaremos a BrasilAPI como base de dados
    # Mas primeiro precisamos do CNPJ. Vou usar um endpoint de busca fonética/termo
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        # Tentativa de busca via API de busca de CNPJs (substituindo o Google instável)
        # Aqui usamos o endpoint de busca da ReceitaWS ou similar
        search_res = requests.get(f"https://www.receitaws.com.br/v1/cnpj/00000000000191") # Apenas exemplo de estrutura
        
        # Estratégia Recomendada: Usar a API da CNPJ.ws (Gratuita para buscas básicas)
        url_busca = f"https://publica.cnpj.ws/cnpj/busca?razao_social={termo}"
        
        # Para evitar erros de API, vamos focar na BrasilAPI que é a mais completa em dados societários
        # Se você tiver o CNPJ, ela te dá TUDO.
        
        # --- BUSCA SIMULADA (Substitua pelo seu CNPJ de teste se o buscador falhar) ---
        # No terminal, você vai inserir o nome e o script tentará converter.
        
        # Para fins de robustez, vamos usar o buscador do 'CNPJ.rocks' que é bem aberto
        # ou solicitar que o usuário confirme o CNPJ caso a busca automatizada falhe.
        
        # Aqui está o motor principal:
        cnpj_limpo = localiza_cnpj_pelo_termo(termo)
        
        if cnpj_limpo:
            return consultar_brasil_api(cnpj_limpo)
        else:
            return {"erro": "Não foi possível converter a Razão Social em um CNPJ válido automaticamente."}

    except Exception as e:
        return {"erro": f"Falha na conexão: {str(e)}"}

def localiza_cnpj_pelo_termo(termo):
    """ Tenta encontrar o CNPJ em um buscador alternativo ao Google """
    # Tentativa via autocomplete/search de serviços abertos
    try:
        # Usando o buscador do site 'CNPJ' que costuma ser mais permissivo que o Google
        url = f"https://brasilapi.com.br/api/cnpj/v1/{termo}" # Placeholder
        # Se o termo já for um CNPJ, ele busca direto. 
        # Se for nome, precisa de um buscador de 'razão social'
        # A melhor alternativa gratuita hoje para NOME -> CNPJ é a API da CNPJ.ws
        return "00000000000191" # Exemplo: Banco do Brasil (Substitua pela lógica de busca)
    except:
        return None

def consultar_brasil_api(cnpj):
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    res = requests.get(url)
    if res.status_code == 200:
        d = res.json()
        return {
            "CNPJ": d.get("cnpj"),
            "RAZÃO SOCIAL": d.get("razao_social"),
            "TEMPO ATIVIDADE": d.get("data_inicio_atividade"),
            "CAPITAL SOCIAL": f"R$ {d.get('capital_social', 0):,.2f}",
            "CIDADE": d.get("municipio"),
            "ESTADO": d.get("uf"),
            "SÓCIOS": [s.get("nome_socio") for s in d.get("qsa", [])]
        }
    return None

# --- LOOP DE TERMINAL ---
if __name__ == "__main__":
    while True:
        entrada = input("\n🏢 Digite a Razão Social (ou 'sair'): ").strip()
        if entrada.lower() == 'sair': break
        
        # Se você digitar o CNPJ direto, ele já detalha. 
        # Se for nome, ele precisa da lógica de conversão.
        resultado = consultar_brasil_api(entrada.replace(".", "").replace("/", "").replace("-", ""))
        
        if resultado:
            print("\n" + "—"*40)
            for chave, valor in resultado.items():
                print(f"**{chave}:** {valor}")
            print("—"*40)
        else:
            print("❌ Empresa não encontrada. Tente o CNPJ direto ou verifique o nome.")