from flask import Flask, render_template, request
import pandas as pd
import os

app = Flask(__name__)

# Configuração de caminhos (Baseado no seu ambiente de Analista)
DIRETORIO_BASE = os.path.abspath(r'C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\Scripts Python')
ARQUIVO_FATURAMENTO = os.path.join(DIRETORIO_BASE, 'FATURAMENTO ETL.xlsx')

def carregar_filtros():
    try:
        df = pd.read_excel(ARQUIVO_FATURAMENTO)
        # Normaliza nomes de colunas: string, sem espaços e minúsculo
        df.columns = df.columns.astype(str).str.strip().str.lower()
        
        # Puxa anos únicos e garante que sejam strings para a picklist
        anos = sorted(df['chave_ano'].astype(str).str.strip().unique().tolist(), reverse=True)
        meses = ["JANEIRO", "FEVEREIRO", "MARCO", "ABRIL", "MAIO", "JUNHO", 
                 "JULHO", "AGOSTO", "SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO"]
        return anos, meses
    except Exception as e:
        print(f"Erro ao carregar filtros: {e}")
        return ["2026", "2025"], ["JANEIRO"]

def carregar_dados_historicos(mes, ano):
    try:
        df = pd.read_excel(ARQUIVO_FATURAMENTO)
        df.columns = df.columns.astype(str).str.strip().str.lower()
        
        # NORMALIZAÇÃO: Forçamos a coluna da base para string, removemos espaços e pomos em UPPER
        df['chave_mes'] = df['chave_mes'].astype(str).str.strip().str.upper()
        df['chave_ano'] = df['chave_ano'].astype(str).str.strip().str.upper()

        # TRADUÇÃO: Garantimos que o que veio do Flask também esteja limpo e em UPPER
        mes_busca = str(mes).strip().upper()
        ano_busca = str(ano).strip().upper()

        # Filtro comparando STRING LIMPA com STRING LIMPA
        df_filtrado = df[(df['chave_mes'] == mes_busca) & (df['chave_ano'] == ano_busca)]
        
        if df_filtrado.empty:
            return None

        # Cálculos tratando possíveis valores vazios (NaN)
        faturamento = df_filtrado['vlr. nota'].fillna(0).sum()
        meta = df_filtrado['meta'].fillna(0).sum()
        atingimento = (faturamento / meta) * 100 if meta > 0 else 0
        
        # Lista de vendedores associados ao filtro
        vendedores = sorted(df_filtrado['vendedor'].unique().tolist())
        
        return {
            'Faturamento_Mes': f"{faturamento:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            'Meta': f"{meta:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            'Atingimento': f"{atingimento:.1f}%",
            'Mes_Nome': mes_busca.capitalize(),
            'Ano_Nome': ano_busca,
            'Vendedores': vendedores
        }
    except Exception as e:
        print(f"Erro no processamento de dados: {e}")
        return None

@app.route('/', methods=['GET', 'POST'])
def principal():
    anos_disponiveis, meses_disponiveis = carregar_filtros()
    
    # Captura valores ou define padrão (Garante que ano_sel seja String)
    ano_sel = request.form.get('ano_input', str(anos_disponiveis[0]))
    mes_sel = request.form.get('mes_input', 'JANEIRO')
    
    dados_venda = carregar_dados_historicos(mes_sel, ano_sel)
    
    if not dados_venda:
        dados_venda = {
            'Faturamento_Mes': '0,00', 'Meta': '0,00', 'Atingimento': '0%', 
            'Mes_Nome': mes_sel.capitalize(), 'Ano_Nome': ano_sel, 'Vendedores': []
        }

    return render_template('index.html', dados=dados_venda, anos=anos_disponiveis, meses=meses_disponiveis)

if __name__ == '__main__':
    # IP da sua rede para acesso remoto (ROG Ally/Mobile)
    app.run(debug=True, host='192.168.0.105', port=5000)