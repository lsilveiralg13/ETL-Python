import pandas as pd
import requests
from datetime import datetime, timedelta
import locale

def extrair_dolar_classificacao_propria():
    try:
        locale.setlocale(locale.LC_TIME, "pt_BR.utf8")
    except:
        try:
            locale.setlocale(locale.LC_TIME, "Portuguese_Brazil.1252")
        except:
            pass

    print("--- GERADOR DE COTAÇÃO COM AJUSTE DE FUSO HORÁRIO ---")
    data_inicio_input = input("Digite a DATA INÍCIO (DD-MM-AAAA): ")
    data_fim_input = input("Digite a DATA FIM (DD-MM-AAAA): ")

    try:
        d_ini = datetime.strptime(data_inicio_input, "%d-%m-%Y").strftime("%m-%d-%Y")
        d_fim = datetime.strptime(data_fim_input, "%d-%m-%Y").strftime("%m-%d-%Y")
    except ValueError:
        print("Erro: Use o formato DD-MM-AAAA.")
        return

    url = (
        f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
        f"CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"
        f"@dataInicial='{d_ini}'&@dataFinalCotacao='{d_fim}'&$format=json"
    )
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        dados = response.json().get('value', [])
        
        if not dados:
            print("Aviso: Sem dados para este período.")
            return

        df = pd.DataFrame(dados)
        
        # 1. Converter para datetime e AJUSTAR FUSO (Subtrair 3 horas)
        df['dataHoraCotacao'] = pd.to_datetime(df['dataHoraCotacao'])
        df['dataHoraBrasilia'] = df['dataHoraCotacao'] - timedelta(hours=3)

        # 2. NOVA CLASSIFICAÇÃO baseada no horário de Brasília
        def classificar_boletim(row):
            hora = row['dataHoraBrasilia'].hour
            
            if 10 <= hora < 11:
                return "Abertura"
            elif 11 <= hora < 12:
                return "Intermediaria"
            elif hora >= 12:
                return "Fechamento"
            else:
                return "Pre-Abertura"

        df['tipoBoletim'] = df.apply(classificar_boletim, axis=1)

        # 3. Tratamento de Data para o Excel (Usando a data ajustada)
        df['DataCotacao'] = df['dataHoraBrasilia'].dt.strftime('%A, %d de %B de %Y')

        # 4. Organização final (Mantive a coluna original e a de Brasília para você conferir)
        ordem_final = ['cotacaoCompra', 'cotacaoVenda', 'dataHoraBrasilia', 'tipoBoletim', 'DataCotacao']
        df = df[ordem_final]
        
        nome_arquivo = "Cotacao do dolar.xlsx"
        df.to_excel(nome_arquivo, index=False)
        
        print("-" * 50)
        print(f"Sucesso! Arquivo '{nome_arquivo}' gerado com ajuste de fuso.")
        print("-" * 50)
        print(df.tail(10))

    except Exception as e:
        print(f"Erro: {e}")

extrair_dolar_classificacao_propria()