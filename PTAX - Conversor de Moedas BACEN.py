# ===============================================================
# 💰 ENRIQUECIMENTO CAMBIAL - FATURAMENTO ETL (USD, EUR, GBP)
# ===============================================================
# Autor: Lucas (com apoio da Bia 🧠)
# Versão: 2.2 - Resiliente (requisições fracionadas e tolerância a erro)
# ===============================================================

import pandas as pd
import requests
from datetime import datetime
from colorama import Fore, Style, init
import time
import warnings
warnings.filterwarnings("ignore")
init(autoreset=True)

# ===============================================================
# 🎯 ENTRADA DE DADOS
# ===============================================================
arquivo = "FATURAMENTO ETL.xlsx"
aba = "FAT - TOTAL"

print(Fore.CYAN + "📂 Lendo base de faturamento...")
df = pd.read_excel(arquivo, sheet_name=aba)

# ===============================================================
# 🧹 LIMPEZA E PREPARAÇÃO
# ===============================================================
print(Fore.CYAN + "🧽 Limpando e preparando dados...")

df = df.copy()
df['Dt. Neg.'] = pd.to_datetime(df['Dt. Neg.'], errors='coerce')
df = df.dropna(subset=['Dt. Neg.', 'Vlr. Nota'])

data_inicial = df['Dt. Neg.'].min().strftime("%Y-%m-%d")
data_final = df['Dt. Neg.'].max().strftime("%Y-%m-%d")

print(Fore.YELLOW + f"📅 Período detectado: {data_inicial} a {data_final}")

# ===============================================================
# 🌎 FUNÇÃO ROBUSTA DE CONSULTA À PTAX
# ===============================================================
def consulta_ptax(moeda: str, inicio: str, fim: str) -> pd.DataFrame:
    """
    Consulta cotações PTAX fracionando por ano (evita erro 500).
    """
    data_inicio = pd.to_datetime(inicio)
    data_fim = pd.to_datetime(fim)
    anos = range(data_inicio.year, data_fim.year + 1)
    frames = []

    for ano in anos:
        ini = f"{ano}-01-01"
        fim_ano = f"{ano}-12-31"
        print(Fore.LIGHTBLUE_EX + f"🔹 Baixando {moeda} para {ano}...")

        url = (
            f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
            f"CotacaoMoedaPeriodoFechamento(codigoMoeda=@moeda,dataInicialCotacao=@ini,dataFinalCotacao=@fim)?"
            f"@moeda='{moeda}'&@ini='{ini}'&@fim='{fim_ano}'&$format=json"
        )

        for tentativa in range(3):
            try:
                r = requests.get(url, timeout=15)
                if r.status_code == 500:
                    raise Exception("Erro 500 - servidor indisponível")
                r.raise_for_status()
                dados = r.json().get("value", [])
                if dados:
                    df_temp = pd.DataFrame(dados)
                    df_temp["dataHoraCotacao"] = pd.to_datetime(df_temp["dataHoraCotacao"])
                    df_temp["cotacaoMedia"] = (
                        df_temp["cotacaoCompra"].astype(float) + df_temp["cotacaoVenda"].astype(float)
                    ) / 2
                    df_temp = (
                        df_temp.groupby(df_temp["dataHoraCotacao"].dt.date)["cotacaoMedia"]
                        .mean()
                        .reset_index()
                        .rename(columns={"dataHoraCotacao": "Data"})
                    )
                    frames.append(df_temp)
                    break
            except Exception as e:
                print(Fore.RED + f"⚠️ Tentativa {tentativa+1}/3 falhou ({e}), tentando novamente...")
                time.sleep(3)

    if not frames:
        print(Fore.RED + f"⚠️ Nenhum dado disponível para {moeda}.")
        return pd.DataFrame()

    df_moeda = pd.concat(frames)
    df_moeda = df_moeda.drop_duplicates("Data").sort_values("Data")
    return df_moeda

# ===============================================================
# 🌍 CONSULTANDO MOEDAS
# ===============================================================
moedas = ["USD", "EUR", "GBP"]
df_cotacoes = {}

for moeda in moedas:
    print(Fore.CYAN + f"💱 Consultando cotação {moeda}...")
    df_moeda = consulta_ptax(moeda, data_inicial, data_final)
    if not df_moeda.empty:
        df_cotacoes[moeda] = df_moeda
        print(Fore.GREEN + f"✅ {moeda} carregado com {len(df_moeda)} registros.")
    else:
        print(Fore.RED + f"❌ Nenhum dado encontrado para {moeda}.")

# ===============================================================
# 🔗 MERGE COM BASE DE FATURAMENTO
# ===============================================================
print(Fore.CYAN + "🔗 Unindo cotações ao faturamento...")

df['Data'] = df['Dt. Neg.'].dt.date

for moeda, df_moeda in df_cotacoes.items():
    df = df.merge(df_moeda, on="Data", how="left")
    df.rename(columns={"cotacaoMedia": f"Cotação {moeda}"}, inplace=True)

# ===============================================================
# 💰 CÁLCULO DAS CONVERSÕES
# ===============================================================
print(Fore.CYAN + "💵 Calculando valores convertidos...")

for moeda in moedas:
    if f"Cotação {moeda}" in df.columns:
        df[f"Vlr. Nota ({moeda})"] = df['Vlr. Nota'] / df[f"Cotação {moeda}"]
    else:
        df[f"Vlr. Nota ({moeda})"] = None

# ===============================================================
# 🧾 EXPORTAÇÃO FINAL
# ===============================================================
print(Fore.CYAN + "💾 Gerando arquivo enriquecido...")

colunas_export = [
    'Dt. Neg.', 'Vlr. Nota',
    'Cotação USD', 'Cotação EUR', 'Cotação GBP',
    'Vlr. Nota (USD)', 'Vlr. Nota (EUR)', 'Vlr. Nota (GBP)'
]

colunas_existentes = [c for c in colunas_export if c in df.columns]
df_final = df[colunas_existentes]

saida = "FATURAMENTO_ETL_ENRIQUECIDO.xlsx"
df_final.to_excel(saida, index=False)

print(Fore.GREEN + "✅ Processo concluído com sucesso!")
print(Fore.YELLOW + f"📊 Arquivo gerado: {saida}")
print(Style.RESET_ALL)
