# ===============================================================
# 🧠 PROJEÇÃO DE FATURAMENTO - V28.5 (COMPLETA + FORMATOS EXTRAS)
# ===============================================================
# Inclui colunas:
# - DATA_ANO (MMM/AAAA)
# - MÊS (MMMM)
# Substitui "Variação % Mês a Mês" por "Crescimento Médio Mês a Mês"
# ===============================================================

import pandas as pd
import numpy as np
from prophet import Prophet
from datetime import datetime
from colorama import Fore, Style, init
import warnings
warnings.filterwarnings("ignore")

init(autoreset=True)

# ===============================================================
# 🎯 ENTRADA DE DADOS
# ===============================================================
arquivo_excel = 'MODELO DE PREVISÃO.xlsx'
aba = 'FAT - TOTAL'
coluna_data = 'Dt. Neg.'
coluna_valor = 'Vlr. Nota'

df_base = pd.read_excel(arquivo_excel, sheet_name=aba)

# ===============================================================
# 🧹 PRÉ-PROCESSAMENTO
# ===============================================================
df_base = df_base[[coluna_data, coluna_valor]].copy()
df_base.columns = ['ds', 'y']
df_base = df_base.dropna(subset=['ds', 'y'])
df_base = df_base[df_base['y'] > 0]
df_base['ds'] = pd.to_datetime(df_base['ds'], errors='coerce')

# ===============================================================
# ⚙️ CORREÇÃO AUTOMÁTICA DE ESCALA
# ===============================================================
mediana_valores = df_base.loc[df_base['y'] > 0, 'y'].median()

if mediana_valores > 10_000_000:
    print(Fore.YELLOW + "⚙️ Escala detectada: valores muito altos — aplicando /1000.")
    df_base['y'] = df_base['y'] / 1000
elif mediana_valores < 1_000 and mediana_valores > 0:
    print(Fore.YELLOW + "⚙️ Escala detectada: valores muito baixos — aplicando *1000.")
    df_base['y'] = df_base['y'] * 1000
else:
    print(Fore.GREEN + "✅ Escala adequada — sem ajuste.")

print(Fore.CYAN + f"📊 Média após correção: R$ {df_base['y'].mean():,.2f}")

# ===============================================================
# 📆 AGREGAÇÃO MENSAL
# ===============================================================
df_mensal = df_base.copy()
df_mensal['mes'] = df_mensal['ds'].dt.to_period('M')
df_mensal = df_mensal.groupby('mes')['y'].sum().reset_index()
df_mensal['mes'] = df_mensal['mes'].dt.to_timestamp()

ultimo_mes_historico = df_mensal['mes'].max()
print(Fore.CYAN + f"📅 Histórico até {ultimo_mes_historico.strftime('%b/%Y')}")

# ===============================================================
# 🔮 MODELAGEM E PROJEÇÃO (ATÉ DEZ/2028)
# ===============================================================
modelo = Prophet(
    seasonality_mode='multiplicative',
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False
)
modelo.fit(df_mensal.rename(columns={'mes': 'ds', 'y': 'y'}))

# 🔧 Calcula quantos meses faltam até dezembro de 2028
data_fim_projecao = pd.Timestamp('2028-12-31')
meses_ate_fim = (data_fim_projecao.year - ultimo_mes_historico.year) * 12 + (12 - ultimo_mes_historico.month)

# Gera períodos mensais até dezembro/2028
futuro = modelo.make_future_dataframe(periods=meses_ate_fim, freq='M')
previsao = modelo.predict(futuro)

# ===============================================================
# 📊 RESULTADOS
# ===============================================================
df_resultado = previsao[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
df_resultado.columns = ['Data', 'Faturamento Previsto', 'Limite Inferior', 'Limite Superior']

df_final = pd.merge(df_mensal, df_resultado, left_on='mes', right_on='Data', how='right')
df_final['Faturamento Real'] = df_final['y']
df_final.drop(columns=['y', 'mes'], inplace=True)

# ===============================================================
# ➕ ANÁLISE ADICIONAL
# ===============================================================
# 🗓️ Cria colunas auxiliares
df_final['DATA_ANO'] = df_final['Data'].dt.strftime('%b/%Y')
df_final['MÊS'] = df_final['Data'].dt.strftime('%B')

# 📈 Substitui variação simples por crescimento médio mês a mês
df_final['Crescimento Médio Mês a Mês'] = (
    (df_final['Faturamento Previsto'] / df_final['Faturamento Previsto'].shift(1) - 1) * 100
)
df_final['Crescimento Médio Mês a Mês'] = df_final['Crescimento Médio Mês a Mês'].rolling(window=3).mean()

df_final['Projeção Acumulada'] = df_final['Faturamento Previsto'].cumsum()

# ===============================================================
# 🧾 RESUMO POR ANO
# ===============================================================
df_final['Ano'] = df_final['Data'].dt.year

# ===============================================================
# 🇧🇷 AJUSTES DE LAYOUT E LOCALIZAÇÃO
# ===============================================================
import locale

# Tenta definir o idioma para português do Brasil
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
except:
    locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')

# Reconstrói as colunas com o formato e ordem desejados
df_final['DATA_ANO'] = df_final['Data'].dt.strftime('%b/%Y').str.capitalize()
df_final['MÊS'] = df_final['Data'].dt.strftime('%B').str.capitalize()
df_final['Ano'] = df_final['Data'].dt.year

# Reorganiza a ordem das colunas
colunas_ordenadas = ['DATA_ANO', 'MÊS', 'Ano'] + [col for col in df_final.columns if col not in ['DATA_ANO', 'MÊS', 'Ano', 'Data']]
df_final = df_final[colunas_ordenadas]

# Remove a coluna "Data" (não mais necessária)
if 'Data' in df_final.columns:
    df_final = df_final.drop(columns=['Data'])

resumo_anual = df_final.groupby('Ano')['Faturamento Previsto'].sum().reset_index()
resumo_anual = resumo_anual[resumo_anual['Ano'].between(2026, 2028)]

print(Fore.LIGHTMAGENTA_EX + "\n💰 PREVISÃO TOTAL POR ANO (2026–2028):")
for _, linha in resumo_anual.iterrows():
    print(f"  {linha['Ano']}: R$ {linha['Faturamento Previsto']:,.2f}")

# ===============================================================
# 💾 EXPORTAÇÃO
# ===============================================================
arquivo_saida = f"PROJEÇÃO DE FATURAMENTO 2023_2028.xlsx"
with pd.ExcelWriter(arquivo_saida, engine='openpyxl') as writer:
    df_final.to_excel(writer, sheet_name='Projecao_Completa', index=False)
    resumo_anual.to_excel(writer, sheet_name='Resumo_Anual', index=False)

# ===============================================================
# ✅ LOG FINAL
# ===============================================================
ultimo_mes_previsto = df_resultado['Data'].max()
print(Fore.GREEN + "\n===============================================================")
print(Fore.GREEN + "✅ PROJEÇÃO CONCLUÍDA COM SUCESSO - VERSÃO 28.5")
print(Fore.CYAN + f"📂 Arquivo salvo: {arquivo_saida}")
print(Fore.MAGENTA + f"📈 Último mês projetado: {ultimo_mes_previsto.strftime('%b/%Y')}")
print(Fore.LIGHTYELLOW_EX + "🧮 Inclui NOV e DEZ (2026, 2027, 2028) e colunas DATA_ANO + MÊS")
print(Fore.GREEN + "===============================================================\n")
