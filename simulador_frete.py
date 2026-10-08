import streamlit as st
import pandas as pd
import numpy as np

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Motor de Frete Frio Peças v2.0", layout="wide")

# --- SIMULAÇÃO DE BANCO DE DADOS (PARAMETRIZAÇÃO) ---
# Em um cenário real, esses dados viriam de tabelas SQL ou Planilhas
def load_mock_data():
    # Tabela de Frete Exemplo
    freight_table = pd.DataFrame({
        'ZipCodeStart': [1000000, 5000000],
        'ZipCodeEnd': [4999999, 9999999],
        'WeightStart': [0.0, 0.0],
        'WeightEnd': [100.0, 100.0],
        'CostFixed': [50.0, 80.0],
        'ExtraKg': [2.5, 3.0],
        'AdValorem': [0.01, 0.015], # 1% e 1.5%
        'MinFreight': [60.0, 90.0],
        'FatorCubagem': [300, 300]
    })

    # Matriz ICMS (Origem -> Destino)
    icms_matrix = {
        ('MG', 'SP'): 0.12,
        ('MG', 'RJ'): 0.12,
        ('SP', 'MG'): 0.07,
    }

    # Histórico para SLA (Simulando Moda)
    history_sla = pd.DataFrame({
        'ZipCode': [1000000, 1000000, 1000000, 2000000],
        'City': ['Sao Paulo', 'Sao Paulo', 'Sao Paulo', 'BH'],
        'Region': ['Sudeste', 'Sudeste', 'Sudeste', 'Sudeste'],
        'TimeCost': [3, 3, 5, 2] # A moda para 1000000 é 3
    })

    return freight_table, icms_matrix, history_sla

# --- MOTOR DE CÁLCULO ---
class FreightEngine:
    @staticmethod
    def calculate(zip_dest, weight_phys, volume, nf_value, origin_uf, dest_uf, tables):
        f_table, icms_map, _ = tables

        # Passo 1: Cubagem
        fator = f_table.iloc[0]['FatorCubagem'] # Simplificado
        weight_cubed = volume * fator
        weight_taxable = max(weight_phys, weight_cubed)

        # Passo 2: Enquadramento
        match = f_table[
            (zip_dest >= f_table['ZipCodeStart']) &
            (zip_dest <= f_table['ZipCodeEnd']) &
            (weight_taxable >= f_table['WeightStart']) &
            (weight_taxable <= f_table['WeightEnd'])
        ]

        if match.empty:
            return {"error": "Sem cobertura: CEP ou Peso fora da malha."}

        row = match.iloc[0]

        # Passo 3: Composição do Custo
        cost_fixed = row['CostFixed']
        extra = (weight_taxable - row['WeightStart']) * row['ExtraKg']
        seguro = nf_value * row['AdValorem']

        subtotal = cost_fixed + extra + seguro
        subtotal = max(subtotal, row['MinFreight'])

        # Passo 4: ICMS por dentro
        aliquota = icms_map.get((origin_uf, dest_uf), 0.0)
        freight_final = subtotal / (1 - aliquota)

        return {
            "Peso Tarifável": weight_taxable,
            "Subtotal": subtotal,
            "Alíquota ICMS": aliquota,
            "Frete Final": freight_final,
            "Método": "Cálculo Motor v2.0"
        }

    @staticmethod
    def get_sla(zip_dest, city, region, history_df):
        # Nível 1: CEP
        subset = history_df[history_df['ZipCode'] == zip_dest]
        if not subset.empty:
            return int(subset['TimeCost'].mode()[0]), "Histórico (CEP)"

        # Nível 2: Cidade
        subset = history_df[history_df['City'] == city]
        if not subset.empty:
            return int(subset['TimeCost'].mode()[0]), "Cidade"

        # Nível 3: Região
        subset = history_df[history_df['Region'] == region]
        if not subset.empty:
            return int(subset['TimeCost'].mode()[0]), "Região"

        return None, "Sem SLA"

# --- INTERFACE STREAMLIT ---
st.title("🚚 Validador de Frete & SLA - Frio Peças")

# Sidebar - Parâmetros
st.sidebar.header("Parâmetros do Pedido")
zip_input = st.sidebar.number_input("CEP Destino (8 dígitos)", value=1000000)
weight_input = st.sidebar.number_input("Peso Físico (kg)", value=10.0)
vol_input = st.sidebar.number_input("Volume (m³)", value=0.05)
nf_input = st.sidebar.number_input("Valor da NF (R$)", value=1500.0)
origem = st.sidebar.selectbox("UF Origem", ["MG", "SP"])
destino = st.sidebar.selectbox("UF Destino", ["SP", "RJ", "MG"])

# Carregar dados
tables = load_mock_data()

if st.sidebar.button("Calcular Frete e SLA"):
    engine = FreightEngine()

    col1, col2 = st.columns(2)

    # Executar Frete
    res = engine.calculate(zip_input, weight_input, vol_input, nf_input, origem, destino, tables)

    with col1:
        st.subheader("Resultado do Frete")
        if "error" in res:
            st.error(res["error"])
        else:
            st.json(res)
            st.metric("Frete Final", f"R$ {res['Frete Final']:.2f}")

    # Executar SLA
    with col2:
        st.subheader("Resultado do SLA")
        # Mock de cidade/região para o teste
        prazo, etiqueta = engine.get_sla(zip_input, "Sao Paulo", "Sudeste", tables[2])
        if prazo:
            st.metric("Prazo Estimado", f"{prazo} dias")
            st.info(f"Nível de busca: {etiqueta}")
        else:
            st.warning("Prazo não encontrado no histórico.")

st.divider()
st.caption("Regra de Negócio: Economia = Frete Real - Frete Simulado. TimeCost vazio é erro de cadastro.")