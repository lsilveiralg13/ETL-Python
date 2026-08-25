import streamlit as st

st.set_page_config(page_title="Simulador Previdenciário", layout="wide")

st.title("🛡️ Simulador de Aposentadoria e Regras de Transição")

# Upload do CNIS
uploaded_file = st.sidebar.file_uploader("Suba o PDF do CNIS", type=["pdf"])

if uploaded_file:
    st.success("CNIS carregado com sucesso!")

# Renderiza a tabela de cenários calculados pelo motor
df_cenarios = motor.consolidar_cenarios()
st.dataframe(df_cenarios, use_container_width=True)