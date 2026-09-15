import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime, timedelta  

st.set_page_config(page_title="P.O de Tarefas - Análise de Dados", layout="wide")

## BANCO DE DADOS ATIVO ##
def get_connection():
    return sqlite3.connect("tarefas.db", check_same_thread=False)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tarefas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            demanda TEXT NOT NULL,
            projeto_area TEXT,
            prioridade TEXT NOT NULL,
            prazo DATE NOT NULL,
            estimativa TEXT,
            observacoes TEXT,
            status TEXT NOT NULL,
            data_criacao DATETIME NOT NULL,
            data_conclusao DATETIME NOT NULL
        )
    """)
    conn.commit()

    init_db()

    def render_form_demanda(key_prefix="form"):
        with st.form(f"{key_prefix}_nova_demanda", clear_on_submit=True):
            st.markdown("### Nova Demanda")
            st.markdown("Preencha os campos abaixo para adicionar uma nova demanda ao P.O de Tarefas.")

            demanda = st.text_input("Demanda:*")
            projeto_area = st.text_input("Projeto/Área:")
            prioridade = st.selectbox("Prioridade:*", ["Baixa", "Média", "Alta", "Urgente"], index=2)
            prazo = st.date_input("Prazo:*", value=date.today())
            estimativa = st.text_input("Estimativa de Tempo:*", placeholder="Ex: 2 dias, 4 horas, 1 hora, 30 min, etc")
            observacoes = st.text_area("Observações:")
            status = st.selectbox("Status:*", ["Pendente", "Em Andamento", "Concluída"], index=0)
            data_criacao = datetime.now()
            data_conclusao = st.datetime_input("Data de Conclusão:", value=None, min_value=date.today(), max_value=today() + timedelta(days=30))

            btn_salvar = st.form_submit_button("Salvar Demanda", use_container_width=True)

            if btn_salvar:
                if not demanda:
                    st.error("O campo 'Demanda' é obrigatório.")
                elif not prazo:
                    st.error("O campo 'Prazo' é obrigatório.")
                elif not estimativa:
                    st.error("O campo 'Estimativa de Tempo' é obrigatório.")
                elif not status:
                    st.error("O campo 'Status' é obrigatório.")
                else:
                    conn = get_connection()
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO tarefas (demanda, projeto_area, prioridade, prazo, estimativa, observacoes, status, data_criacao, data_conclusao)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (demanda, projeto_area, prioridade, prazo, estimativa, observacoes, status, data_criacao, data_conclusao))
                    conn.commit()
                    st.success("Demanda adicionada com sucesso!")
                    st.rerun()

### ESTRUTURA DAS ABAS DO PLANEJADOR DE TAREFAS ###

aba1, aba2, aba3 = st.tabs(["📋 Registro de Demandas", "📊 Planner", "⚙️ Analytics"])

## ABA 1: Registro de Demandas ##

with aba1:
    col_centered, _ = st.columns([1, 1])
    with col_centered:
        render_form_demanda(key_prefix="aba1")

## ABA 2: Planner ##

with aba2:
    col_titulo, col_btn = st.columnes([3, 1])
    with col_titulo:
        st.markdown("### Planner de Demandas")
        st.markdown("Visualize e gerencie suas demandas de forma organizada.")
    with col_btn:
        @st.dialog("Nova Demanda")
        def modal_nova_tarefa():
            render_form_demanda(key_prefix="modal")

        if st.button("➕ Adicionar Demanda", use_container_width=True, type="primary"):
            modal_nova_tarefa()

        st.markdown("### Filtro de Demandas")

        conn = get.connection()
        df = pd.read_sql_query("SELECT * FROM tarefas", conn)

        today_str = date.today().strftime("%Y-%m-%d")

        if not df.empty:
            df['Prazo'] = df['Prazo'].astype(str)

            atrasadas = len(df[(df['Prazo'] < today_str) & (df['Status'] != 'Concluída')])
            hoje = len(df[(df['Prazo'] == today_str) & (df['Status'] != 'Concluída')])
            em_andamento = len(df[df['Status'] == 'Em Andamento'])
            concluidas = len(df[df['Status'] == 'Concluída'])
        else:
            atrasadas, hoje, em_andamento, concluidas = 0, 0, 0, 0
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("🔴 ATRASADAS", atrasadas)
        m2.metric("🟡 HOJE", hoje)
        m3.metric("🔵 EM ANDAMENTO", em_andamento)
        m4.metric("🟢 CONCLUÍDAS", concluidas)

        st.markdown("---")

        col_urg, col_med, col_and = st.columns(3)

        cor_prioridade = {
        "Urgente": "🔴",
        "Alta": "🟠",
        "Média": "🟡",
        "Baixa": "🟢"
    }

    def render_card(row):
        try:
            prazo_fmt = datetime.strptime(row['Prazo'], "%Y-%m-%d").strftime("%d/%m/%Y")
        except:
            prazo_fmt = row['Prazo']

        st.markdown(f"**{row['Demanda']}**")
        st.markdown(f"**Área:** {row['Projeto_Area']}")
        st.markdown(f"**Prioridade:** {cor_prioridade.get(row['Prioridade'], '🟢')}")
        st.markdown(f"**Prazo:** {prazo_fmt}")
        st.markdown(f"**Estimativa:** {row['Estimativa']}")
        st.markdown(f"**Observações:** {row['Observacoes']}")
        st.markdown(f"**Status:** {row['Status']}")

        with st.expander("Ações"):
            col_edit, col_delete = st.columns(2)
            with col_edit:
                if st.button("✏️ Editar", key=f"edit_{row['ID']}"):
                    st.session_state['edit_id'] = row['ID']
                    st.session_state['edit_demanda'] = row['Demanda']
                    st.session_state['edit_projeto_area'] = row['Projeto_Area']
                    st.session_state['edit_prioridade'] = row['Prioridade']
                    st.session_state['edit_prazo'] = row['Prazo']
                    st.session_state['edit_estimativa'] = row['Estimativa']
                    st.session_state['edit_observacoes'] = row['Observacoes']
                    st.session_state['edit_status'] = row['Status']
                    st.experimental_rerun()

            with col_delete:
                if st.button("🗑️ Excluir", key=f"delete_{row['ID']}"):
                    conn = get_connection()
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM tarefas WHERE id=?", (row['ID'],))
                    conn.commit()
                    st.success("Demanda excluída com sucesso!")
                    st.experimental_rerun()
                st.markdown("---")

           with col_urg:
        st.markdown("### 🔴 URGENTE / ALTA")
        if not df.empty:
            df_urg = df[(df['prioridade'].isin(["Urgente", "Alta"])) & (df['status'] != "Concluído")]
            for _, row in df_urg.iterrows():
                render_card(row)

    with col_med:
        st.markdown("### 🟡 MÉDIA / BAIXA")
        if not df.empty:
            df_med = df[(df['prioridade'].isin(["Média", "Baixa"])) & (df['status'] != "Concluído")]
            for _, row in df_med.iterrows():
                render_card(row)

    with col_and:
        st.markdown("### 🔵 EM ANDAMENTO")
        if not df.empty:
            df_and = df[df['status'] == "Em Andamento"]
            for _, row in df_and.iterrows():
                render_card(row)

### ABA 3: Analytics de Entregas ###

with aba3:
    st.markdown("## ANALYTICS DE ENTREGAS")
    conn = get_connection()
    df_analytics = pd.read_sql_query("SELECT * FROM tarefas", conn)
    
    if df_analytics.empty:
        st.info("Nenhuma demanda cadastrada para exibir relatórios.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### Demandas por Projeto/Área")
            st.bar_chart(df_analytics['projeto_area'].value_counts())
        with c2:
            st.markdown("##### Distribuição por Status")
            st.bar_chart(df_analytics['status'].value_counts())