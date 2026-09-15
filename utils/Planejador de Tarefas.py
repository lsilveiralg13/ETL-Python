import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime, timedelta  

st.set_page_config(page_title="P.O de Tarefas - Análise de Dados", layout="wide")

# ==========================================
# BANCO DE DADOS ROBUSTO
# ==========================================
def get_connection():
    return sqlite3.connect("tarefas.db", check_same_thread=False)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Tabela Principal
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tarefas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            demanda TEXT NOT NULL,
            projeto_area TEXT,
            sponsor TEXT,
            area_sponsor TEXT,
            prioridade TEXT NOT NULL,
            prazo DATE NOT NULL,
            estimativa TEXT,
            observacoes TEXT,
            status TEXT NOT NULL,
            data_criacao DATETIME NOT NULL,
            data_conclusao DATETIME
        )
    """)
    
    # Tabela de Subtarefas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS subtarefas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tarefa_id INTEGER NOT NULL,
            item_texto TEXT NOT NULL,
            concluido INTEGER DEFAULT 0,
            FOREIGN KEY (tarefa_id) REFERENCES tarefas (id) ON DELETE CASCADE
        )
    """)
    conn.commit()

    for coluna, tipo in [("sponsor", "TEXT"), ("area_sponsor", "TEXT")]:
        try:
            cursor.execute(f"ALTER TABLE tarefas ADD COLUMN {coluna} {tipo}")
            conn.commit()
        except sqlite3.OperationalError:
            pass

init_db()

# ==========================================
# FORMULÁRIO DE DEMANDAS
# ==========================================
def render_form_demanda(key_prefix="form"):
    with st.form(f"{key_prefix}_nova_demanda", clear_on_submit=True):
        st.markdown("### 📝 Nova Demanda")
        st.markdown("Preencha os campos abaixo para adicionar uma nova demanda.")

        demanda = st.text_input("Demanda:*", key=f"{key_prefix}_demanda")
        
        col_proj, col_spon, col_area_spon = st.columns(3)
        with col_proj:
            projeto_area = st.text_input("Projeto/Área:", key=f"{key_prefix}_proj")
        with col_spon:
            sponsor = st.text_input("Sponsor (Stakeholder):", key=f"{key_prefix}_spon")
        with col_area_spon:
            area_sponsor = st.text_input("Área do Stakeholder:", key=f"{key_prefix}_area_spon")

        col_prio, col_prazo, col_est = st.columns(3)
        with col_prio:
            prioridade = st.selectbox("Prioridade:*", ["Baixa", "Média", "Alta", "Urgente"], index=2, key=f"{key_prefix}_prio")
        with col_prazo:
            prazo = st.date_input("Prazo:*", value=date.today(), key=f"{key_prefix}_prazo")
        with col_est:
            estimativa = st.text_input("Estimativa de Tempo:*", placeholder="Ex: 2 dias, 4 horas, 30 min", key=f"{key_prefix}_est")

        observacoes = st.text_area("Observações:", key=f"{key_prefix}_obs")
        status = st.selectbox("Status:*", ["Pendente", "Em Andamento", "Concluída"], index=0, key=f"{key_prefix}_status")
        
        data_criacao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        btn_salvar = st.form_submit_button("💾 Salvar Demanda", use_container_width=True)

        if btn_salvar:
            if not demanda.strip():
                st.error("❌ O campo 'Demanda' é obrigatório.")
            elif not estimativa.strip():
                st.error("❌ O campo 'Estimativa de Tempo' é obrigatório.")
            else:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO tarefas (demanda, projeto_area, sponsor, area_sponsor, prioridade, prazo, estimativa, observacoes, status, data_criacao)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (demanda, projeto_area, sponsor, area_sponsor, prioridade, prazo, estimativa, observacoes, status, data_criacao))
                conn.commit()
                st.success("✅ Demanda cadastrada com sucesso!")
                st.rerun()

# ==========================================
# ESTRUTURA DE ABAS
# ==========================================
aba1, aba2, aba3 = st.tabs(["📋 Registro de Demandas", "📊 Planner (Kanban)", "⚙️ Analytics & Performance"])

# ------------------------------------------
# ABA 1: REGISTRO
# ------------------------------------------
with aba1:
    col_centered, _ = st.columns([2, 0.1])
    with col_centered:
        render_form_demanda(key_prefix="aba1")

# ------------------------------------------
# ABA 2: PLANNER (KANBAN COM EDIÇÃO E CHECKLIST)
# ------------------------------------------
with aba2:
    col_titulo, col_btn = st.columns([3, 1])
    with col_titulo:
        st.markdown("### Planner Kanban")
        st.markdown("Gerencie o fluxo de vida das suas demandas e subitens.")
    with col_btn:
        @st.dialog("Nova Demanda")
        def modal_nova_tarefa():
            render_form_demanda(key_prefix="modal")

        if st.button("➕ Adicionar Demanda", use_container_width=True, type="primary"):
            modal_nova_tarefa()

    st.markdown("---")

    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM tarefas", conn)
    today_str = date.today().strftime("%Y-%m-%d")

    if not df.empty:
        df['prazo'] = df['prazo'].astype(str)

        busca = st.text_input("🔍 Filtrar demandas por palavra-chave:", "")
        if busca:
            df = df[
                df['demanda'].str.contains(busca, case=False, na=False) |
                df['projeto_area'].str.contains(busca, case=False, na=False) |
                df['sponsor'].str.contains(busca, case=False, na=False)
            ]

        pendentes = len(df[df['status'] == 'Pendente'])
        em_andamento = len(df[df['status'] == 'Em Andamento'])
        concluidas = len(df[df['status'] == 'Concluída'])
        atrasadas = len(df[(df['prazo'] < today_str) & (df['status'] != 'Concluída')])
    else:
        pendentes, em_andamento, concluidas, atrasadas = 0, 0, 0, 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("🔴 PENDENTES", pendentes)
    m2.metric("🔵 EM ANDAMENTO", em_andamento)
    m3.metric("🟢 CONCLUÍDAS", concluidas)
    m4.metric("⚠️ ATRASADAS", atrasadas)

    st.markdown("---")

    col_pend, col_and, col_conc = st.columns(3)
    cor_prioridade = {"Urgente": "🔴 Urgente", "Alta": "🟠 Alta", "Média": "🟡 Média", "Baixa": "🟢 Baixa"}

    def render_card(row, prefix="card"):
        c = conn.cursor()
        c.execute("SELECT id, item_texto, concluido FROM subtarefas WHERE tarefa_id = ? ORDER BY id ASC", (row['id'],))
        subitems = c.fetchall()

        total_items = len(subitems)
        concluidos_count = sum(1 for item in subitems if item[2] == 1)
        todas_subtarefas_concluidas = (total_items > 0) and (concluidos_count == total_items)

        with st.container(border=True):
            try:
                prazo_fmt = datetime.strptime(row['prazo'], "%Y-%m-%d").strftime("%d/%m/%Y")
                prazo_dt = datetime.strptime(row['prazo'], "%Y-%m-%d").date()
            except:
                prazo_fmt = row['prazo']
                prazo_dt = date.today()

            is_concluida = (row['status'] == 'Concluída') or todas_subtarefas_concluidas
            is_atrasada = (row['prazo'] < today_str) and not is_concluida

            st.markdown(f"#### {row['demanda']}")

            if is_concluida:
                st.success("✅ **Demanda 100% Concluída**")
            elif is_atrasada:
                st.error("🚨 **TAREFA ATRASADA**")

            st.markdown(f"**Área:** {row['projeto_area'] or 'N/A'}")
            if row.get('sponsor'):
                st.markdown(f"**Sponsor:** {row['sponsor']} *({row.get('area_sponsor') or 'N/A'})*")
            
            st.markdown(f"**Prioridade:** {cor_prioridade.get(row['prioridade'], '🟢 Baixa')}")
            st.markdown(f"**Prazo:** {prazo_fmt} | **Est:** {row['estimativa']}")

            if row.get('observacoes'):
                st.caption(f"**Obs:** {row['observacoes']}")

            # Checklist
            st.markdown("---")
            if total_items > 0:
                progresso = concluidos_count / total_items
                st.progress(progresso, text=f"Checklist: {concluidos_count}/{total_items} concluídos")

            for idx, (sub_id, item_texto, status_item) in enumerate(subitems, start=1):
                checked = st.checkbox(f"**{idx}.** {item_texto}", value=bool(status_item), key=f"{prefix}_sub_{sub_id}")
                if checked != bool(status_item):
                    c.execute("UPDATE subtarefas SET concluido = ? WHERE id = ?", (1 if checked else 0, sub_id))
                    c.execute("SELECT COUNT(*) FROM subtarefas WHERE tarefa_id = ? AND concluido = 0", (row['id'],))
                    if c.fetchone()[0] == 0 and total_items > 0:
                        dt_conc = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        c.execute("UPDATE tarefas SET status='Concluída', data_conclusao=? WHERE id=?", (dt_conc, row['id']))
                    else:
                        c.execute("UPDATE tarefas SET status='Em Andamento' WHERE id=?", (row['id'],))
                    conn.commit()
                    st.rerun()

            with st.expander("➕ Adicionar Item ao Checklist"):
                novo_item = st.text_input("Texto da Subtarefa:", key=f"{prefix}_newitem_{row['id']}")
                if st.button("Inserir Subtarefa", key=f"{prefix}_btnitem_{row['id']}", use_container_width=True):
                    if novo_item.strip():
                        c.execute("INSERT INTO subtarefas (tarefa_id, item_texto, concluido) VALUES (?, ?, 0)", (row['id'], novo_item.strip()))
                        conn.commit()
                        st.rerun()

            # EDIÇÃO DE DADOS DO CARD
            with st.expander("⚙️ Editar / Gerenciar Demanda"):
                with st.form(key=f"{prefix}_edit_form_{row['id']}"):
                    st.markdown("##### ✏️ Editar Informações")
                    e_demanda = st.text_input("Demanda:", value=row['demanda'])
                    e_proj = st.text_input("Projeto/Área:", value=row['projeto_area'] or "")
                    
                    col_e1, col_e2 = st.columns(2)
                    with col_e1:
                        e_spon = st.text_input("Sponsor:", value=row['sponsor'] or "")
                    with col_e2:
                        e_area_spon = st.text_input("Área Sponsor:", value=row['area_sponsor'] or "")

                    col_e3, col_e4 = st.columns(2)
                    with col_e3:
                        prio_opts = ["Baixa", "Média", "Alta", "Urgente"]
                        prio_idx = prio_opts.index(row['prioridade']) if row['prioridade'] in prio_opts else 0
                        e_prio = st.selectbox("Prioridade:", prio_opts, index=prio_idx)
                    with col_e4:
                        e_prazo = st.date_input("Prazo:", value=prazo_dt)

                    col_e5, col_e6 = st.columns(2)
                    with col_e5:
                        e_est = st.text_input("Estimativa:", value=row['estimativa'] or "")
                    with col_e6:
                        status_opts = ["Pendente", "Em Andamento", "Concluída"]
                        st_idx = status_opts.index(row['status']) if row['status'] in status_opts else 0
                        e_status = st.selectbox("Status:", status_opts, index=st_idx)

                    e_obs = st.text_area("Observações:", value=row['observacoes'] or "")

                    btn_salvar_edicao = st.form_submit_button("💾 Salvar Alterações", use_container_width=True)

                    if btn_salvar_edicao:
                        dt_conc = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if e_status == "Concluída" else None
                        c.execute("""
                            UPDATE tarefas 
                            SET demanda=?, projeto_area=?, sponsor=?, area_sponsor=?, prioridade=?, prazo=?, estimativa=?, observacoes=?, status=?, data_conclusao=?
                            WHERE id=?
                        """, (e_demanda, e_proj, e_spon, e_area_spon, e_prio, e_prazo, e_est, e_obs, e_status, dt_conc, row['id']))
                        conn.commit()
                        st.success("Atualizado!")
                        st.rerun()

                st.markdown("---")
                if st.button("🗑️ Excluir Demanda", key=f"{prefix}_del_{row['id']}", use_container_width=True, type="secondary"):
                    c.execute("DELETE FROM subtarefas WHERE tarefa_id=?", (row['id'],))
                    c.execute("DELETE FROM tarefas WHERE id=?", (row['id'],))
                    conn.commit()
                    st.rerun()

    with col_pend:
        st.markdown("### 🔴 PENDENTE")
        if not df.empty:
            df_pend = df[df['status'] == "Pendente"]
            for _, row in df_pend.iterrows():
                render_card(row, prefix="pend")

    with col_and:
        st.markdown("### 🔵 EM ANDAMENTO")
        if not df.empty:
            df_and = df[df['status'] == "Em Andamento"]
            for _, row in df_and.iterrows():
                render_card(row, prefix="and")

    with col_conc:
        st.markdown("### 🟢 CONCLUÍDA")
        if not df.empty:
            df_conc = df[df['status'] == "Concluída"]
            for _, row in df_conc.iterrows():
                render_card(row, prefix="conc")

# ------------------------------------------
# ABA 3: ANALYTICS E INDICADORES
# ------------------------------------------
with aba3:
    st.markdown("## ANALYTICS & INDICADORES DE DESEMPENHO")
    conn = get_connection()
    df_analytics = pd.read_sql_query("SELECT * FROM tarefas", conn)
    
    if df_analytics.empty:
        st.info("Nenhuma demanda cadastrada para exibir relatórios.")
    else:
        # Download
        csv = df_analytics.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Exportar Relatório Completo (CSV)",
            data=csv,
            file_name=f"relatorio_tarefas_{date.today()}.csv",
            mime="text/csv",
        )
        st.markdown("---")

        # CÁLCULO DOS INDICADORES CHAVE (KPIs)
        total_demandas = len(df_analytics)
        total_concluidas = len(df_analytics[df_analytics['status'] == 'Concluída'])
        
        df_analytics['prazo'] = df_analytics['prazo'].astype(str)
        today_str = date.today().strftime("%Y-%m-%d")
        total_atrasadas = len(df_analytics[(df_analytics['prazo'] < today_str) & (df_analytics['status'] != 'Concluída')])

        taxa_conclusao = (total_concluidas / total_demandas * 100) if total_demandas > 0 else 0
        taxa_atraso = (total_atrasadas / total_demandas * 100) if total_demandas > 0 else 0

        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("📊 Total de Demandas", total_demandas)
        kpi2.metric("🎯 Taxa de Conclusão", f"{taxa_conclusao:.1f}%")
        kpi3.metric("🚨 Índice de Atraso", f"{taxa_atraso:.1f}%")
        kpi4.metric("⏳ Em Andamento", len(df_analytics[df_analytics['status'] == 'Em Andamento']))

        st.markdown("---")

        # PAINEL DE GRÁFICOS
        g1, g2 = st.columns(2)
        with g1:
            st.markdown("##### 1. Distribuição por Nível de Prioridade")
            st.bar_chart(df_analytics['prioridade'].value_counts())

            st.markdown("##### 2. Volumetria por Projeto / Área")
            st.bar_chart(df_analytics['projeto_area'].value_counts())

        with g2:
            st.markdown("##### 3. Demandas por Sponsor (Stakeholder)")
            if 'sponsor' in df_analytics.columns and not df_analytics['sponsor'].dropna().empty:
                st.bar_chart(df_analytics['sponsor'].value_counts())
            else:
                st.caption("Sem dados de sponsor registrados.")

            st.markdown("##### 4. Evolução de Cadastro no Tempo (Por Data)")
            df_analytics['data_curta'] = df_analytics['data_criacao'].str.slice(0, 10)
            st.line_chart(df_analytics['data_curta'].value_counts().sort_index())