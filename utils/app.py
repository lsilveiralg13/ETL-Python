import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime

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
            Demanda TEXT NOT NULL,
            Projeto_Area TEXT,
            Prioridade TEXT NOT NULL,
            Prazo DATE NOT NULL,
            Estimativa TEXT,
            Observacoes TEXT,
            Status TEXT NOT NULL,
            Data_Criacao DATETIME NOT NULL,
            Data_Conclusao DATETIME NOT NULL
        )
    """)
    conn.commit()

    init_db()

    def render_form_demanda(key_prefix:"form")
        with st.form(f"{key_prefix}_nova_demanda", clear_on_submit=True):
            st.markdown("### Nova Demanda")
            st.markdown("Preencha os campos abaixo para adicionar uma nova demanda ao P.O de Tarefas.")
