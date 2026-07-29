# ===============================================================
# 🧠 ETL CRM MULTIMARCAS - VERSÃO FINAL 2025 (ULTRA-STABLE)
# ===============================================================

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# ===============================================================
# 🎯 CONFIGURAÇÕES
# ===============================================================

INPUT_FILE = Path("11 - CRM MM - Nov.25.xlsx")

OUTPUT_EXCEL = Path("CRM_TRATADO.xlsx")
OUTPUT_CSV = Path("CRM_TRATADO.csv")

RELATORIO_TXT = Path("RELATORIO_ETL.txt")
RELATORIO_XLSX = Path("RELATORIO_ETL.xlsx")

PBI_FATO = Path("BASE_PBI_FATO_LEADS.xlsx")
PBI_DIM_CIDADE = Path("BASE_PBI_DIM_CIDADE.xlsx")
PBI_DIM_SDR = Path("BASE_PBI_DIM_SDR.xlsx")
PBI_DIM_STATUS = Path("BASE_PBI_DIM_STATUS.xlsx")

SDR_SHEETS = ["ANDRÉ", "DUDA", "RAIANE", "ROBERTA", "SCARLAT", "TARVYLLA"]
IBGE_SHEET = "BASE DE CIDADES IBGE"


# ===============================================================
# 🧩 FUNÇÕES AUXILIARES
# ===============================================================

def normalize_text(text: str) -> str:
    if pd.isna(text):
        return np.nan
    return str(text).strip().upper()


def clean_phone(phone: str) -> str:
    if pd.isna(phone):
        return np.nan
    only_digits = "".join(ch for ch in str(phone) if ch.isdigit())
    return only_digits if only_digits else np.nan


def clean_cnpj(cnpj: str) -> str:
    if pd.isna(cnpj):
        return np.nan
    digits = "".join(ch for ch in str(cnpj) if ch.isdigit())
    return digits if digits else np.nan


def build_city_key(city: str, uf: str) -> str:
    city_norm = normalize_text(city)
    uf_norm = normalize_text(uf)
    if pd.isna(city_norm) or pd.isna(uf_norm):
        return np.nan
    return f"{city_norm}|{uf_norm}"


# ===============================================================
# 📥 LEITURA SDR
# ===============================================================

def load_and_concatenate_sdr_sheets(input_file: Path, sheets: list) -> pd.DataFrame:
    frames = []
    for sheet in sheets:
        print(f"Lendo aba: {sheet}...")
        df = pd.read_excel(input_file, sheet_name=sheet)
        df["NomeSDR"] = sheet.upper().replace(" ", "")
        frames.append(df)
    full = pd.concat(frames, ignore_index=True)
    print(f"Total de linhas (todas as abas SDR): {len(full)}")
    return full


# ===============================================================
# 🧼 LIMPEZA
# ===============================================================

def filter_real_leads(df: pd.DataFrame) -> pd.DataFrame:
    mask = df["ID"].notna() & df["Nome do Cliente"].notna()
    filtered = df.loc[mask].copy()
    print(f"Leads reais após filtro: {len(filtered)}")
    return filtered


def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Datas
    for col in ["DATA", "Data do Próximo Contato"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Telefone
    df["Contato_limpo"] = df["Contato"].apply(clean_phone)

    # CNPJ
    df["CNPJ_limpo"] = df["CNPJ"].apply(clean_cnpj)

    # Localização
    df["Cidade_norm"] = df["Cidade"].apply(normalize_text)
    df["UF_norm"] = df["UF"].apply(normalize_text)
    df["CHAVE_CIDADE_UF"] = df.apply(
        lambda r: build_city_key(r["Cidade"], r["UF"]), axis=1
    )

    # Dias sem contato
    hoje = pd.Timestamp.today().normalize()
    df["Dias desde o Ultim. Contato"] = (hoje - df["DATA"]).dt.days

    return df


# ===============================================================
# 🌍 IBGE
# ===============================================================

def load_ibge_base(input_file: Path) -> pd.DataFrame:
    print("Lendo base IBGE: aba 'BASE DE CIDADES IBGE'...")
    ibge = pd.read_excel(input_file, sheet_name=IBGE_SHEET)

    ibge = ibge[ibge["NOME DO MUNICÍPIO"].notna()].copy()

    ibge["Cidade_norm"] = ibge["NOME DO MUNICÍPIO"].apply(normalize_text)
    ibge["UF_norm"] = ibge["UF"].apply(normalize_text)

    ibge["CHAVE_CIDADE_UF"] = ibge.apply(
        lambda r: build_city_key(r["NOME DO MUNICÍPIO"], r["UF"]), axis=1
    )

    print(f"Total de cidades IBGE consideradas: {len(ibge)}")
    return ibge


def enrich_with_ibge(df: pd.DataFrame, ibge: pd.DataFrame) -> pd.DataFrame:
    desired_cols = [
        "CHAVE_CIDADE_UF", "NOME DO MUNICÍPIO",
        "POPULAÇÃO ESTIMADA", "Mesorregião", "UF", "ESTADO",
        "TEM FRANQUIA", "TEM MULTIMARCAS?", "QQTD",
        "BLOCO POPULACIONAL"
    ]

    # Apenas o que existe
    ibge_cols_to_keep = [c for c in desired_cols if c in ibge.columns]
    missing_cols = [c for c in desired_cols if c not in ibge.columns]

    if missing_cols:
        print(f"⚠ Atenção: As seguintes colunas NÃO existem na base IBGE e serão ignoradas: {missing_cols}")

    ibge_subset = ibge[ibge_cols_to_keep]

    merged = df.merge(ibge_subset, on="CHAVE_CIDADE_UF", how="left")

    match = merged["NOME DO MUNICÍPIO"].notna().sum()
    total = len(merged)
    print(f"Cobertura IBGE: {match} de {total} leads ({match/total:.1%})")

    return merged


# ===============================================================
# 🟥 PRIORIDADE + FAROL
# ===============================================================

def compute_priority(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    score_final = []

    for _, row in df.iterrows():
        sc = 0

        # 1. Dias sem contato
        dias = row["Dias desde o Ultim. Contato"]
        if pd.isna(dias):
            sc += 4
        elif dias > 30:
            sc += 3
        elif 15 <= dias <= 30:
            sc += 2
        elif 7 <= dias < 15:
            sc += 1

        # 2. Status
        st = str(row["Status do Lead"]).upper()
        if "PROSPEC" in st:
            sc += 3
        elif "NEGOCIA" in st:
            sc += 2
        elif "CADAST" in st or "CRÉDITO" in st:
            sc += 1
        elif "RESTRI" in st:
            sc -= 2
        elif "SEM PERFIL" in st or "SEM INTERESSE" in st or "DESIST" in st:
            sc -= 5

        # 3. População
        pop = row["POPULAÇÃO ESTIMADA"]
        if pd.notna(pop):
            if pop > 100000:
                sc += 3
            elif 50000 <= pop <= 100000:
                sc += 2
            elif 30000 <= pop < 50000:
                sc += 1
            elif pop < 10000:
                sc -= 2

        # 4. Concorrência
        franq = str(row.get("TEM FRANQUIA", "")).upper()
        mm = str(row.get("TEM MULTIMARCAS?", "")).upper()

        if franq != "SIM" and mm != "SIM":
            sc += 3
        elif franq == "SIM" and mm == "SIM":
            sc -= 3
        elif franq == "SIM":
            sc -= 3
        elif mm == "SIM":
            sc -= 2

        score_final.append(sc)

    df["SCORE_FINAL_INTERNAL"] = score_final

    prioridade = []
    farol = []

    for s in score_final:
        if s >= 7:
            prioridade.append("ALTA")
            farol.append("VERMELHO")
        elif 3 <= s <= 6:
            prioridade.append("MÉDIA")
            farol.append("AMARELO")
        else:
            prioridade.append("BAIXA")
            farol.append("VERDE")

    df["PRIORIDADE_CATEGORIA"] = prioridade
    df["FAROL"] = farol

    return df


# ===============================================================
# 📦 EXPORTAÇÃO PRINCIPAL
# ===============================================================

def export_clean(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "ID", "NomeSDR", "DATA",
        "Nome do Cliente", "Contato_limpo", "Instagram", "CNPJ_limpo",
        "Status do Lead", "Fonte do Lead",
        "Cidade", "UF", "POPULAÇÃO ESTIMADA",
        "Mesorregião", "BLOCO POPULACIONAL",
        "TEM FRANQUIA", "TEM MULTIMARCAS?", "QQTD",
        "Data do Próximo Contato", "Dias desde o Ultim. Contato",
        "MOTIVO" if "MOTIVO" in df.columns else None,
        "PRIORIDADE_CATEGORIA", "FAROL"
    ]

    cols = [c for c in cols if c in df.columns]

    clean = df[cols].copy()
    print(f"Exportando {len(clean)} linhas e {len(clean.columns)} colunas...")

    clean.to_excel(OUTPUT_EXCEL, index=False)
    clean.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")

    print(f"✅ Arquivo Excel gerado: {OUTPUT_EXCEL.resolve()}")
    print(f"✅ Arquivo CSV gerado: {OUTPUT_CSV.resolve()}")

    return clean


# ===============================================================
# 🧾 RELATÓRIOS (CORRIGIDO)
# ===============================================================

def generate_reports(df, ibge, txt_path, xlsx_path):

    # ===== TXT =====
    lines = []
    lines.append("===== RELATORIO ETL CRM =====")
    lines.append(f"Data/Hora: {datetime.now()}")
    lines.append("")
    lines.append(f"Total de leads tratados: {len(df)}")

    if "NomeSDR" in df.columns:
        lines.append("\nLeads por SDR:")
        for name, qtd in df["NomeSDR"].value_counts().items():
            lines.append(f"  - {name}: {qtd}")

    if "Status do Lead" in df.columns:
        lines.append("\nLeads por Status:")
        for name, qtd in df["Status do Lead"].value_counts().items():
            lines.append(f"  - {name}: {qtd}")

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"✅ Relatório TXT gerado: {txt_path.resolve()}")

    # ===== EXCEL =====
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:

        # Aba Nulls
        nulls = df.isna().sum().reset_index()
        nulls.columns = ["Coluna", "Qtde_Nulls"]
        nulls.to_excel(writer, sheet_name="Nulls", index=False)

        # Aba cidades sem IBGE (100% segura)
        uf_cols = [c for c in ["UF", "UF_norm", "UF_IBGE"] if c in df.columns]

        if "Cidade" in df.columns and "NOME DO MUNICÍPIO" in df.columns and uf_cols:
            uf_col = uf_cols[0]
            nf = df[df["NOME DO MUNICÍPIO"].isna()][["Cidade", uf_col]].drop_duplicates()
            nf.to_excel(writer, sheet_name="Cidades_Sem_IBGE", index=False)
        else:
            fallback = pd.DataFrame({"Aviso": ["Colunas para identificar cidades sem IBGE não disponíveis."]})
            fallback.to_excel(writer, sheet_name="Cidades_Sem_IBGE", index=False)

    print(f"✅ Relatório Excel gerado: {xlsx_path.resolve()}")


# ===============================================================
# 🟩 BASES PARA POWER BI (CORRIGIDO)
# ===============================================================

def export_pbi_bases(df):

    # ----- FATO -----
    df.to_excel(PBI_FATO, index=False)
    print(f"🔷 BASE_PBI_FATO_LEADS gerada: {PBI_FATO.resolve()}")

    # ----- DIM_CIDADE (100% dinâmica e segura) -----
    possible_cols = [
        "Cidade", "UF", "ESTADO", "POPULAÇÃO ESTIMADA",
        "Mesorregião", "BLOCO POPULACIONAL",
        "TEM FRANQUIA", "TEM MULTIMARCAS?", "QQTD"
    ]

    available_cols = [c for c in possible_cols if c in df.columns]

    if available_cols:
        dim_cidade = df[available_cols].drop_duplicates()
    else:
        dim_cidade = pd.DataFrame({"Aviso": ["Nenhuma coluna válida encontrada para DIM_CIDADE"]})

    dim_cidade.to_excel(PBI_DIM_CIDADE, index=False)
    print(f"🔷 BASE_PBI_DIM_CIDADE gerada: {PBI_DIM_CIDADE.resolve()}")

    # ----- DIM_SDR -----
    dim_sdr = df[["NomeSDR"]].drop_duplicates()
    dim_sdr.to_excel(PBI_DIM_SDR, index=False)
    print(f"🔷 BASE_PBI_DIM_SDR gerada: {PBI_DIM_SDR.resolve()}")

    # ----- DIM_STATUS -----
    dim_status = df[["Status do Lead"]].drop_duplicates()
    dim_status.to_excel(PBI_DIM_STATUS, index=False)
    print(f"🔷 BASE_PBI_DIM_STATUS gerada: {PBI_DIM_STATUS.resolve()}")


# ===============================================================
# 🏁 MAIN
# ===============================================================

def main():
    print("===================================================")
    print("🚀 INICIANDO ETL CRM MULTIMARCAS – FINAL CORRIGIDO")
    print("===================================================")

    df_raw = load_and_concatenate_sdr_sheets(INPUT_FILE, SDR_SHEETS)
    df_leads = filter_real_leads(df_raw)
    df_clean = basic_cleaning(df_leads)

    ibge = load_ibge_base(INPUT_FILE)
    df_enriched = enrich_with_ibge(df_clean, ibge)

    df_prior = compute_priority(df_enriched)

    df_export = export_clean(df_prior)

    generate_reports(df_prior, ibge, RELATORIO_TXT, RELATORIO_XLSX)

    export_pbi_bases(df_export)

    print("\n🎉 ETL FINAL executado com sucesso!")
    print("CRM ENXUTO + INTELIGÊNCIA PRONTA PARA POWER BI.")


if __name__ == "__main__":
    main()

