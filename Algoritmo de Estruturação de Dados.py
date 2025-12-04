import os
import re
import warnings
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# =============================================================================
# CONFIGURAÇÕES BÁSICAS
# =============================================================================

# 1) Arquivo de origem (planilha ou CSV)
ARQUIVO = r"C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\Scripts Python\BASE DE DADOS.xlsx"  # <<< AJUSTAR AQUI
ABA = "FATURADO"  # se for Excel e quiser uma aba específica da FATO, coloque o nome; se None, lê a primeira aba

# 2) Configuração do MySQL
DB_USER = "root"       # <<< AJUSTAR
DB_PASSWORD = "root"     # <<< AJUSTAR
DB_HOST = "localhost"         # ajuste se necessário
DB_PORT = 3306                # ajuste se necessário
DB_NAME = "dvwarehouse"       # conforme combinado

# 3) Nome da coluna de apelido do vendedor na planilha
APELIDO_VENDEDOR_COL = "Apelido (Vendedor)"  # <<< AJUSTAR SE O NOME FOR DIFERENTE

# 4) Configuração da aba e colunas da DIM CADASTRO
ABA_CADASTRO = "CADASTRO"
COLUNAS_CADASTRO = [
    "Cód. Parceiro",
    "Nome Parceiro",
    "Status",
    "Descrição (Motivo Status Multimarcas)",
    "Apelido (Vendedor)",
    "Nome (Cidade)",
    "Data Cadastramento",
]


# =============================================================================
# FUNÇÕES UTILITÁRIAS
# =============================================================================

def ler_arquivo(caminho, aba=None):
    """
    Lê o arquivo inteiro (Excel ou CSV) em um DataFrame pandas.
    """
    ext = os.path.splitext(caminho)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(caminho, sheet_name=aba)
    elif ext in [".csv", ".txt"]:
        # ajuste o separador se necessário
        df = pd.read_csv(caminho, sep=";", engine="python")
    else:
        raise ValueError(f"Extensão de arquivo não suportada: {ext}")
    return df


def cardinalidade(s: pd.Series) -> int:
    return s.dropna().nunique()


def proporcao_unicos(s: pd.Series) -> float:
    n = len(s)
    if n == 0:
        return 0.0
    return cardinalidade(s) / n


def proporcao_nulos(s: pd.Series) -> float:
    n = len(s)
    if n == 0:
        return 0.0
    return s.isna().sum() / n


def is_date_series(s: pd.Series) -> bool:
    """
    Detecta se a coluna é de data analisando toda a série.
    Usa pd.to_datetime com warnings suprimidos para o caso
    de 'Could not infer format...'.
    """
    # Se já veio como datetime, beleza
    if np.issubdtype(s.dtype, np.datetime64):
        return True

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Could not infer format, so each element will be parsed individually, falling back to `dateutil`."
        )
        convertida = pd.to_datetime(s, errors="coerce", dayfirst=True)

    total = len(s)
    if total == 0:
        return False

    proporcao_validos = convertida.notna().sum() / total
    return proporcao_validos >= 0.7  # 70%+ dos valores parecem datas


def nome_parece_id(nome_coluna: str) -> bool:
    """
    Heurística para identificar nome de coluna de ID/chave.
    """
    nome = nome_coluna.lower()
    padroes = ["id_", "_id", "codigo", "código", "chave", "nr_", "num_", "seq_", "pk_", "fk_"]
    return any(p in nome for p in padroes)


def eh_candidato_chave(s: pd.Series, nome_coluna: str, limiar_unicidade=0.98) -> bool:
    """
    Coluna candidata a chave:
    - Alta unicidade (>= limiar)
    - Poucos nulos
    - Normalmente numérica inteira ou texto
    - E/ou nome com cara de ID
    """
    if s.isna().all():
        return False

    unicos = proporcao_unicos(s)
    nulos = proporcao_nulos(s)
    nome_id = nome_parece_id(nome_coluna)

    # se o nome já parece ID, pode aceitar um pouco menos de unicidade
    limiar = limiar_unicidade if not nome_id else 0.90

    if unicos >= limiar and nulos <= 0.05:
        # evitar tratar valores monetários como chave: floats com muitas casas
        if np.issubdtype(s.dtype, np.floating):
            amostra = s.dropna().head(50)
            tem_decimais = any(val % 1 != 0 for val in amostra)
            if tem_decimais:
                return False
        return True

    return False


def eh_medida(s: pd.Series, nome_coluna: str) -> bool:
    """
    Medida típica de tabela FATO:
    - Numérico
    - Não é candidato a chave
    - Alta cardinalidade (muitos valores diferentes)
    - Nome NÃO parece ID
    """
    if not np.issubdtype(s.dtype, np.number):
        return False

    if nome_parece_id(nome_coluna):
        return False

    card = cardinalidade(s)
    if card < 15:
        return False

    # se quase tudo é único, pode ser chave, não métrica
    if proporcao_unicos(s) > 0.98:
        return False

    return True


def eh_atributo_dimensao(s: pd.Series, nome_coluna: str) -> bool:
    """
    Atributo típico de DIMENSÃO:
    - Texto ou categórico
    - Cardinalidade relativamente baixa/média
    - Não é data
    - Não é métrica numérica
    """
    if is_date_series(s):
        return False

    if np.issubdtype(s.dtype, np.number):
        # numérico tende a ser métrica, a menos que cardinalidade seja muito baixa
        if cardinalidade(s) <= 20 and not eh_medida(s, nome_coluna):
            return True
        return False

    # texto/objeto
    card = cardinalidade(s)
    n = len(s)
    if n == 0:
        return False

    # heurística: se menos de 20% das linhas são valores distintos, tende a ser categórica
    if card / n <= 0.2:
        return True

    # nomes que claramente parecem descrição/atributo
    desc_padroes = ["nome", "descricao", "descrição", "cidade", "estado", "uf", "bairro", "logradouro"]
    if any(p in nome_coluna.lower() for p in desc_padroes):
        return True

    return False


def normalizar_nome_dim(base: str) -> str:
    base = base.strip().lower()
    base = re.sub(r"[^a-z0-9_]+", "_", base)
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = "entidade"
    return f"dim_{base}"


def sugerir_primary_key(analise_df: pd.DataFrame):
    """
    Escolhe a melhor coluna para ser PRIMARY KEY, se houver.
    Critérios:
      1. é candidata a chave
      2. nome parece ID
      3. maior proporção de únicos
      4. menor proporção de nulos
    """
    cand = analise_df[analise_df["eh_candidato_chave"]].copy()
    if cand.empty:
        return None

    cand["score_nome_id"] = cand["nome_parece_id"].astype(int)
    cand = cand.sort_values(
        by=["score_nome_id", "proporcao_unicos", "proporcao_nulos"],
        ascending=[False, False, True]
    )
    return cand.iloc[0]["coluna"]


def extrair_tema_da_chave(nome_coluna: str) -> str:
    """
    Tenta extrair o 'tema' da chave, ex:
    id_cliente -> cliente
    cod_produto -> produto
    """
    tema = re.sub(r"id_|_id|codigo|código|chave|nr_|num_|pk_|fk_", "", nome_coluna, flags=re.IGNORECASE)
    tema = tema.strip("_").strip().lower()
    return tema


def criar_engine_mysql():
    """
    Cria engine SQLAlchemy para o MySQL usando PyMySQL.
    Certifique-se de instalar o driver:
    pip install pymysql
    """
    url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    return engine


# =============================================================================
# DIM_CALENDARIO - GERAÇÃO
# =============================================================================

def calcular_semana_mes(data):
    """
    Calcula 'Semana X' dentro do mês, no estilo:
    - dias antes da primeira segunda-feira do mês -> Semana 0
    - depois disso, semanas numeradas a partir de 1
    """
    from datetime import timedelta

    primeiro_dia_mes = data.replace(day=1)
    # 0=segunda, 6=domingo
    offset = (7 - primeiro_dia_mes.weekday()) % 7
    primeira_segunda = primeiro_dia_mes + timedelta(days=offset)

    if data < primeira_segunda:
        semana = 0
    else:
        delta_dias = (data - primeira_segunda).days
        semana = 1 + (delta_dias // 7)
    return f"Semana {semana}"


def criar_dim_calendario(df_base: pd.DataFrame):
    """
    Cria uma DIM_CALENDARIO a partir do menor e maior período de datas
    encontrado nas colunas de data do df_base.
    Agora garante datas até 31/12/2030.
    """
    from datetime import date as date_cls

    # Encontrar colunas de data no df_base
    datas_min = []
    datas_max = []

    for col in df_base.columns:
        s = df_base[col]
        if is_date_series(s):
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                conv = pd.to_datetime(s, errors="coerce", dayfirst=True)
            if conv.notna().any():
                datas_min.append(conv.min())
                datas_max.append(conv.max())

    if not datas_min:
        # Se não encontrar datas, cria um calendário padrão
        inicio = pd.to_datetime("2020-01-01")
        fim = pd.to_datetime("2030-12-31")
    else:
        inicio = min(datas_min)
        fim = max(datas_max)

        # Garante que o calendário vá pelo menos até 31/12/2030
        limite_superior = pd.to_datetime("2030-12-31")
        if fim < limite_superior:
            fim = limite_superior

    rng = pd.date_range(inicio, fim, freq="D")

    # mapas de nomes PT-BR
    nomes_dia = {
        0: "SEGUNDA-FEIRA",
        1: "TERÇA-FEIRA",
        2: "QUARTA-FEIRA",
        3: "QUINTA-FEIRA",
        4: "SEXTA-FEIRA",
        5: "SÁBADO",
        6: "DOMINGO",
    }
    nomes_mes = {
        1: "JANEIRO",
        2: "FEVEREIRO",
        3: "MARÇO",
        4: "ABRIL",
        5: "MAIO",
        6: "JUNHO",
        7: "JULHO",
        8: "AGOSTO",
        9: "SETEMBRO",
        10: "OUTUBRO",
        11: "NOVEMBRO",
        12: "DEZEMBRO",
    }
    nomes_mes_abrev = {
        1: "JAN",
        2: "FEV",
        3: "MAR",
        4: "ABR",
        5: "MAI",
        6: "JUN",
        7: "JUL",
        8: "AGO",
        9: "SET",
        10: "OUT",
        11: "NOV",
        12: "DEZ",
    }

    df = pd.DataFrame({"DATA": rng})

    df["ANO"] = df["DATA"].dt.year
    df["MES_NUM"] = df["DATA"].dt.month
    df["DIA_ANO"] = df["DATA"].dt.dayofyear
    df["DOW"] = df["DATA"].dt.weekday  # 0=segunda

    df["DATA_DIA"] = df["DOW"].map(nomes_dia)
    df["DATA_MES"] = df["MES_NUM"].map(nomes_mes)
    df["MÊS_ABREV"] = df["MES_NUM"].map(nomes_mes_abrev)
    df["MÊS/ANO"] = df["MÊS_ABREV"] + "/" + df["ANO"].astype(str)
    df["DATA_ ANO"] = df["ANO"]  # mantendo o nome com espaço igual à planilha
    df["SEMANA"] = df["DATA"].apply(calcular_semana_mes)

    trimestre = ((df["MES_NUM"] - 1) // 3) + 1
    df["TRI"] = trimestre.astype(str) + " Trimestre " + df["ANO"].astype(str)

    # É FDS?
    df["É FDS?"] = np.where(df["DOW"] >= 5, "SIM", "NÃO")

    # Dia útil no ano (ignorando feriados por enquanto)
    df["DIAÚTIL_ANO"] = 0
    for ano, grupo in df.groupby("ANO"):
        grupo = grupo.sort_values("DATA")
        contador = 0
        indices = []
        valores = []
        for idx, row in grupo.iterrows():
            if row["DOW"] < 5:  # segunda a sexta
                contador += 1
            indices.append(idx)
            valores.append(contador)
        df.loc[indices, "DIAÚTIL_ANO"] = valores

    df["DIANORMAL_ANO"] = df["DIA_ANO"]

    # Marcação de feriado - por padrão, tudo como "DIA NORMAL" (pode ser refinado depois)
    df["É FERIADO?"] = "DIA NORMAL"
    df["SE FERIADO, QUAL É?"] = ""

    # DATA_EVENTO: por padrão, igual à DATA
    df["DATA_EVENTO"] = df["DATA"]

    # É DATAHOJE?
    hoje = pd.to_datetime(date_cls.today())
    df["É DATAHOJE?"] = np.where(df["DATA"].dt.date == hoje.date(), "SIM", "NÃO")

    # Quantidade de dias úteis no mês (QTD D.U)
    df["QTD D.U"] = 0
    for (ano, mes), grupo in df.groupby(["ANO", "MES_NUM"]):
        qtd_du = ((grupo["DOW"] < 5)).sum()
        df.loc[grupo.index, "QTD D.U"] = qtd_du

    # MÊS (num) e MÊS/ANO.1 de referência
    df["MÊS"] = df["DATA_MES"]
    df["MÊS/ANO.1"] = df["MÊS/ANO"]

    # Colunas de feriados extras - vazias por enquanto
    df["ANO_FERIADO"] = np.nan
    df["DATA_FERIADO"] = pd.NaT
    df["NOME_FERIADO"] = ""
    df["TIPO"] = ""

    colunas = [
        "DATA",
        "DATA_DIA",
        "DATA_MES",
        "MÊS_ABREV",
        "MÊS/ANO",
        "DATA_ ANO",
        "SEMANA",
        "TRI",
        "DIAÚTIL_ANO",
        "DIANORMAL_ANO",
        "É FDS?",
        "É FERIADO?",
        "SE FERIADO, QUAL É?",
        "DATA_EVENTO",
        "É DATAHOJE?",
        "ANO_FERIADO",
        "DATA_FERIADO",
        "NOME_FERIADO",
        "TIPO",
        "MÊS",
        "ANO",
        "MÊS/ANO.1",
        "QTD D.U",
    ]

    df = df[colunas]

    return df


def exportar_dim_calendario(dim_cal_df: pd.DataFrame, nome_tabela: str = "dim_calendario"):
    """
    Exporta a DIM_CALENDARIO para o MySQL.
    """
    print(f"\nExportando {nome_tabela} para o MySQL (dvwarehouse)...")
    try:
        engine = criar_engine_mysql()
    except Exception as e:
        print("Erro ao criar conexão com MySQL para dim_calendario:")
        print(e)
        return

    try:
        dim_cal_df.to_sql(nome_tabela, con=engine, if_exists="replace", index=False)
        print(f"Tabela {nome_tabela} criada com {len(dim_cal_df)} linhas.")
    except Exception as e:
        print(f"Erro ao criar tabela {nome_tabela}:")
        print(e)
    finally:
        try:
            engine.dispose()
            print("Conexão com MySQL encerrada (engine.dispose()) para dim_calendario.")
        except Exception:
            pass


# =============================================================================
# NOVA DIMENSÃO: DIM_CADASTRO
# =============================================================================

def exportar_dim_cadastro(
    caminho_arquivo: str,
    nome_aba: str = ABA_CADASTRO,
    colunas_desejadas=None,
    nome_tabela: str = "dim_cadastro"
):
    """
    Lê a aba CADASTRO do arquivo Excel e exporta apenas as colunas relevantes
    para o MySQL como dim_cadastro.
    """
    if colunas_desejadas is None:
        colunas_desejadas = COLUNAS_CADASTRO

    print(f"\nLendo aba '{nome_aba}' do arquivo para criar {nome_tabela}...")

    try:
        df_cad = pd.read_excel(caminho_arquivo, sheet_name=nome_aba)
    except Exception as e:
        print(f"[AVISO] Não foi possível ler a aba '{nome_aba}' ({e}). Dimensão {nome_tabela} não será criada.")
        return

    # Filtra apenas as colunas desejadas que existem no DataFrame
    colunas_existentes = [c for c in colunas_desejadas if c in df_cad.columns]

    if not colunas_existentes:
        print(f"[AVISO] Nenhuma das colunas desejadas existe na aba '{nome_aba}'. Dimensão {nome_tabela} não será criada.")
        return

    if len(colunas_existentes) < len(colunas_desejadas):
        faltando = set(colunas_desejadas) - set(colunas_existentes)
        print(f"[AVISO] As seguintes colunas não foram encontradas em '{nome_aba}' e serão ignoradas: {', '.join(faltando)}")

    dim_df = df_cad[colunas_existentes].drop_duplicates().reset_index(drop=True)

    print(f"Exportando {nome_tabela} com colunas: {', '.join(colunas_existentes)}")

    try:
        engine = criar_engine_mysql()
    except Exception as e:
        print("Erro ao criar conexão com MySQL para dim_cadastro:")
        print(e)
        return

    try:
        dim_df.to_sql(nome_tabela, con=engine, if_exists="replace", index=False)
        print(f"Tabela {nome_tabela} criada com {len(dim_df)} linhas.")
    except Exception as e:
        print(f"Erro ao criar tabela {nome_tabela}:")
        print(e)
    finally:
        try:
            engine.dispose()
            print("Conexão com MySQL encerrada (engine.dispose()) para dim_cadastro.")
        except Exception:
            pass


# =============================================================================
# EXPORTAÇÃO FATO/DIMENSÕES DO ARQUIVO BASE
# =============================================================================

def exportar_para_mysql(
    df: pd.DataFrame,
    sugestao_dimensoes,
    fato_nome: str,
    pk_escolhida: str | None,
    cols_medidas,
    cols_datas
):
    """
    Cria tabelas dimensão e fato no MySQL e exporta os dados.
    Usa pandas.to_sql (sem constraints avançadas).
    """
    print("\nIniciando exportação para MySQL (dvwarehouse)...")

    try:
        engine = criar_engine_mysql()
    except Exception as e:
        print("Erro ao criar conexão com MySQL:")
        print(e)
        return

    try:
        # -------------------------
        # CRIAÇÃO DE DIMENSÕES (das chaves escolhidas)
        # -------------------------
        for dim in sugestao_dimensoes:
            dim_name = dim["dimensao"]
            nk_col = dim["chave_natural"]

            if nk_col not in df.columns:
                print(f"[AVISO] Coluna de chave natural '{nk_col}' não encontrada no DataFrame. Pulando dimensão {dim_name}.")
                continue

            tema = extrair_tema_da_chave(nk_col)

            atributos = []
            for col in df.columns:
                if col == nk_col:
                    continue
                if eh_atributo_dimensao(df[col], col):
                    if tema and tema in col.lower():
                        atributos.append(col)

            colunas_dim = [nk_col] + atributos
            dim_df = df[colunas_dim].drop_duplicates().reset_index(drop=True)

            try:
                dim_df.to_sql(dim_name, con=engine, if_exists="replace", index=False)
                print(f"Tabela dimensão criada: {dim_name} ({len(dim_df)} linhas). Colunas: {', '.join(colunas_dim)}")
            except Exception as e:
                print(f"Erro ao criar dimensão {dim_name}:")
                print(e)

        # -------------------------
        # CRIAÇÃO DA FATO
        # -------------------------
        fact_cols = []

        if pk_escolhida and pk_escolhida in df.columns:
            fact_cols.append(pk_escolhida)

        for dim in sugestao_dimensoes:
            nk = dim["chave_natural"]
            if nk in df.columns and nk not in fact_cols:
                fact_cols.append(nk)

        for c in cols_datas:
            if c in df.columns and c not in fact_cols:
                fact_cols.append(c)

        for c in cols_medidas:
            if c in df.columns and c not in fact_cols:
                fact_cols.append(c)

        # 🔹 FORÇAR A COLUNA "Apelido (Vendedor)" NA FATO, SE EXISTIR
        if APELIDO_VENDEDOR_COL in df.columns and APELIDO_VENDEDOR_COL not in fact_cols:
            fact_cols.append(APELIDO_VENDEDOR_COL)

        if not fact_cols:
            print("\nNenhuma coluna identificada para a tabela fato. Exportação da fato não será realizada.")
            return

        fato_df = df[fact_cols].copy()

        try:
            fato_df.to_sql(fato_nome, con=engine, if_exists="replace", index=False)
            print(f"Tabela fato criada: {fato_nome} ({len(fato_df)} linhas). Colunas: {', '.join(fact_cols)}")
        except Exception as e:
            print(f"Erro ao criar tabela fato {fato_nome}:")
            print(e)

        print("\nExportação de fato/dimensões concluída.")
    finally:
        try:
            engine.dispose()
            print("Conexão com MySQL encerrada (engine.dispose()).")
        except Exception:
            pass


# =============================================================================
# ANÁLISE PRINCIPAL
# =============================================================================

def analisar_planilha_para_dw(df: pd.DataFrame, nome_tabela_base: str = "fato_generico"):
    n_linhas = len(df)
    n_colunas = len(df.columns)

    print("=" * 80)
    print("ANÁLISE DE PLANILHA PARA DATA WAREHOUSE (FATO/DIMENSÃO)")
    print("=" * 80)
    print(f"Tabela base (origem): {nome_tabela_base}")
    print(f"Qtd. linhas : {n_linhas}")
    print(f"Qtd. colunas: {n_colunas}")
    print()

    # ---- Análise coluna a coluna ----
    analise = []
    for col in df.columns:
        s = df[col]
        info = {
            "coluna": col,
            "dtype": str(s.dtype),
            "cardinalidade": cardinalidade(s),
            "proporcao_unicos": round(proporcao_unicos(s), 3),
            "proporcao_nulos": round(proporcao_nulos(s), 3),
            "eh_data": is_date_series(s),
            "nome_parece_id": nome_parece_id(col),
        }
        info["eh_candidato_chave"] = eh_candidato_chave(s, col)
        info["eh_medida"] = eh_medida(s, col)
        info["eh_atributo_dimensao"] = eh_atributo_dimensao(s, col)
        analise.append(info)

    analise_df = pd.DataFrame(analise)

    # ---- Sugestão de PRIMARY KEY ----
    # Preferência explícita pela coluna "Parceiro", se existir
    parceiro_col = None
    for c in df.columns:
        if c.strip().lower() == "parceiro":
            parceiro_col = c
            break

    if parceiro_col is not None:
        pk_sugerida = parceiro_col
    else:
        pk_sugerida = sugerir_primary_key(analise_df)

    pk_escolhida = pk_sugerida  # por padrão, é a sugerida

    # --------------------------------
    # INTERAÇÃO COM O USUÁRIO SOBRE A PRIMARY KEY
    # --------------------------------
    print("===== SUGESTÃO INICIAL DE PRIMARY KEY =====")
    if pk_sugerida:
        print(f"PRIMARY KEY sugerida pelo analisador: {pk_sugerida}")
    else:
        print("Nenhuma PRIMARY KEY foi sugerida automaticamente.")

    print("\nVocê concorda com essa sugestão?")
    print("  S = aceitar a sugestão")
    print("  N = escolher outra coluna manualmente")
    print("  Enter vazio = não usar PRIMARY KEY")

    resp = input("Sua escolha (S/N/Enter): ").strip().upper()

    if resp == "N":
        print("\nColunas disponíveis na tabela:")
        for idx, col in enumerate(df.columns, start=1):
            print(f"{idx:2d}. {col}")
        escolha = input("\nDigite o NOME exato da coluna que você deseja usar como PRIMARY KEY (ou deixe em branco para nenhuma): ").strip()
        if escolha:
            if escolha in df.columns:
                pk_escolhida = escolha
                print(f"PRIMARY KEY ajustada pelo usuário: {pk_escolhida}")
            else:
                print("Coluna não encontrada. Mantendo sugestão original.")
        else:
            pk_escolhida = None
            print("Nenhuma PRIMARY KEY será considerada.")
    elif resp == "":
        pk_escolhida = None
        print("Nenhuma PRIMARY KEY será considerada.")
    else:
        if pk_escolhida:
            print(f"PRIMARY KEY mantida como: {pk_escolhida}")
        else:
            print("Sem PRIMARY KEY definida.")

    # ---- Agrupar visão geral ----
    cols_chave = analise_df[analise_df["eh_candidato_chave"]]["coluna"].tolist()
    cols_medidas = analise_df[analise_df["eh_medida"]]["coluna"].tolist()
    cols_datas = analise_df[analise_df["eh_data"]]["coluna"].tolist()
    cols_dim = analise_df[analise_df["eh_atributo_dimensao"]]["coluna"].tolist()

    # --------------------------------
    # CURADORIA DAS DIMENSÕES (ESCOLHA PELO USUÁRIO)
    # --------------------------------
    print("\n===== COLUNAS CANDIDATAS A CHAVE (POTENCIAIS DIMENSÕES) =====")
    if cols_chave:
        print(analise_df[analise_df["eh_candidato_chave"]][
            ["coluna", "dtype", "cardinalidade", "proporcao_unicos", "proporcao_nulos", "nome_parece_id"]
        ].to_string(index=False))
    else:
        print("Nenhuma coluna forte candidata a chave encontrada.")

    print("\nCuradoria de dimensões:")
    print("- Essas colunas acima são candidatas a virar DIMENSÃO (ex: nro_nota, id_cliente, id_produto...).")
    print("- Digite os NOMES das colunas que você quer usar como DIMENSÕES, separados por vírgula.")
    print("- Se quiser usar apenas a PRIMARY KEY (ou nenhuma, se não houver PK), deixe em branco e pressione Enter.")
    entrada_dims = input("\nColunas escolhidas para DIMENSÃO (separadas por vírgula): ").strip()

    dim_key_cols = []

    if entrada_dims == "":
        if pk_escolhida:
            dim_key_cols = [pk_escolhida]
            print(f"\nNenhuma coluna informada. Usando apenas a PRIMARY KEY '{pk_escolhida}' como dimensão.")
        else:
            dim_key_cols = []
            print("\nNenhuma coluna informada e nenhuma PRIMARY KEY definida. Nenhuma dimensão será criada.")
    else:
        nomes_informados = [c.strip() for c in entrada_dims.split(",") if c.strip()]
        colunas_validas = []
        for nome in nomes_informados:
            if nome in df.columns:
                colunas_validas.append(nome)
            else:
                print(f"[AVISO] Coluna '{nome}' não encontrada no DataFrame. Ignorando.")
        dim_key_cols = colunas_validas
        print(f"\nDimensões selecionadas pelo usuário (chaves naturais): {', '.join(dim_key_cols) if dim_key_cols else 'nenhuma'}")

    # ---- Sugerir dimensões baseadas nas chaves escolhidas ----
    sugestao_dimensoes = []
    for col in dim_key_cols:
        tema = extrair_tema_da_chave(col)
        nome_dim = normalizar_nome_dim(tema if tema else col)
        sugestao_dimensoes.append(
            {
                "dimensao": nome_dim,
                "chave_natural": col,
            }
        )

    fato_nome = f"fato_{normalizar_nome_dim(nome_tabela_base).replace('dim_', '')}"

    # --------------------------------
    # PAINEL – RESUMO
    # --------------------------------
    print("\n===== VISÃO GERAL =====")
    print(f"Total de colunas candidatas a chave........: {len(cols_chave)}")
    print(f"Total de colunas de data...................: {len(cols_datas)}")
    print(f"Total de colunas de medida (fato)..........: {len(cols_medidas)}")
    print(f"Total de colunas de atributo (dimensão)....: {len(cols_dim)}")
    print()

    print("PRIMARY KEY definida para a tabela base (após interação):")
    if pk_escolhida:
        print(f"  -> {pk_escolhida}")
    else:
        print("  -> Nenhuma PRIMARY KEY foi definida.")
    print()

    print("===== COLUNAS DE DATA =====")
    print(", ".join(cols_datas) if cols_datas else "Nenhuma coluna de data identificada com confiança.")
    print()

    print("===== COLUNAS DE MEDIDA (POTENCIAIS FATO) =====")
    print(", ".join(cols_medidas) if cols_medidas else "Nenhuma coluna claramente identificada como métrica numérica.")
    print()

    print("===== COLUNAS DE ATRIBUTO (POTENCIAIS DIMENSÕES) =====")
    print(", ".join(cols_dim) if cols_dim else "Poucas colunas categóricas/atributos de dimensão foram detectadas.")
    print()

    print("===== SUGESTÃO DE TABELA FATO =====")
    print(f"Nome sugerido: {fato_nome}")
    print("Sugestão de colunas (lógica, NÃO é SQL):")
    print("  - PRIMARY KEY (após interação):", pk_escolhida if pk_escolhida else "nenhuma")
    print("  - Colunas de medida   :", ", ".join(cols_medidas) if cols_medidas else "nenhuma")
    print("  - Colunas de data     :", ", ".join(cols_datas) if cols_datas else "nenhuma")
    print("  - FKs para dimensões  : chaves naturais selecionadas:", ", ".join(dim_key_cols) if dim_key_cols else "nenhuma")
    print()

    print("===== SUGESTÃO DE TABELAS DIMENSÃO (APÓS CURADORIA) =====")
    if sugestao_dimensoes:
        for dim in sugestao_dimensoes:
            print(f"  - {dim['dimensao']} (chave natural: {dim['chave_natural']})")
    else:
        print("Nenhuma dimensão será criada (nenhuma chave selecionada).")
    print()

    print("===== DETALHE COMPLETO DAS COLUNAS =====")
    print(analise_df.sort_values(by="cardinalidade", ascending=False).to_string(index=False))
    print()
    print("Fim da análise lógica. Agora você pode decidir se quer exportar as tabelas fato/dimensão para o MySQL.")
    print("=" * 80)

    # --------------------------------
    # INTERAÇÃO SOBRE EXPORTAÇÃO PARA MYSQL
    # --------------------------------
    resp_mysql = input("\nDeseja exportar as tabelas fato/dimensão sugeridas para o MySQL (dvwarehouse)? (S/N): ").strip().upper()
    if resp_mysql == "S":
        exportar_para_mysql(
            df=df,
            sugestao_dimensoes=sugestao_dimensoes,
            fato_nome=fato_nome,
            pk_escolhida=pk_escolhida,
            cols_medidas=cols_medidas,
            cols_datas=cols_datas
        )

        # Cria e exporta DIM_CALENDARIO
        dim_cal = criar_dim_calendario(df)
        exportar_dim_calendario(dim_cal, nome_tabela="dim_calendario")

        # Cria e exporta DIM_CADASTRO a partir da aba CADASTRO
        exportar_dim_cadastro(ARQUIVO, nome_aba=ABA_CADASTRO, colunas_desejadas=COLUNAS_CADASTRO, nome_tabela="dim_cadastro")

    else:
        print("Exportação para MySQL não realizada. Encerrando apenas com a análise lógica.")


# =============================================================================
# EXECUÇÃO
# =============================================================================

if __name__ == "__main__":
    df_base = ler_arquivo(ARQUIVO, ABA)
    nome_tabela = os.path.splitext(os.path.basename(ARQUIVO))[0]
    analisar_planilha_para_dw(df_base, nome_tabela_base=nome_tabela)
