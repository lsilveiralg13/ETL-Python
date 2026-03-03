import pandas as pd
import requests
from typing import Any, List, Optional


def _get_json(url: str, timeout: int = 180) -> Any:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _to_float_ptbr(x) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _fetch_agregado_series(agregado: int, variavel: str, ano: int, localidades: str = "N6[all]") -> pd.DataFrame:
    url = (
        f"https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}"
        f"/periodos/{ano}/variaveis/{variavel}?localidades={localidades}"
    )
    payload = _get_json(url)

    resultados = payload[0].get("resultados", [])
    series = resultados[0].get("series", []) if resultados else []
    if not series:
        raise RuntimeError(f"Agregado {agregado} retornou vazio para ano={ano}.")

    rows = []
    for s in series:
        loc = s.get("localidade", {})
        serie = s.get("serie", {})

        cod = str(loc.get("id")) if loc.get("id") is not None else None
        nome = loc.get("nome")

        val = None
        if isinstance(serie, dict) and str(ano) in serie:
            val = serie.get(str(ano))
        elif isinstance(serie, dict) and len(serie) == 1:
            # fallback (caso venha só uma chave)
            val = next(iter(serie.values()))

        rows.append({
            "Codigo_Municipio": cod,
            "Nome_Municipio": nome,
            "Ano": str(ano),
            "Valor": val
        })

    return pd.DataFrame(rows)


def _metadados_variaveis(agregado: int) -> List[dict]:
    meta_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/metadados"
    meta = _get_json(meta_url)
    return meta.get("variaveis", [])


def _find_variable_id_by_name(agregado: int, must_contain: List[str]) -> str:
    variaveis = _metadados_variaveis(agregado)
    if not variaveis:
        raise RuntimeError(f"Agregado {agregado} sem variáveis no metadados.")

    must = [t.casefold() for t in must_contain]
    for v in variaveis:
        nome = str(v.get("nome", "")).casefold()
        if all(t in nome for t in must):
            return str(v.get("id"))
    raise RuntimeError(
        f"Não achei variável no agregado {agregado} com {must_contain}. "
        f"Exemplos: {[str(v.get('nome')) for v in variaveis[:25]]}"
    )


def extrair_pib_total_municipios(ano: int = 2021) -> pd.DataFrame:
    agregado_pib = 5938
    # PIB total: normalmente aparece como "Produto Interno Bruto a preços correntes"
    var_pib = _find_variable_id_by_name(agregado_pib, ["produto", "interno", "bruto"])
    df = _fetch_agregado_series(agregado_pib, var_pib, ano)

    df["PIB_Total"] = df["Valor"].apply(_to_float_ptbr)
    df.drop(columns=["Valor"], inplace=True)

    return df[["Codigo_Municipio", "Nome_Municipio", "Ano", "PIB_Total"]]


def extrair_populacao_municipios(ano: int = 2021, agregado_pop: int = 6579) -> pd.DataFrame:
    """
    População municipal (tentativa padrão com agregado_pop=6579).
    Se esse agregado não existir no seu ambiente, eu ajusto pra você com 1 linha
    depois que você me devolver o erro / metadados.
    """
    # Tentamos achar variável contendo "população" no metadados do agregado informado
    var_pop = _find_variable_id_by_name(agregado_pop, ["popula"])
    df = _fetch_agregado_series(agregado_pop, var_pop, ano)

    df["Populacao"] = df["Valor"].apply(_to_float_ptbr)
    df.drop(columns=["Valor"], inplace=True)

    return df[["Codigo_Municipio", "Nome_Municipio", "Ano", "Populacao"]]


def exportar_pib_total_e_populacao_com_uf(ano: int = 2021, output_filename: str = "pib_pop_municipios.xlsx"):
    # UF por município
    mun_api_url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    municipios_raw = _get_json(mun_api_url, timeout=180)

    municipios_uf_data = []
    for m in municipios_raw:
        mun_id = str(m.get("id"))
        mun_nome = m.get("nome")

        microrregiao = m.get("microrregiao")
        uf_sigla = None
        if microrregiao and isinstance(microrregiao, dict):
            mesor = microrregiao.get("mesorregiao")
            if mesor and isinstance(mesor, dict):
                uf = mesor.get("UF")
                if uf and isinstance(uf, dict):
                    uf_sigla = uf.get("sigla")

        municipios_uf_data.append({
            "Codigo_Municipio": mun_id,
            "Nome_Municipio_Original": mun_nome,
            "Estado": uf_sigla
        })

    df_uf = pd.DataFrame(municipios_uf_data)
    df_uf["Codigo_Municipio"] = df_uf["Codigo_Municipio"].astype(str)

    print("Baixando PIB total...")
    df_pib = extrair_pib_total_municipios(ano=ano)

    print("Baixando população...")
    df_pop = extrair_populacao_municipios(ano=ano)  # se falhar, te digo como ajustar

    # Merge
    df = (
        df_uf
        .merge(df_pib[["Codigo_Municipio", "Ano", "PIB_Total"]], on="Codigo_Municipio", how="left")
        .merge(df_pop[["Codigo_Municipio", "Ano", "Populacao"]], on=["Codigo_Municipio", "Ano"], how="left")
    )

    # Se quiser já calcular aqui (opcional):
    df["PIB_per_Capita_Calc"] = df["PIB_Total"] / df["Populacao"]

    # Renomear cidades duplicadas (Nome + UF quando necessário)
    counts = df.groupby(["Nome_Municipio_Original", "Estado"]).size().reset_index(name="count")
    dup_names = counts.groupby("Nome_Municipio_Original")["Estado"].nunique()
    cities_to_rename = dup_names[dup_names > 1].index.tolist()

    df["Nome_Municipio"] = df.apply(
        lambda r: f"{r['Nome_Municipio_Original']} ({r['Estado']})"
        if r["Nome_Municipio_Original"] in cities_to_rename and pd.notna(r["Estado"])
        else r["Nome_Municipio_Original"],
        axis=1
    )
    df.drop(columns=["Nome_Municipio_Original"], inplace=True, errors="ignore")

    # Exportar
    df = df[["Codigo_Municipio", "Nome_Municipio", "Estado", "Ano", "PIB_Total", "Populacao", "PIB_per_Capita_Calc"]]
    df.to_excel(output_filename, index=False)
    print(f"✅ Exportado: {output_filename} | Linhas: {len(df)}")
    return df


if __name__ == "__main__":
    ANO = 2021
    SAIDA = "pib_total_populacao.xlsx"

    try:
        df_final = exportar_pib_total_e_populacao_com_uf(ano=ANO, output_filename=SAIDA)
        print(df_final.head())
    except Exception as e:
        print("❌ Erro:", e)
