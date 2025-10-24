import os
import re
import time
import io
import pandas as pd
import streamlit as st
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# ======= Config =======
AF_TAG = os.getenv("AF_TAG", "")  # sua tag de afiliado da Amazon, ex: "seu-id-20"
HEADLESS = True

# ======= Utilidades de preço =======
def limpar_preco(valor: Optional[str]) -> Optional[str]:
    if not valor:
        return None
    preco = valor.strip()
    lixo = ["R$", "r$", "por", "à vista", "PIX", "pix", "\n", "\t"]
    for l in lixo:
        preco = preco.replace(l, "")
    preco = re.sub(r"\s+", "", preco)
    preco = re.sub(r"[^0-9\.,]", "", preco)
    preco = preco.replace(",,", ",")
    if "," not in preco:
        preco += ",00"
    return f"R$ {preco}"

def preco_para_num(valor: Optional[str]) -> Optional[float]:
    if not valor:
        return None
    try:
        return float(valor.replace("R$", "").replace(".", "").replace(",", ".").strip())
    except:
        return None

# ======= Catálogos =======
OPCOES_PC: Dict[str, Dict] = {
    "PC Fraco": {"Processador": "Ryzen 3 4100", "Placa de Vídeo": "GTX 1050 Ti", "Placa Mãe": "A320M AM4",
                 "Memória RAM": "8GB DDR4 2666", "Armazenamento": "SSD 240GB SATA", "Fonte": "Fonte 450W 80 Plus",
                 "Gabinete": "Gabinete Micro ATX"},
    "PC Médio": {"Processador": "Ryzen 5 5600G", "Placa de Vídeo": "GTX 1660 Super", "Placa Mãe": "B450M AM4",
                 "Memória RAM": "16GB DDR4 3200", "Armazenamento": "SSD NVMe 500GB", "Fonte": "Fonte 550W 80 Plus Bronze",
                 "Gabinete": "Gabinete Mid Tower"},
    "PC Forte": {"Processador": "Ryzen 5 5600", "Placa de Vídeo": "RTX 3060", "Placa Mãe": "B550M AM4",
                 "Memória RAM": "16GB DDR4 3200", "Armazenamento": "SSD NVMe 1TB", "Fonte": "Fonte 650W 80 Plus Bronze",
                 "Gabinete": "Gabinete Mid Tower Vidro"},
    "PC Multitarefas": {"Processador": "Ryzen 7 5800X", "Placa de Vídeo": "RTX 3060 Ti", "Placa Mãe": "B550 ATX AM4",
                 "Memória RAM": "32GB DDR4 3200", "Armazenamento": "SSD NVMe 1TB", "Fonte": "Fonte 750W 80 Plus Gold",
                 "Gabinete": "Gabinete ATX Vidro"},
    "PC Multitarefas + Jogos": {"Processador": "Ryzen 7 7800X3D", "Placa de Vídeo": "RTX 4070", "Placa Mãe": "B650 AM5",
                 "Memória RAM": "32GB DDR5 6000", "Armazenamento": "SSD NVMe 2TB", "Fonte": "Fonte 850W 80 Plus Gold",
                 "Gabinete": "Gabinete ATX Vidro"},
    "PC Entusiasta": {"Processador": "Ryzen 9 7950X3D", "Placa de Vídeo": "RTX 4090", "Placa Mãe": "X670E ATX AM5",
                 "Memória RAM": "64GB DDR5 6000", "Armazenamento": "SSD NVMe 4TB Gen4", "Fonte": "Fonte 1000W 80 Plus Platinum",
                 "Gabinete": "Gabinete ATX Premium"},
}

REFINOS_TIPO = {
    "Processador": ["processador", "amd", "intel", "am4", "am5"],
    "Placa de Vídeo": ["rtx", "gtx", "radeon", "gpu", "placa de vídeo"],
    "Placa Mãe": ["placa mãe", "motherboard", "am4", "am5", "b550", "b650", "x670", "a320", "b450"],
    "Memória RAM": ["memória", "ram", "ddr4", "ddr5"],
    "Armazenamento": ["ssd", "nvme", "m.2", "sata"],
    "Fonte": ["fonte", "psu", "80 plus"],
    "Gabinete": ["gabinete", "mid tower", "atx", "micro atx"],
}

SINONIMOS = {
    "SSD NVMe 1TB": "SSD NVMe 1TB M.2",
    "SSD NVMe 2TB": "SSD NVMe 2TB M.2",
    "SSD NVMe 4TB Gen4": "SSD NVMe 4TB M.2 Gen4",
    "B550M AM4": "Placa Mãe B550M AM4",
    "B450M AM4": "Placa Mãe B450M AM4",
    "B650 AM5": "Placa Mãe B650 AM5",
    "X670E ATX AM5": "Placa Mãe X670E AM5"
}

# ======= Selenium (opcional no app) =======
def criar_driver(headless=True):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager

    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1400,10000")
    chrome_options.add_argument("--lang=pt-BR")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--ignore-ssl-errors")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(25)
    return driver

def scroll_lento(driver, passos=6, pausa=0.4):
    for i in range(passos):
        driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight*{(i+1)/(passos)});")
        time.sleep(pausa)

def build_amazon_query(tipo: str, termo: str) -> str:
    termo_base = SINONIMOS.get(termo, termo)
    refinadores = " ".join(REFINOS_TIPO.get(tipo, []))
    return f"{termo_base} {refinadores}"

@dataclass
class Resultado:
    tipo: str
    loja: str
    produto: str
    preco: str
    link: str

def amazon_buscar(driver, tipo: str, termo: str, limit: int = 12) -> List[Resultado]:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException

    query = build_amazon_query(tipo, termo)
    url = f"https://www.amazon.com.br/s?k={query.replace(' ', '+')}" + (f"&tag={AF_TAG}" if AF_TAG else "")
    out: List[Resultado] = []

    driver.get(url)
    try:
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.s-main-slot")))
    except TimeoutException:
        return out

    scroll_lento(driver, passos=7, pausa=0.4)
    cards = driver.find_elements(By.CSS_SELECTOR, "div.s-card-container")[:60]

    for item in cards:
        try:
            nome = item.find_element(By.CSS_SELECTOR, "h2").text.strip()
        except Exception:
            continue

        preco = None
        for sel in ["span.a-price-whole", "span.a-offscreen", "span.a-price-range"]:
            try:
                cand = item.find_element(By.CSS_SELECTOR, sel).text.strip()
                if cand:
                    preco = cand
                    break
            except Exception:
                pass
        if not preco:
            continue

        try:
            link = item.find_element(By.CSS_SELECTOR, "h2 a").get_attribute("href")
            # força inclusão da tag de afiliado (se houver)
            if AF_TAG and "amazon.com.br" in link and "tag=" not in link:
                joiner = "&" if "?" in link else "?"
                link = f"{link}{joiner}tag={AF_TAG}"
        except Exception:
            link = url

        # filtro simples por tipo
        termos_relevantes = [t for t in REFINOS_TIPO.get(tipo, []) if len(t) >= 3]
        texto = f"{nome} {query}".lower()
        score = sum(1 for t in termos_relevantes if t in texto)
        if tipo == "Processador" and ("ryzen" not in texto and "intel" not in texto):
            continue
        if score == 0 and tipo != "Processador":
            continue

        out.append(Resultado(tipo, "Amazon", nome, limpar_preco(preco), link))
        if len(out) >= limit:
            break

    return out

# ======= Demo data (modo sem Selenium) =======
def demo_buscar(tipo: str, termo: str) -> List[Resultado]:
    exemplos = [
        Resultado(tipo, "Amazon", f"{termo} – Modelo A", "R$ 999,00", f"https://www.amazon.com.br/s?k={termo.replace(' ', '+')}"),
        Resultado(tipo, "Amazon", f"{termo} – Modelo B", "R$ 1.149,00", f"https://www.amazon.com.br/s?k={termo.replace(' ', '+')}"),
        Resultado(tipo, "Amazon", f"{termo} – Modelo C", "R$ 1.199,00", f"https://www.amazon.com.br/s?k={termo.replace(' ', '+')}"),
    ]
    return exemplos

# ===================== UI (Streamlit) =====================
st.set_page_config(page_title="BIA PC Builder", page_icon="💻", layout="wide")

st.title("🧠 BIA PC Builder — MVP Comercial")
st.caption("Monte sua configuração, compare preços e gere links de compra. (v3.0 MVP)")

col1, col2, col3 = st.columns([1,1,1])

with col1:
    perfil = st.selectbox("Perfil de usabilidade", list(OPCOES_PC.keys()), index=2)
with col2:
    modo = st.radio("Modo de coleta", ["Selenium (tempo real)", "Demo (rápido)"], index=0)
with col3:
    limite = st.slider("Máx. resultados por peça", 3, 20, 10)

st.divider()
st.subheader("Peças sugeridas (edite se quiser)")
pecas = OPCOES_PC[perfil].copy()

# formulário para personalização das peças
with st.form("form_pecas"):
    edits = {}
    cols = st.columns(3)
    i = 0
    for tipo, padrao in pecas.items():
        with cols[i % 3]:
            edits[tipo] = st.text_input(tipo, padrao)
        i += 1
    submitted = st.form_submit_button("Aplicar alterações")
if submitted:
    for k, v in edits.items():
        pecas[k] = v

st.divider()
start = st.button("🔎 Buscar preços")

if start:
    data_rows: List[dict] = []
    if modo.startswith("Selenium"):
        # criar driver uma única vez
        try:
            driver = criar_driver(headless=HEADLESS)
        except Exception as e:
            st.error(f"Erro ao iniciar Selenium: {e}")
            driver = None

        if driver:
            progress = st.progress(0)
            total = len(pecas)
            done = 0
            for tipo, termo in pecas.items():
                st.write(f"**Buscando**: {tipo} → _{termo}_")
                try:
                    resultados = amazon_buscar(driver, tipo, termo, limit=limite)
                except Exception as e:
                    st.warning(f"Falha para {tipo}: {e}")
                    resultados = []
                for r in resultados:
                    data_rows.append(r.__dict__)
                done += 1
                progress.progress(int((done/total)*100))
            try:
                driver.quit()
            except:
                pass
    else:
        # modo demo
        for tipo, termo in pecas.items():
            for r in demo_buscar(tipo, termo):
                data_rows.append(r.__dict__)

    if not data_rows:
        st.warning("Nenhum resultado encontrado. Tente termos mais específicos (ex.: incluir marca/modelo).")
    else:
        df = pd.DataFrame(data_rows)
        df["preco_num"] = df["preco"].apply(preco_para_num)
        df = df.sort_values(["tipo", "preco_num"], ascending=[True, True])

        st.subheader("💰 Resultados (ordenado por preço)")
        st.dataframe(df[["tipo", "loja", "produto", "preco", "link"]], use_container_width=True)

        # ranking menor preço por tipo
        st.subheader("🏆 Melhor custo-benefício por peça")
        ranking = df.loc[df.groupby("tipo")["preco_num"].idxmin()].reset_index(drop=True)
        st.table(ranking[["tipo", "loja", "produto", "preco", "link"]])

        # exportar
        buffer = io.BytesIO()
        df.to_excel(buffer, index=False)
        st.download_button("📥 Baixar Excel (comparativo_pc.xlsx)", data=buffer.getvalue(),
                           file_name="comparativo_pc.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.divider()
st.caption("Dica: configure sua tag de afiliado na variável de ambiente AF_TAG para monetizar automaticamente os links.")
