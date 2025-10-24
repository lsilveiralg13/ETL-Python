import os
import re
import time
import io
import pandas as pd
import streamlit as st
from dataclasses import dataclass
from typing import Dict, List, Optional

# ================== CONFIG ==================
AF_TAG = os.getenv("AF_TAG", "")  # ex.: "seu-id-20" (Amazon Afiliados). Pode ficar vazio.
HEADLESS = True
RESULTS_PER_TYPE = 12

# ============= UTIL: PREÇOS & LINKS =========
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

def ensure_affiliate(link: str) -> str:
    if not AF_TAG:
        return link
    # remove tag anterior se houver e aplica a nova
    link = re.sub(r"(\?|&)tag=[^&]+", "", link)
    joiner = "&" if "?" in link else "?"
    return f"{link}{joiner}tag={AF_TAG}"

# ================ CATÁLOGOS ==================
OPCOES_PC: Dict[str, Dict] = {
    "PC Fraco": {
        "Processador": "Ryzen 3 4100",
        "Placa de Vídeo": "GTX 1050 Ti",
        "Placa Mãe": "A320M AM4",
        "Memória RAM": "8GB DDR4 2666",
        "Armazenamento": "SSD 240GB SATA",
        "Fonte": "Fonte 450W 80 Plus",
        "Gabinete": "Gabinete Micro ATX",
        "Mouse": "Mouse Gamer 3200 DPI",
        "Teclado": "Teclado Membrana Gamer",
        "Headset": "Headset Gamer Básico",
        "Mouse Pad": "Mouse Pad Gamer Médio"
    },
    "PC Médio": {
        "Processador": "Ryzen 5 5600G",
        "Placa de Vídeo": "GTX 1660 Super",
        "Placa Mãe": "B450M AM4",
        "Memória RAM": "16GB DDR4 3200",
        "Armazenamento": "SSD NVMe 500GB",
        "Fonte": "Fonte 550W 80 Plus Bronze",
        "Gabinete": "Gabinete Mid Tower",
        "Mouse": "Mouse Gamer 7200 DPI",
        "Teclado": "Teclado Mecânico ABNT2",
        "Headset": "Headset Gamer com Microfone",
        "Mouse Pad": "Mouse Pad Gamer Grande"
    },
    "PC Forte": {
        "Processador": "Ryzen 5 5600",
        "Placa de Vídeo": "RTX 3060",
        "Placa Mãe": "B550M AM4",
        "Memória RAM": "16GB DDR4 3200",
        "Armazenamento": "SSD NVMe 1TB",
        "Fonte": "Fonte 650W 80 Plus Bronze",
        "Gabinete": "Gabinete Mid Tower Vidro",
        "Mouse": "Mouse Gamer 16000 DPI",
        "Teclado": "Teclado Mecânico RGB ABNT2",
        "Headset": "Headset Gamer 7.1",
        "Mouse Pad": "Mouse Pad Gamer XXL"
    },
    "PC Multitarefas": {
        "Processador": "Ryzen 7 5800X",
        "Placa de Vídeo": "RTX 3060 Ti",
        "Placa Mãe": "B550 ATX AM4",
        "Memória RAM": "32GB DDR4 3200",
        "Armazenamento": "SSD NVMe 1TB",
        "Fonte": "Fonte 750W 80 Plus Gold",
        "Gabinete": "Gabinete ATX Vidro",
        "Mouse": "Mouse Gamer 26000 DPI",
        "Teclado": "Teclado Mecânico RGB ABNT2",
        "Headset": "Headset Gamer Premium",
        "Mouse Pad": "Mouse Pad Gamer Controle"
    },
    "PC Multitarefas + Jogos": {
        "Processador": "Ryzen 7 7800X3D",
        "Placa de Vídeo": "RTX 4070",
        "Placa Mãe": "B650 AM5",
        "Memória RAM": "32GB DDR5 6000",
        "Armazenamento": "SSD NVMe 2TB",
        "Fonte": "Fonte 850W 80 Plus Gold",
        "Gabinete": "Gabinete ATX Vidro",
        "Mouse": "Mouse Gamer 26000 DPI Wireless",
        "Teclado": "Teclado Mecânico HotSwap ABNT2",
        "Headset": "Headset Gamer Wireless",
        "Mouse Pad": "Mouse Pad Gamer Rígido"
    },
    "PC Entusiasta": {
        "Processador": "Ryzen 9 7950X3D",
        "Placa de Vídeo": "RTX 4090",
        "Placa Mãe": "X670E ATX AM5",
        "Memória RAM": "64GB DDR5 6000",
        "Armazenamento": "SSD NVMe 4TB Gen4",
        "Fonte": "Fonte 1000W 80 Plus Platinum",
        "Gabinete": "Gabinete ATX Premium",
        "Mouse": "Mouse Gamer Ultra Wireless",
        "Teclado": "Teclado Mecânico Premium ABNT2",
        "Headset": "Headset Gamer Hi-Fi",
        "Mouse Pad": "Mouse Pad Gamer Low Friction"
    },
}

REFINOS_TIPO = {
    "Processador": ["processador", "amd", "intel", "am4", "am5"],
    "Placa de Vídeo": ["rtx", "gtx", "radeon", "gpu", "placa de vídeo"],
    "Placa Mãe": ["placa mãe", "motherboard", "am4", "am5", "b550", "b650", "x670", "a320", "b450"],
    # RAM desktop only
    "Memória RAM": ["memória ram desktop", "udimm", "ddr4", "ddr5", "-notebook", "-so-dimm", "-sodimm", "-laptop", "-macbook", "-ultrabook"],
    "Armazenamento": ["ssd", "nvme", "m.2", "sata"],
    "Fonte": ["fonte", "psu", "80 plus"],
    "Gabinete": ["gabinete", "mid tower", "atx", "micro atx"],
    "Mouse": ["mouse gamer", "sensor", "dpi", "usb"],
    "Teclado": ["teclado mecânico", "switch", "gamer", "rgb", "abnt2"],
    "Headset": ["headset gamer", "fone de ouvido", "microfone", "7.1", "surround"],
    "Mouse Pad": ["mouse pad gamer", "grande", "rgb", "speed", "controle"],
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

# ============== SELENIUM CORE ===============
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
    driver.set_page_load_timeout(30)
    return driver

def scroll_lento(driver, passos=6, pausa=0.4):
    for i in range(passos):
        driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight*{(i+1)/(passos)});")
        time.sleep(pausa)

def build_amazon_query(tipo: str, termo: str) -> str:
    termo_base = SINONIMOS.get(termo, termo)
    refinadores = " ".join(REFINOS_TIPO.get(tipo, []))
    return f"{termo_base} {refinadores}"

# --------- EXTRAIR ASIN (apenas se preciso) ----------
def extract_asin_from_current_context(driver) -> Optional[str]:
    """
    Tenta extrair o ASIN da página atual (já carregada no driver)
    Vias: URL atual, link canonical, og:url, data-asin e regex no HTML.
    """
    from selenium.webdriver.common.by import By

    # 1) URL atual
    m = re.search(r"/dp/([A-Z0-9]{10})", driver.current_url)
    if m:
        return m.group(1)

    # 2) link[rel=canonical]
    try:
        canon = driver.find_element(By.CSS_SELECTOR, "link[rel='canonical']").get_attribute("href")
        m = re.search(r"/dp/([A-Z0-9]{10})", canon or "")
        if m:
            return m.group(1)
    except Exception:
        pass

    # 3) meta[property='og:url']
    try:
        og = driver.find_element(By.CSS_SELECTOR, "meta[property='og:url']").get_attribute("content")
        m = re.search(r"/dp/([A-Z0-9]{10})", og or "")
        if m:
            return m.group(1)
    except Exception:
        pass

    # 4) atributos comuns na página
    for sel in ["#dp", "#ppd", "div[data-asin]", "div#centerCol"]:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            html = el.get_attribute("outerHTML") or ""
            m = re.search(r'["\']asin["\']\s*[:=]\s*["\']([A-Z0-9]{10})["\']', html, re.I)
            if m:
                return m.group(1)
            m = re.search(r"data-asin=['\"]([A-Z0-9]{10})['\"]", html, re.I)
            if m:
                return m.group(1)
        except Exception:
            continue

    # 5) regex no page_source
    try:
        src = driver.page_source
        m = re.search(r"/dp/([A-Z0-9]{10})", src)
        if m:
            return m.group(1)
        m = re.search(r'asin["\']\s*[:=]\s*["\']([A-Z0-9]{10})["\']', src, re.I)
        if m:
            return m.group(1)
    except Exception:
        pass

    return None

def resolve_product_link(driver, raw_link: str) -> str:
    """
    Se raw_link já tiver /dp/ASIN => retorna (com AF_TAG)
    Senão, abre em nova aba, extrai ASIN e retorna link /dp/ASIN; se falhar, devolve raw_link.
    """
    # Caso já tenha /dp/ no próprio link
    m = re.search(r"/dp/([A-Z0-9]{10})", raw_link)
    if m:
        final = f"https://www.amazon.com.br/dp/{m.group(1)}"
        return ensure_affiliate(final)

    # Links de busca ou de anúncios/redirect — precisamos abrir
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    original = driver.current_window_handle
    try:
        driver.execute_script("window.open('about:blank', '_blank');")
        driver.switch_to.window(driver.window_handles[-1])
        driver.get(raw_link)

        # espera curta: dom pronto ou mudança de URL
        try:
            WebDriverWait(driver, 10).until(lambda d: re.search(r"/(dp|gp)/", d.current_url) or d.execute_script("return document.readyState") == "complete")
        except Exception:
            pass

        # pequena pausa para dom estável
        time.sleep(0.8)

        asin = extract_asin_from_current_context(driver)
        if asin:
            final = f"https://www.amazon.com.br/dp/{asin}"
            return ensure_affiliate(final)
        else:
            # fallback: usa a URL atual (se útil) ou o raw_link
            if "amazon.com.br" in driver.current_url:
                return ensure_affiliate(driver.current_url)
            return raw_link
    except Exception:
        return raw_link
    finally:
        # fecha a aba e volta
        try:
            driver.close()
            driver.switch_to.window(original)
        except Exception:
            pass

# ================== SCRAPER AMAZON =================
@dataclass
class Resultado:
    tipo: str
    loja: str
    produto: str
    preco: str
    link: str

def amazon_buscar(driver, tipo: str, termo: str, limit: int = RESULTS_PER_TYPE) -> List[Resultado]:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException

    query = build_amazon_query(tipo, termo)
    search_url = f"https://www.amazon.com.br/s?k={query.replace(' ', '+')}" + (f"&tag={AF_TAG}" if AF_TAG else "")
    out: List[Resultado] = []

    driver.get(search_url)
    try:
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.s-main-slot")))
    except TimeoutException:
        return out

    scroll_lento(driver, passos=7, pausa=0.4)
    cards = driver.find_elements(By.CSS_SELECTOR, "div.s-card-container")[:80]

    termos_ram_ban = {"notebook", "so-dimm", "sodimm", "laptop", "macbook", "ultrabook"}

    for item in cards:
        # Nome
        try:
            nome = item.find_element(By.CSS_SELECTOR, "h2").text.strip()
        except Exception:
            continue

        # RAM: evitar notebook/SO-DIMM
        if tipo == "Memória RAM":
            low = nome.lower()
            if any(t in low for t in termos_ram_ban):
                continue

        # Preço (varia pelos seletores)
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

        # Link (pode ser busca/redirect). Resolve para /dp/ASIN se necessário.
        try:
            a = item.find_element(By.CSS_SELECTOR, "h2 a")
            raw_link = a.get_attribute("href")
            link = resolve_product_link(driver, raw_link)
        except Exception:
            link = search_url

        # Relevância básica (mais flexível para periféricos)
        termos_relevantes = [t for t in REFINOS_TIPO.get(tipo, []) if len(t) >= 3 and not t.startswith("-")]
        texto = f"{nome} {query}".lower()
        score = sum(1 for t in termos_relevantes if t in texto)
        if tipo == "Processador" and ("ryzen" not in texto and "intel" not in texto):
            continue
        if tipo not in {"Mouse", "Teclado", "Headset", "Mouse Pad"} and score == 0 and tipo != "Processador":
            continue

        out.append(Resultado(tipo, "Amazon", nome, limpar_preco(preco), link))
        if len(out) >= limit:
            break

        # Respiro curto entre items (educado com o site)
        time.sleep(0.2)

    return out

# =============== DEMO (sem Selenium) ===============
def demo_buscar(tipo: str, termo: str) -> List[Resultado]:
    exemplos = [
        Resultado(tipo, "Amazon", f"{termo} – Modelo A", "R$ 999,00", f"https://www.amazon.com.br/s?k={termo.replace(' ', '+')}"),
        Resultado(tipo, "Amazon", f"{termo} – Modelo B", "R$ 1.149,00", f"https://www.amazon.com.br/s?k={termo.replace(' ', '+')}"),
        Resultado(tipo, "Amazon", f"{termo} – Modelo C", "R$ 1.199,00", f"https://www.amazon.com.br/s?k={termo.replace(' ', '+')}"),
    ]
    return exemplos

# ====================== UI ==========================
st.set_page_config(page_title="BIA PC Builder", page_icon="💻", layout="wide")

st.title("🧠 BIA PC Builder - Sua Montadora Expert em Satisfação")
st.caption("Monte sua configuração, compare preços e gere links diretos de compra com a BIA 💻")

col1, col2, col3 = st.columns([1,1,1])
with col1:
    perfil = st.selectbox("Perfil de usabilidade", list(OPCOES_PC.keys()), index=2)
with col2:
    modo = st.radio("Modo de coleta", ["Selenium (tempo real)", "Demo (rápido)"], index=0)
with col3:
    limite = st.slider("Máx. resultados por peça", 3, 20, RESULTS_PER_TYPE)

st.divider()
st.subheader("Peças & Periféricos sugeridos (edite se quiser)")
pecas = OPCOES_PC[perfil].copy()

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
        for _, row in df.iterrows():
            st.markdown(
                f"### 🧩 {row['tipo']}: {row['produto']}\n"
                f"💰 **{row['preco']}** — 🏪 {row['loja']}\n"
                f"[🔗 Ver produto na Amazon]({row['link']})",
                unsafe_allow_html=True
            )
            st.divider()

        st.subheader("🏆 Melhor custo-benefício por peça")
        ranking = df.loc[df.groupby("tipo")["preco_num"].idxmin()].reset_index(drop=True)
        for _, row in ranking.iterrows():
            st.markdown(
                f"✅ **{row['tipo']}** — [{row['produto']}]({row['link']}) por **{row['preco']}** ({row['loja']})",
                unsafe_allow_html=True
            )

        buffer = io.BytesIO()
        df.to_excel(buffer, index=False)
        st.download_button(
            "📥 Baixar Excel (comparativo_pc.xlsx)",
            data=buffer.getvalue(),
            file_name="comparativo_pc.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

st.divider()
st.caption("💡 Quando tiver sua tag de afiliado, defina a variável de ambiente AF_TAG para monetizar automaticamente os links.")
