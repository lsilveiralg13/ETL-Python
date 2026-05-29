import pyautogui
import os
import time
import cv2
import numpy as np
from datetime import datetime

# --- CONFIGURAÇÃO ---
NOME_ARQUIVO_PBI = "BM - INDICADORES FÁBRICA.pbix"
PASTA_RAIZ = os.path.dirname(os.path.abspath(__file__))
CAMINHO_PBIX = os.path.join(PASTA_RAIZ, NOME_ARQUIVO_PBI)

# Imagens de Alvo (Tire os prints e salve com esses nomes)
IMG_BOTAO_ATUALIZAR = os.path.join(PASTA_RAIZ, "btn_atualizar_seta.png")
IMG_OPCAO_ESQUEMA = os.path.join(PASTA_RAIZ, "opcao_esquema.png")

def log(mensagem):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] >> {mensagem}")

def ler_imagem_seguro(caminho):
    """Lê imagens em pastas com acento (Área de Trabalho)"""
    try:
        if not os.path.exists(caminho):
            log(f"ERRO: Imagem não encontrada: {caminho}")
            return None
        return cv2.imdecode(np.fromfile(caminho, dtype=np.uint8), cv2.IMREAD_UNCHANGED)
    except Exception as e:
        log(f"Falha ao ler {caminho}: {e}")
        return None

def esperar_clicar(caminho_img, nome, timeout=30):
    """Busca a imagem na tela e clica"""
    img_alvo = ler_imagem_seguro(caminho_img)
    if img_alvo is None: return False

    log(f"Buscando: {nome}...")
    inicio = time.time()
    while time.time() - inicio < timeout:
        try:
            res = pyautogui.locateCenterOnScreen(img_alvo, confidence=0.8)
            if res:
                pyautogui.click(res)
                log(f"Sucesso: {nome} clicado.")
                return True
        except:
            pass
        time.sleep(1)
    return False

def rodar_automacao():
    try:
        log(f"Iniciando: {NOME_ARQUIVO_PBI}")
        os.startfile(CAMINHO_PBIX)
        
        # 1. Aguarda abertura e maximiza
        time.sleep(60)
        pyautogui.hotkey('win', 'up')
        time.sleep(2)

        # 2. Passo 1: Clicar no botão Atualizar (na faixa de opções)
        if not esperar_clicar(IMG_BOTAO_ATUALIZAR, "Botão Atualizar"):
            # Fallback caso a imagem falhe: Alt+C+R
            log("Imagem não detectada, tentando via atalho Alt+C+R...")
            pyautogui.press('alt')
            time.sleep(1)
            pyautogui.press('c')
            time.sleep(1)
            pyautogui.press('r')
        
        # 3. Passo 2: Clicar em 'Esquema e dados' (no menu que abriu)
        time.sleep(2)
        if not esperar_clicar(IMG_OPCAO_ESQUEMA, "Opção Esquema e Dados"):
            log("Menu não detectado, tentando confirmar com Enter...")
            pyautogui.press('enter')

        # 4. Aguarda a carga (Ajuste conforme a necessidade da fábrica)
        log("Carga de dados iniciada. Aguardando 300s...")
        time.sleep(300)

        # 5. SALVAMENTO (Crucial para o seu fluxo)
        log("Salvando o arquivo...")
        pyautogui.hotkey('ctrl', 's')
        time.sleep(20) # Tempo para garantir que o salvamento concluiu no disco

        # 6. Fechamento
        log("Encerrando Power BI.")
        pyautogui.hotkey('alt', 'f4')
        log("=== PROCESSO CONCLUÍDO COM SUCESSO ===")

    except Exception as e:
        log(f"ERRO: {e}")

if __name__ == "__main__":
    rodar_automacao()