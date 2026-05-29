# -*- coding: utf-8 -*-
"""
🤖 ROBÔ DE AUTOMAÇÃO SANKHYA OM -> SHAREPOINT ONLINE (VISÃO COMPUTACIONAL FLEXÍVEL)
Desenvolvido para: Lucas Barros / Orion
Mecânica: Login/Busca (Playwright) + Cliques por Imagem com Alta Tolerância (PyAutoGUI)
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import pyautogui

# Configuração de velocidade do mouse físico
pyautogui.PAUSE = 0.5

# ==============================================================================
# CONFIGURAÇÕES DO USUÁRIO
# ==============================================================================
SANKHYA_URL = "http://belind.grupobelmicro.com.br:8180/mge/login.jsp?ri=fsso"
USUARIO = "LUCAS.BARROS"
SENHA = "Bel@2029"
NOME_RELATORIO = "Relatório Reparo Manutenção"
SHAREPOINT_URL = "https://belmicrotech.sharepoint.com/sites/PowerBI/Documentos%20Compartilhados/Forms/AllItems.aspx"

PASTA_DOWNLOADS_LOCAL = Path(os.path.expanduser("~")) / "Downloads"

# ==============================================================================
# LOGICA PRINCIPAL DO ROBÔ
# ==============================================================================

def log(mensagem):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {mensagem}")

def executar_robo():
    log("🚀 Iniciando o ciclo de automação por reconhecimento de imagem flexível...")

    # Validação inicial das imagens de corte
    if not os.path.exists("setinha.png") or not os.path.exists("excel.png"):
        log("❌ ERRO CRÍTICO: As imagens 'setinha.png' ou 'excel.png' não foram encontradas na pasta!")
        return

    with sync_playwright() as p:
        log("🌐 Abrindo navegador Chromium...")
        browser = p.chromium.launch(headless=False, args=["--start-maximized"])
        context = browser.new_context(no_viewport=True)
        page = context.new_page()
        
        try:
            # 1. ACESSAR O SANKHYA
            log(f"🔗 Conectando ao servidor Sankhya em: {SANKHYA_URL}")
            page.goto(SANKHYA_URL, timeout=60000)
            page.wait_for_load_state("load")
            
            # 2. LOGIN EM DUAS ETAPAS
            log("🔑 Logando no Sankhya Om...")
            page.wait_for_selector("input[placeholder*='Usuário'], input[type='text']", timeout=30000)
            page.fill("input[placeholder*='Usuário'], input[type='text']", USUARIO)
            page.click("button:has-text('Prosseguir')")
            
            page.wait_for_selector("input[type='password']", timeout=15000)
            page.fill("input[type='password']", SENHA)
            page.keyboard.press("Enter")
            
            log("⏳ Aguardando autenticação e carregamento do Dashboard...")
            page.wait_for_load_state("networkidle")
            time.sleep(6)
            
            # 3. BUSCAR RELATÓRIO
            log("🔍 Acionando pesquisa global do Sankhya (Ctrl+Alt+G)...")
            page.keyboard.press("Control+Alt+KeyG")
            time.sleep(2)
            
            log(f"✍️ Digitando o nome do relatório: '{NOME_RELATORIO}'")
            page.keyboard.type(NOME_RELATORIO, delay=50) 
            time.sleep(1)
            page.keyboard.press("Enter")
            
            log("⏳ Aguardando abertura e carregamento total da tabela de reparos...")
            time.sleep(15) 
            
            # ==============================================================================
            # 4. EXPORTAÇÃO POR RECONHECIMENTO VISUAL (FLEXÍVEL)
            # ==============================================================================
            log("📸 Procurando geometricamente o botão do dropdown (setinha.png) na tela...")
            
            # Baixamos a confiança para 0.6 e ativamos grayscale para ignorar variações de cor/brilho
            posicao_seta = None
            for tentativa in range(5):
                try:
                    posicao_seta = pyautogui.locateOnScreen("setinha.png", confidence=0.6, grayscale=True)
                    if posicao_seta:
                        break
                except Exception as e:
                    pass
                time.sleep(2)
                
            if not posicao_seta:
                raise Exception("Não foi possível localizar o botão da setinha preta mesmo com alta tolerância.")
                
            centro_seta = pyautogui.center(posicao_seta)
            log(f"🎯 Setinha localizada em: {centro_seta}. Executando clique...")
            pyautogui.click(centro_seta)
            
            time.sleep(3) 
            
            log("📸 Procurando geometricamente o ícone do Excel (excel.png) no pop-up aberto...")
            posicao_excel = None
            for tentativa in range(5):
                try:
                    posicao_excel = pyautogui.locateOnScreen("excel.png", confidence=0.6, grayscale=True)
                    if posicao_excel:
                        break
                except Exception as e:
                    pass
                time.sleep(2)
                
            if not posicao_excel:
                raise Exception("O menu abriu, mas não localizei o ícone 'Exportar para planilha (xlsx)'.")
                
            centro_excel = pyautogui.center(posicao_excel)
            
            with page.expect_download(timeout=120000) as download_info:
                log(f"💥 Clicando no ícone do XLSX em: {centro_excel}")
                pyautogui.click(centro_excel)
                
            download = download_info.value
            nome_original = download.suggested_filename
            log(f"📦 Sucesso! Relatório capturado pelo robô: '{nome_original}'")
            
            caminho_temporario = PASTA_DOWNLOADS_LOCAL / nome_original
            download.save_as(caminho_temporario)
            
            # ==============================================================================
            # 5. UPLOAD PARA O SHAREPOINT ONLINE
            # ==============================================================================
            log("🌐 Direcionando navegador para o SharePoint Online...")
            page.goto(SHAREPOINT_URL, timeout=60000)
            page.wait_for_load_state("load")
            time.sleep(5)
            
            if "login.microsoftonline.com" in page.url or page.locator("input[type='email']").is_visible():
                log("⚠️ Autenticação do SharePoint necessária! Faça o login no navegador e clique em 'Resume'.")
                page.pause()
            
            log("📁 Realizando upload para o SharePoint...")
            page.wait_for_selector("button:has-text('Carregar'), button:has-text('Upload')", timeout=45000)
            page.locator("button:has-text('Carregar'), button:has-text('Upload')").first.click()
            time.sleep(1)
            
            with page.expect_file_chooser() as fc_info:
                page.locator("button:has-text('Arquivos'), button:has-text('Files'), text=Arquivo").first.click()
            
            file_chooser = fc_info.value
            file_chooser.set_files(caminho_temporario)
            
            log("⏳ Finalizando upload na nuvem...")
            time.sleep(8) 
            log("🏆 Processo concluído com total sucesso!")
            
            if caminho_temporario.exists():
                caminho_temporario.unlink()
                log("🧹 Downloads locais limpos.")
            
        except Exception as e:
            log(f"❌ Erro crítico no processo: {str(e)}")
        finally:
            log("🔒 Encerrando sessões...")
            context.close()
            browser.close()

if __name__ == "__main__":
    executar_robo()