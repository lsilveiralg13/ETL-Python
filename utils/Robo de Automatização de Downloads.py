# -*- coding: utf-8 -*-

import os
import time
from pathlib import Path
from datetime import datetime

import pyautogui
from playwright.sync_api import sync_playwright

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================

SANKHYA_URL = "http://belind.grupobelmicro.com.br:8180/mge/login.jsp?expired=true"
USUARIO = "LUCAS.BARROS"
SENHA = "Bel@2029"

NOME_RELATORIO = "Relatório Reparo Manutenção"

SHAREPOINT_URL = "https://belmicrotech.sharepoint.com/sites/PowerBI/Documentos%20Compartilhados/Forms/AllItems.aspx?id=%2Fsites%2FPowerBI%2FDocumentos%20Compartilhados%2FManaus&viewid=4bacf264%2Defe1%2D4c0b%2Db0a2%2D7bebca4b0a35"

PASTA_DOWNLOADS = Path.home() / "Downloads"
ARQUIVO_ESPERADO = "Relatorio_Reparo_Manutencao"

# Coordenadas validadas para o PyAutoGUI
SETA_X = 1802
SETA_Y = 296

EXCEL_X = 1830
EXCEL_Y = 405

pyautogui.FAILSAFE = True
pyautogui.PAUSE = 0.3

# ==============================================================================

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# ==============================================================================

def limpar_arquivos_antigos():
    arquivos = list(PASTA_DOWNLOADS.glob(f"{ARQUIVO_ESPERADO}*.xlsx"))
    for arq in arquivos:
        try:
            arq.unlink()
            log(f"Arquivo antigo removido: {arq.name}")
        except:
            pass

# ==============================================================================

def aguardar_download(timeout=180):
    inicio = time.time()
    while True:
        arquivos = list(PASTA_DOWNLOADS.glob(f"{ARQUIVO_ESPERADO}*.xlsx"))
        
        if arquivos:
            arquivo = max(arquivos, key=lambda x: x.stat().st_mtime)
            tamanho1 = arquivo.stat().st_size
            time.sleep(2)
            tamanho2 = arquivo.stat().st_size
            
            if tamanho1 == tamanho2 and tamanho1 > 0:
                return arquivo
                
        if time.time() - inicio > timeout:
            raise TimeoutError("Download não foi concluído ou encontrado no tempo limite.")
        time.sleep(1)

# ==============================================================================

def fechar_bia_sankhya(page):
    log("Verificando se a assistente 'BIA' está ativa na tela...")
    try:
        # 1. Tentativa por seletores comuns do botão de fechar da Bia
        botoes_fechar_bia = page.locator(
            "button[title*='Bia'], .bia-close-button, [data-sk-id='bia-close'], button:has-text('Fechar Bia'), .fa-times"
        )
        
        for i in range(botoes_fechar_bia.count()):
            el = botoes_fechar_bia.nth(i)
            if el.is_visible():
                el.click()
                log("Bia fechada clicando no elemento visível.")
                time.sleep(1)
                return

        # 2. Injeção brutal de JavaScript caso o botão HTML mude de classe
        page.evaluate("""
            () => {
                const elementosBia = document.querySelectorAll("[class*='bia'], [id*='bia'], .sk-bia-container");
                elementosBia.forEach(el => el.style.display = 'none');
            }
        """)
        log("Ocultando elementos visuais da Bia via DOM.")
        time.sleep(1)
    except Exception as e:
        log(f"Aviso ao tentar ocultar a Bia: {e}")

# ==============================================================================

def exportar_excel():
    log("Trazendo foco para a tela e abrindo menu de exportação")
    pyautogui.click(100, 100) 
    time.sleep(0.5)
    
    pyautogui.click(SETA_X, SETA_Y)
    time.sleep(1.5) 

    log("Selecionando opção Excel")
    pyautogui.click(EXCEL_X, EXCEL_Y)

# ==============================================================================

def executar():
    log("Iniciando processo ETL - Sankhya para SharePoint")
    limpar_arquivos_antigos()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=["--start-maximized"]
        )
        context = browser.new_context(no_viewport=True)
        page = context.new_page()

        try:
            # ------------------------------------------------------------------
            # LOGIN SANKHYA
            # ------------------------------------------------------------------
            log("Abrindo tela de login do Sankhya")
            page.goto(SANKHYA_URL, timeout=60000)
            page.wait_for_load_state("load")

            log("Preenchendo credenciais")
            page.wait_for_selector("input[type='text']")
            page.fill("input[type='text']", USUARIO)
            page.click("button:has-text('Prosseguir')")

            page.wait_for_selector("input[type='password']")
            page.fill("input[type='password']", SENHA)
            page.keyboard.press("Enter")

            log("Aguardando Dashboard Inicial do Sankhya")
            page.wait_for_load_state("networkidle")
            time.sleep(5)

            # Primeira limpa na BIA
            fechar_bia_sankhya(page)

            # ------------------------------------------------------------------
            # BUSCA RELATÓRIO
            # ------------------------------------------------------------------
            log(f"Abrindo o lançador de rotinas (Ctrl+Alt+G) para: {NOME_RELATORIO}")
            page.keyboard.press("Control+Alt+g") 
            time.sleep(2)

            page.keyboard.type(NOME_RELATORIO, delay=60)
            time.sleep(1.5) 

            log("Selecionando e executando o relatório da lista")
            page.keyboard.press("ArrowDown") 
            time.sleep(0.5)
            page.keyboard.press("Enter")     

            log("Aguardando carregamento completo do relatório (15s)")
            time.sleep(15) 
            page.bring_to_front()

            # Segunda garantia contra a BIA na nova aba
            fechar_bia_sankhya(page)

            # ------------------------------------------------------------------
            # EXPORTAÇÃO (HÍBRIDA COM PYAUTOGUI)
            # ------------------------------------------------------------------
            exportar_excel()

            log("Aguardando arquivo XLSX na pasta Downloads...")
            arquivo_baixado = aguardar_download()
            log(f"Download detectado com sucesso: {arquivo_baixado.name}")

            # ------------------------------------------------------------------
            # SHAREPOINT UPLOAD
            # ------------------------------------------------------------------
            log("Navegando até a pasta do SharePoint (Manaus)")
            page.goto(SHAREPOINT_URL, timeout=60000)
            page.wait_for_load_state("load")
            time.sleep(6) 

            try:
                if page.locator("input[type='email']").is_visible(timeout=3000):
                    log("⚠️ Login corporativo solicitado! Resolva a autenticação na tela.")
                    page.pause() 
            except:
                pass

            log("Localizando botões de Upload no SharePoint")
            page.wait_for_selector("button:has-text('Carregar'), button:has-text('Upload')")
            page.locator("button:has-text('Carregar'), button:has-text('Upload')").first.click()

            with page.expect_file_chooser() as fc:
                page.locator("button:has-text('Arquivos'), button:has-text('Files')").first.click()

            chooser = fc.value
            chooser.set_files(str(arquivo_baixado))

            log("Upload iniciado. Aguardando processamento da nuvem (15s)...")
            time.sleep(15)
            log("🚀 Arquivo enviado com sucesso ao SharePoint!")

            # ------------------------------------------------------------------
            # LIMPEZA LOCAL
            # ------------------------------------------------------------------
            try:
                arquivo_baixado.unlink()
                log("Limpeza concluída: Arquivo temporário local removido.")
            except Exception as e:
                log(f"Aviso: Não foi possível deletar o arquivo local: {e}")

        except Exception as erro:
            log(f"❌ ERRO CRÍTICO NO PROCESSO: {str(erro)}")

        finally:
            log("Fechando instâncias do navegador.")
            context.close()
            browser.close()

# ==============================================================================

if __name__ == "__main__":
    try:
        executar()
    except KeyboardInterrupt:
        log("Processo interrompido manualmente pelo usuário.")