import os
import win32com.client

# Caminho da pasta onde estão os PPTX
pasta_pptx = r"Z:\Comercial\5 - Multimarcas\SDR\Cards Lojas Multimarcas\2025\12 - Dezembro"
# Pasta de destino das imagens completas (cards)
pasta_saida = r"C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\Cards_Exportados"

os.makedirs(pasta_saida, exist_ok=True)

# Inicializa o PowerPoint
app = win32com.client.Dispatch("PowerPoint.Application")
app.Visible = True  # opcional

contador = 1

for arquivo in os.listdir(pasta_pptx):
    if arquivo.lower().endswith((".pptx", ".pprt")) and not arquivo.startswith("~$"):
        caminho_arquivo = os.path.join(pasta_pptx, arquivo)
        try:
            pres = app.Presentations.Open(caminho_arquivo, WithWindow=False)

            for slide in pres.Slides:
                nome_arquivo = f"card_{contador:04d}.png"
                caminho_imagem = os.path.join(pasta_saida, nome_arquivo)

                # Exporta o slide completo (renderizado)
                slide.Export(caminho_imagem, "PNG")
                print(f"✅ Card exportado: {nome_arquivo}")
                contador += 1

            pres.Close()

        except Exception as e:
            print(f"⚠️ Erro ao processar {arquivo}: {e}")

app.Quit()
print(f"\n🏁 Exportação concluída! {contador - 1} cards salvos em '{pasta_saida}'")
