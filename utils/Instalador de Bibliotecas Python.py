import subprocess
import sys
from pathlib import Path

# Caminho absoluto para o seu arquivo requirements.txt
CAMINHO_REQ = r"C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python\requirements.txt"

def instalacao_forcada():
    arquivo = Path(CAMINHO_REQ)
    
    if not arquivo.exists():
        print(f"❌ Erro: O arquivo não foi encontrado em: {CAMINHO_REQ}")
        return

    print("🔄 Atualizando instaladores base (pip, setuptools, wheel)...")
    subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])

    print("📖 Lendo a lista de bibliotecas...")
    with open(arquivo, "r", encoding="utf-8") as f:
        linhas = f.readlines()

    # Filtra as linhas de forma limpa e tradicional
    bibliotecas = []
    for linha in linhas:
        linha_limpa = linha.strip()
        # Ignora linhas vazias ou comentários
        if linha_limpa and not linha_limpa.startswith("#"):
            # Pega apenas o nome da biblioteca antes de qualquer sinal de '==' ou '>='
            nome_lib = linha_limpa.split("=")[0].split(">")[0].strip()
            if nome_lib:
                bibliotecas.append(nome_lib)

    total = len(bibliotecas)
    print(f"\n🚀 Iniciando a instalação de {total} bibliotecas.")
    print("⚠️ Se alguma biblioteca falhar por incompatibilidade com o Python 3.14, eu vou pular e continuar!\n")

    instaladas = 0
    falhou = []

    for i, lib in enumerate(bibliotecas, 1):
        print(f"[{i}/{total}] Tentando instalar: {lib}...")
        
        # Executa o pip de forma individual para isolar os erros
        resultado = subprocess.run(
            [sys.executable, "-m", "pip", "install", lib, "--timeout", "60"],
            stdout=subprocess.DEVNULL,  # Oculta o log gigante de progresso
            stderr=subprocess.PIPE
        )
        
        if resultado.returncode == 0:
            print(f"   ✅ {lib} instalada com sucesso!")
            instaladas += 1
        else:
            print(f"   ⚠️ Falhou (incompatível ou indisponível para Python 3.14). Pulando...")
            falhou.append(lib)

    print("\n" + "="*40)
    print("🏁 PROCESSO CONCLUÍDO!")
    print(f"📦 Total de bibliotecas instaladas com sucesso: {instaladas}")
    print(f"❌ Total de pacotes que falharam: {len(falhou)}")
    print("="*40)

if __name__ == "__main__":
    instalacao_forcada()