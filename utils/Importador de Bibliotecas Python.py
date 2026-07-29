import os
import subprocess
import platform
from datetime import datetime

# ==========================================================
# CONFIGURAÇÃO
# ==========================================================

CAMINHO_BASE = r"C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python"

DATA = datetime.now().strftime("%Y%m%d_%H%M%S")

PASTA_BACKUP = os.path.join(
    CAMINHO_BASE,
    f"Backup_Ambiente_Python_{DATA}"
)

os.makedirs(PASTA_BACKUP, exist_ok=True)

print("=" * 70)
print("INICIANDO BACKUP DO AMBIENTE PYTHON")
print("=" * 70)

# ==========================================================
# REQUIREMENTS.TXT
# ==========================================================

requirements = os.path.join(
    PASTA_BACKUP,
    "requirements.txt"
)

print("\n[1/5] Gerando requirements.txt...")

with open(requirements, "w", encoding="utf-8") as f:
    subprocess.run(
        ["python", "-m", "pip", "freeze"],
        stdout=f,
        text=True
    )

print("OK")

# ==========================================================
# PIP LIST
# ==========================================================

pip_list = os.path.join(
    PASTA_BACKUP,
    "pip_list.txt"
)

print("\n[2/5] Gerando inventário de bibliotecas...")

with open(pip_list, "w", encoding="utf-8") as f:
    subprocess.run(
        ["python", "-m", "pip", "list"],
        stdout=f,
        text=True
    )

print("OK")

# ==========================================================
# INFO PYTHON
# ==========================================================

info = os.path.join(
    PASTA_BACKUP,
    "ambiente.txt"
)

print("\n[3/5] Salvando informações do ambiente...")

with open(info, "w", encoding="utf-8") as f:

    f.write(f"Python Version: {platform.python_version()}\n")
    f.write(f"Sistema: {platform.platform()}\n")
    f.write(f"Arquitetura: {platform.architecture()}\n")

print("OK")

# ==========================================================
# DOWNLOAD DOS PACOTES
# ==========================================================

PASTA_PACOTES = os.path.join(
    PASTA_BACKUP,
    "pacotes"
)

os.makedirs(PASTA_PACOTES, exist_ok=True)

print("\n[4/5] Baixando pacotes para instalação offline...")
print("Isso pode demorar alguns minutos...\n")

subprocess.run([
    "python",
    "-m",
    "pip",
    "download",
    "-r",
    requirements,
    "-d",
    PASTA_PACOTES
])

print("OK")

# ==========================================================
# BAT DE RESTAURAÇÃO
# ==========================================================

bat = os.path.join(
    PASTA_BACKUP,
    "restaurar_ambiente.bat"
)

print("\n[5/5] Criando arquivo de restauração...")

with open(bat, "w", encoding="utf-8") as f:

    f.write("@echo off\n")
    f.write("title Restauracao Ambiente Python\n")
    f.write("echo.\n")
    f.write("echo ==========================================\n")
    f.write("echo RESTAURANDO AMBIENTE PYTHON\n")
    f.write("echo ==========================================\n")
    f.write("echo.\n")
    f.write("python -m pip install --upgrade pip\n")
    f.write("echo.\n")
    f.write("pip install --no-index --find-links=pacotes -r requirements.txt\n")
    f.write("echo.\n")
    f.write("echo Ambiente restaurado com sucesso.\n")
    f.write("pause\n")

print("OK")

# ==========================================================
# FINAL
# ==========================================================

print("\n" + "=" * 70)
print("BACKUP CONCLUÍDO COM SUCESSO")
print("=" * 70)

print(f"\nLocal:")
print(PASTA_BACKUP)

print("\nArquivos gerados:")
print("✓ requirements.txt")
print("✓ pip_list.txt")
print("✓ ambiente.txt")
print("✓ restaurar_ambiente.bat")
print("✓ pasta pacotes\\")

print("\nNa máquina nova:")
print("1. Instale o Python")
print("2. Copie toda a pasta de backup")
print("3. Execute restaurar_ambiente.bat")

input("\nPressione ENTER para finalizar...")