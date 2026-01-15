from __future__ import annotations
from pathlib import Path
import shutil
from datetime import datetime

# --- CONFIGURAÇÕES DE CAMINHO ---
TEMPLATE_PATH = Path(
    r"C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\HUB MULTIMARCAS\HUB SDR - Versão 6.0.xlsm"
)

DESTINO_DIR = Path(r"Z:\Comercial\5 - Multimarcas\SDR\HUB")

NOMES_COPIAS = [
    "ANDRÉ.xlsm",
    "RAIANE.xlsm",
    "TARVYLLA.xlsm",
    "SCARLET.xlsm",
    "MARIA EDUARDA.xlsm",
    "OPEN.xlsm",
    "OPEN 2.xlsm",
]

def copiar_com_backup(template: Path, destino_dir: Path, nomes: list[str]) -> None:
    if not template.exists():
        raise FileNotFoundError(f"Template não encontrado: {template}")
    if template.suffix.lower() != ".xlsm":
        raise ValueError("O template precisa ser um arquivo .xlsm")

    destino_dir.mkdir(parents=True, exist_ok=True)

    # Pasta de backup dentro do destino
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = destino_dir / f"_backup_{stamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)

    print(f"Template: {template}")
    print(f"Destino : {destino_dir}")
    print(f"Backup  : {backup_dir}")
    print("-" * 60)

    for nome in nomes:
        nome = nome.strip()
        if not nome:
            continue
        if not nome.lower().endswith(".xlsm"):
            nome += ".xlsm"

        destino_arquivo = destino_dir / nome

        # Se já existir, move para backup antes de substituir
        if destino_arquivo.exists():
            try:
                backup_path = backup_dir / nome
                # Se por algum motivo já existir no backup, cria sufixo
                if backup_path.exists():
                    backup_path = backup_dir / f"{destino_arquivo.stem}_{stamp}{destino_arquivo.suffix}"

                shutil.move(str(destino_arquivo), str(backup_path))
                print(f"[BACKUP] {destino_arquivo.name} -> {backup_path.name}")
            except PermissionError:
                print(f"[ERRO] Não consegui mover para backup (arquivo em uso): {destino_arquivo.name}")
                continue # Pula para o próximo nome em vez de parar o script

        # Copia o template para o destino com o novo nome
        try:
            shutil.copy2(str(template), str(destino_arquivo))
            print(f"[OK] Criado/Substituído: {destino_arquivo.name}")
        except PermissionError:
            print(f"[ERRO] Sem permissão de escrita: {destino_arquivo.name}")
        except Exception as e:
            print(f"[ERRO FATAL] {destino_arquivo.name}: {e}")

    print("-" * 60)
    print("Concluído.")

if __name__ == "__main__":
    copiar_com_backup(TEMPLATE_PATH, DESTINO_DIR, NOMES_COPIAS)