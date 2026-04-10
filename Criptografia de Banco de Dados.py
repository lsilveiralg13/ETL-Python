import os
import gzip
import shutil
import subprocess
from datetime import datetime, timedelta
from cryptography.fernet import Fernet

# ==============================
# CONFIGURAÇÕES
# ==============================

DB_NAME = "belmicro"
DB_USER = "root"
DB_PASSWORD = "root"
DB_HOST = "localhost"

MYSQLDUMP_PATH = r"C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python\MySQLDUMP"

BACKUP_DIR = r"C:\backup_mysql"
LOG_FILE = os.path.join(BACKUP_DIR, "backup_log.txt")

KEY_FILE = "chave.key"
RETENTION_DAYS = 7

os.makedirs(BACKUP_DIR, exist_ok=True)

# ==============================
# LOG
# ==============================

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} - {msg}\n")

# ==============================
# CRIPTOGRAFIA
# ==============================

def criptografar(arquivo):
    with open(KEY_FILE, "rb") as f:
        key = f.read()

    fernet = Fernet(key)

    with open(arquivo, "rb") as f:
        dados = f.read()

    dados_criptografados = fernet.encrypt(dados)

    arquivo_enc = arquivo + ".enc"

    with open(arquivo_enc, "wb") as f:
        f.write(dados_criptografados)

    os.remove(arquivo)

    return arquivo_enc

# ==============================
# BACKUP
# ==============================

def realizar_backup():
    data_str = datetime.now().strftime("%Y-%m-%d_%H-%M")

    arquivo_sql = os.path.join(BACKUP_DIR, f"{DB_NAME}_{data_str}.sql")
    arquivo_gz = arquivo_sql + ".gz"

    comando = [
        MYSQLDUMP_PATH,
        "-h", DB_HOST,
        "-u", DB_USER,
        f"-p{DB_PASSWORD}",
        DB_NAME
    ]

    log("Iniciando backup...")

    with open(arquivo_sql, "w", encoding="utf-8") as f:
        subprocess.run(comando, stdout=f)

    # Compactação
    with open(arquivo_sql, "rb") as f_in:
        with gzip.open(arquivo_gz, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove(arquivo_sql)

    log(f"Arquivo compactado: {arquivo_gz}")

    return arquivo_gz

# ==============================
# LIMPEZA
# ==============================

def limpar_antigos():
    limite = datetime.now() - timedelta(days=RETENTION_DAYS)

    for arquivo in os.listdir(BACKUP_DIR):
        caminho = os.path.join(BACKUP_DIR, arquivo)

        if os.path.isfile(caminho):
            data_mod = datetime.fromtimestamp(os.path.getmtime(caminho))

            if data_mod < limite:
                os.remove(caminho)
                log(f"Removido: {arquivo}")

# ==============================
# EXECUÇÃO
# ==============================

if __name__ == "__main__":
    log("=== INICIO ===")

    try:
        arquivo = realizar_backup()
        arquivo_enc = criptografar(arquivo)
        limpar_antigos()

        log(f"Backup final: {arquivo_enc}")

    except Exception as e:
        log(f"ERRO: {str(e)}")

    log("=== FIM ===\n")