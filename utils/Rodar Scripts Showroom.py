import subprocess
import os
import time
from datetime import datetime

# Configurações de diretório
diretorio = r"C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\Scripts Python"
# Removido o script de Mix de Produtos da lista
arquivos_py = ["ETL - Venda SR.py", "ETL - Cadastro SR.py"]
log_robusto = os.path.join(diretorio, "manutencao_tecnica.log")

def executar_com_logs():
    # --- LOG DE CONSOLE (SIMPLES) ---
    print(f"\n>>> STATUS DE EXECUÇÃO - {datetime.now().strftime('%d/%m %H:%M')} <<<")
    print(f"{'-'*60}")
    print(f"{'PROCESSO':<25} | {'STATUS':<10} | {'LINHAS'} | {'TEMPO'}")
    print(f"{'-'*60}")

    # --- LOG EM TEXTO (ROBUSTO) ---
    with open(log_robusto, "a", encoding='utf-8') as f_log:
        f_log.write(f"\n{'='*80}\n")
        f_log.write(f"SESSÃO DE MANUTENÇÃO: {datetime.now()}\n")
        f_log.write(f"DIRETÓRIO: {diretorio}\n{'='*80}\n")

        for script in arquivos_py:
            caminho = os.path.join(diretorio, script)
            inicio = time.time()
            
            # Executa capturando TUDO (Saída padrão e Erros)
            proc = subprocess.run(["python", caminho], capture_output=True, text=True, encoding='utf-8')
            
            duracao = f"{time.time() - inicio:.1f}s"
            status = "OK" if proc.returncode == 0 else "ERRO"
            
            # Lógica para o Console: busca apenas a linha de contagem
            linhas = "N/A"
            for line in proc.stdout.split('\n'):
                if "linhas" in line.lower():
                    numeros = "".join(filter(str.isdigit, line))
                    linhas = numeros if numeros else "Lido"

            # Print no Console (Minimalista)
            print(f"{script[:25]:<25} | {status:<10} | {linhas:<6} | {duracao}")

            # Escrita no Arquivo (Robusta para Manutenção)
            f_log.write(f"\n[SCRIPT]: {script}\n")
            f_log.write(f"[STATUS]: {status} (Código: {proc.returncode})\n")
            f_log.write(f"[TEMPO]: {duracao}\n")
            f_log.write(f"[STDOUT]:\n{proc.stdout}\n")
            if proc.stderr:
                f_log.write(f"[STDERR/TRACEBACK]:\n{proc.stderr}\n")
            f_log.write(f"{'-'*40}\n")

        f_log.write(f"FINAL DA SESSÃO: {datetime.now()}\n")

    print(f"{'-'*60}")
    print(f"Log técnico detalhado: {log_robusto}\n")

if __name__ == "__main__":
    executar_com_logs()