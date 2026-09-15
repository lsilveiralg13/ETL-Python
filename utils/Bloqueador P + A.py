import os
import sys
import platform

# Lista de domínios para bloquear
SITES_PARA_BLOQUEAR = [
    # Sites de Apostas / Cassinos
    "bet365.com", "www.bet365.com",
    "betano.com", "www.betano.com",
    "blaze.com", "www.blaze.com",
    "stake.com", "www.stake.com",
    "kto.com", "www.kto.com",
    
    # Sites Adultos
    "xvideos.com", "www.xvideos.com",
    "pornhub.com", "www.pornhub.com",
    "redtube.com", "www.redtube.com",
    "xnxx.com", "www.xnxx.com"
]

# Determina o caminho do arquivo hosts de acordo com o Sistema Operacional
if platform.system() == "Windows":
    HOSTS_PATH = r"C:\Windows\System32\drivers\etc\hosts"
else: # Linux ou macOS
    HOSTS_PATH = "/etc/hosts"

# IP de Loopback (Aponta para a própria máquina, sem precisar consultar IP da rede)
REDIRECT_IP = "127.0.0.1"

def aplicar_bloqueio():
    try:
        # LER o conteúdo atual do arquivo
        with open(HOSTS_PATH, "r", encoding="utf-8") as file:
            conteudo = file.read()

        novos_bloqueios = []
        for site in SITES_PARA_BLOQUEAR:
            # Verifica se o site já não está na lista para evitar duplicatas
            if site not in conteudo:
                novos_bloqueios.append(f"{REDIRECT_IP} {site}\n")

        # ADICIONAR os novos bloqueios ao final do arquivo
        if novos_bloqueios:
            with open(HOSTS_PATH, "a", encoding="utf-8") as file:
                file.write("\n# --- BLOQUEIO APOSTAS E ADULTO ---\n")
                file.writelines(novos_bloqueios)
                file.write("# -----------------------------------\n")
            print("✅ Sites bloqueados com sucesso!")
        else:
            print("ℹ️ Todos os sites informados já estão bloqueados.")

        # Limpar o cache de DNS do sistema
        limpar_cache_dns()

    except PermissionError:
        exibir_erro_permissao()


# ==============================================================================
# FUNÇÃO DE DESBLOQUEIO (DESCOMENTE O BLOCO ABAIXO CASO PRECISE REMOVER OS BLOQUEIOS)
# ==============================================================================
# def remover_bloqueio():
#     try:
#         with open(HOSTS_PATH, "r", encoding="utf-8") as file:
#             linhas = file.readlines()
#
#         # Filtra mantendo apenas as linhas que NÃO pertencem aos sites da lista
#         linhas_limpas = [
#             linha for linha in linhas 
#             if not any(site in linha for site in SITES_PARA_BLOQUEAR) 
#             and "# --- BLOQUEIO" not in linha
#         ]
#
#         with open(HOSTS_PATH, "w", encoding="utf-8") as file:
#             file.writelines(linhas_limpas)
#
#         print("🔓 Todos os bloqueios foram removidos do arquivo hosts!")
#         limpar_cache_dns()
#
#     except PermissionError:
#         exibir_erro_permissao()
# ==============================================================================


def limpar_cache_dns():
    """Força o sistema a atualizar as rotas de rede imediatamente."""
    if platform.system() == "Windows":
        os.system("ipconfig /flushdns > nul")
        print("🔄 Cache de DNS do Windows limpo.")
    elif platform.system() == "Darwin": # macOS
        os.system("killall -HUP mDNSResponder")
        print("🔄 Cache de DNS do macOS limpo.")

def exibir_erro_permissao():
    print("❌ ERRO DE PERMISSÃO:")
    print("Você precisa rodar o script com privilégios de Administrador / Root.")
    if platform.system() == "Windows":
        print("👉 Abra o Prompt/PowerShell como Administrador e execute: python bloquear.py")
    else:
        print("👉 Execute no terminal: sudo python3 bloquear.py")


if __name__ == "__main__":
    # Executa o bloqueio por padrão
    aplicar_bloqueio()

    # Se quiser desbloquear no futuro:
    # 1. Comente a linha 'aplicar_bloqueio()' acima
    # 2. Descomente todo o bloco da função 'remover_bloqueio()'
    # 3. Descomente a linha abaixo:
    # remover_bloqueio()
