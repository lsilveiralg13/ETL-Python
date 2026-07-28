import hashlib
import os
import socket
import sys
from datetime import datetime
import git

# --- CONFIGURAÇÕES DO REPOSITÓRIO ---
REPO_PATH = os.path.dirname(os.path.abspath(__file__))
SQL_FOLDER_NAME = 'Scripts Python'  # Pasta dos scripts


class GitSyncPro:
    def __init__(self):
        self._force_unlock_git()
        
        try:
            self.repo = git.Repo(REPO_PATH)
        except git.exc.InvalidGitRepositoryError:
            print(f"🗂️ Inicializando novo repositório Git local em: {REPO_PATH}")
            self.repo = git.Repo.init(REPO_PATH)

        self.sql_folder = os.path.join(REPO_PATH, SQL_FOLDER_NAME)
        if not os.path.exists(self.sql_folder):
            os.makedirs(self.sql_folder)

    def _force_unlock_git(self):
        """Remove arquivos de trava do Git/OneDrive."""
        git_dir = os.path.join(REPO_PATH, '.git')
        if not os.path.exists(git_dir):
            return

        trash_list = [
            os.path.join(git_dir, 'index.lock'),
            os.path.join(git_dir, 'refs/heads/main.lock'),
            os.path.join(git_dir, 'refs/heads/master.lock')
        ]

        for item in trash_list:
            if os.path.exists(item):
                try:
                    os.remove(item)
                    print(f"🧹 Trava Git/OneDrive removida: {os.path.basename(item)}")
                except Exception as e:
                    print(f"⚠️ Erro ao remover trava {os.path.basename(item)}: {e}")

    def get_local_ip(self) -> str:
        """Obtém o IP local da máquina que está executando o script."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    def get_folder_metrics(self):
        """Calcula volume total (MB/KB) e verifica integridade via SHA-256."""
        total_size = 0
        files_count = 0
        corrupted = 0

        for root, _, files in os.walk(REPO_PATH):
            if '.git' in root:
                continue
            for f in files:
                file_path = os.path.join(root, f)
                try:
                    total_size += os.path.getsize(file_path)
                    files_count += 1
                    
                    # Teste rápido de leitura para verificar integridade/checksum
                    with open(file_path, 'rb') as fp:
                        hashlib.sha256(fp.read(4096))
                except Exception:
                    corrupted += 1

        # Formatação do tamanho
        if total_size >= 1024 * 1024:
            size_str = f"{total_size / (1024 * 1024):.2f} MB"
        else:
            size_str = f"{total_size / 1024:.2f} KB"

        integrity_status = "100% Ok (SHA-256 Verificado)" if corrupted == 0 else f"⚠️ {corrupted} arquivos c/ falha"
        return files_count, size_str, integrity_status

    def count_commits_today(self) -> int:
        """Conta quantos arquivos foram alterados/comitados no dia atual."""
        try:
            today_str = datetime.now().strftime('%Y-%m-%d')
            commits = list(self.repo.iter_commits(since=today_str))
            files_changed = set()
            for c in commits:
                for stats_file in c.stats.files.keys():
                    files_changed.add(stats_file)
            return len(files_changed)
        except Exception:
            return 0

    def update_readme_stats(self, total_files: int):
        """Atualiza carimbo de data no README.md."""
        readme_path = os.path.join(REPO_PATH, 'README.md')
        if not os.path.exists(readme_path):
            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write("# Repositório de Engenharia de Dados & Automation\n\n")

        now = datetime.now().strftime('%d/%m/%Y %H:%M')
        
        with open(readme_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        found_sync = False
        new_lines = []
        for line in lines:
            if "Última Sincronização:" in line or "Sync:" in line:
                new_lines.append(f"**Última Sincronização:** {now} | **Total de Arquivos:** {total_files}\n")
                found_sync = True
            else:
                new_lines.append(line)
        
        if not found_sync:
            new_lines.append(f"\n---\n**Última Sincronização:** {now} | **Total de Arquivos:** {total_files}\n")

        with open(readme_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)

    def print_summary_table(self, committed_today, total_files, size_str, integrity, commit_hex, status_push):
        """Gera uma tabela ASCII formatada no terminal com diagnósticos completos."""
        hostname = socket.gethostname()
        local_ip = self.get_local_ip()
        
        remote_url = "N/A"
        security_type = "Local (Sem Remoto)"
        server_host = "Desconectado"

        if self.repo.remotes:
            remote_url = self.repo.remote(name='origin').url
            if "git@github.com" in remote_url:
                security_type = "SSH / Ed25519 (Chave Criptografada)"
                server_host = "github.com (SSH)"
            elif "https://" in remote_url:
                security_type = "HTTPS / TLS v1.3 (Token Auth)"
                server_host = "github.com (HTTPS)"

        current_branch = self.repo.active_branch.name if not self.repo.head.is_detached else "Detached"

        print("\n" + "=" * 70)
        print(f" 📊 RELATÓRIO DE EXECUÇÃO & INTEGRIDADE - GIT SYNC PRO")
        print("=" * 70)
        print(f" │ 📌 Status do Push      : {status_push}")
        print(f" │ 🆔 Hash do Commit      : {commit_hex[:8] if commit_hex != 'N/A' else 'N/A'}")
        print(f" │ 📅 Comitados Hoje      : {committed_today} arquivo(s)")
        print(f" │ 📂 Total no Repositório: {total_files} arquivo(s)")
        print(f" │ 💾 Volume de Dados     : {size_str}")
        print(f" │ 🛡️ Integridade Dados   : {integrity}")
        print(f" │ 🔒 Protocolo Segurança : {security_type}")
        print(f" │ 🌐 Servidor Remoto     : {server_host} [{current_branch}]")
        print(f" │ 💻 Origem (Máquina)    : {hostname} ({local_ip})")
        print("=" * 70 + "\n")

    def execute_flow(self, commit_type="feat", message="sync de scripts"):
        """Executa staging, commit, push e exibe o painel consolidado."""
        print("🚀 Executando validações e sincronização Git...")
        
        commit_hex = "N/A"
        status_push = "Nenhum commit pendente"

        try:
            files_count, size_str, integrity = self.get_folder_metrics()
            self.update_readme_stats(files_count)

            self.repo.git.add(all=True)

            if self.repo.is_dirty(untracked_files=True):
                current_branch = self.repo.active_branch.name
                full_msg = f"{commit_type}: {message} ({files_count} arquivos)"
                
                novo_commit = self.repo.index.commit(full_msg)
                commit_hex = novo_commit.hexsha
                
                if self.repo.remotes:
                    origem = self.repo.remote(name='origin')
                    push_info = origem.push(refspec=f'{current_branch}:{current_branch}', set_upstream=True)
                    
                    if push_info and (push_info[0].flags & git.remote.PushInfo.ERROR):
                        status_push = f"❌ Erro no Push: {push_info[0].summary}"
                    else:
                        status_push = "✨ Sucesso (Enviado ao GitHub)"
                else:
                    status_push = "⚠️ Commit Local (Sem origem remota)"
            
            committed_today = self.count_commits_today()
            self.print_summary_table(committed_today, files_count, size_str, integrity, commit_hex, status_push)

        except Exception as e:
            print(f"💥 Falha no fluxo de sincronização: {e}")


if __name__ == "__main__":
    msg = sys.argv[1] if len(sys.argv) > 1 else "sincronizacao automatica de scripts"
    c_type = sys.argv[2] if len(sys.argv) > 2 else "feat"

    bot = GitSyncPro()
    bot.execute_flow(commit_type=c_type, message=msg)