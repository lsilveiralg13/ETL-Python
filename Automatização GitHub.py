import os
import sys
from datetime import datetime
import git

# --- CONFIGURAÇÕES DO REPOSITÓRIO ---
REPO_PATH = os.path.dirname(os.path.abspath(__file__))
SQL_FOLDER_NAME = 'Scripts Python'  # Pasta onde você salva suas queries/procedures


class GitSyncPro:
    def __init__(self):
        self._force_unlock_git()
        
        try:
            self.repo = git.Repo(REPO_PATH)
        except git.exc.InvalidGitRepositoryError:
            print(f"🗂️ Inicializando novo repositório Git local em: {REPO_PATH}")
            self.repo = git.Repo.init(REPO_PATH)

        # Garante que a pasta de scripts existe localmente
        self.sql_folder = os.path.join(REPO_PATH, SQL_FOLDER_NAME)
        if not os.path.exists(self.sql_folder):
            os.makedirs(self.sql_folder)

    def _force_unlock_git(self):
        """Remove arquivos de trava criados pelo OneDrive ou processos interrompidos do Git."""
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
                    print(f"🧹 Trava do Git/OneDrive removida: {os.path.basename(item)}")
                except Exception as e:
                    print(f"⚠️ Não foi possível remover {os.path.basename(item)}: {e}")

    def count_sql_files(self) -> int:
        """Conta quantos arquivos SQL estão armazenados no repositório local."""
        if not os.path.exists(self.sql_folder):
            return 0
        sql_files = [f for f in os.listdir(self.sql_folder) if f.endswith('.sql')]
        return len(sql_files)

    def update_readme_stats(self, total_files: int):
        """Atualiza a estampa de tempo e quantidade de scripts no README.md."""
        readme_path = os.path.join(REPO_PATH, 'README.md')
        if not os.path.exists(readme_path):
            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write("# Repositório de Engenharia de Dados & SQLs\n\n")

        now = datetime.now().strftime('%d/%m/%Y %H:%M')
        
        with open(readme_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        found_sync = False
        new_lines = []
        for line in lines:
            if "Última Sincronização:" in line or "Sync:" in line:
                new_lines.append(f"**Última Sincronização:** {now} | **Scripts SQL Rastreados:** {total_files}\n")
                found_sync = True
            else:
                new_lines.append(line)
        
        if not found_sync:
            new_lines.append(f"\n---\n**Última Sincronização:** {now} | **Scripts SQL Rastreados:** {total_files}\n")

        with open(readme_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)

    def execute_flow(self, commit_type="feat", message="sync de scripts"):
        """Ciclo completo de staging, commit e push para o GitHub pessoal."""
        print("🚀 Iniciando sincronização do repositório local...")
        
        try:
            total_sql = self.count_sql_files()
            self.update_readme_stats(total_sql)

            # Adiciona todas as alterações ao staging
            self.repo.git.add(all=True)

            # Verifica se há alterações para commit
            if self.repo.is_dirty(untracked_files=True):
                current_branch = self.repo.active_branch.name
                full_msg = f"{commit_type}: {message} ({total_sql} scripts)"
                
                novo_commit = self.repo.index.commit(full_msg)
                print(f"✅ Commit local gerado: [{novo_commit.hexsha[:7]}] - \"{full_msg}\"")

                # Valida se existe um remoto configurado
                if not self.repo.remotes:
                    print("\n⚠️ Nenhum repositório remoto (origin) do GitHub vinculado.")
                    print("👉 Para vincular seu GitHub pessoal, execute no terminal:")
                    print("   git remote add origin https://github.com/SEU_USUARIO/SEU_REPOSITORIO.git")
                    return

                origem = self.repo.remote(name='origin')
                print(f"📡 Enviando alterações para o GitHub (Branch: {current_branch})...")

                push_info = origem.push(refspec=f'{current_branch}:{current_branch}', set_upstream=True)
                
                if push_info and (push_info[0].flags & git.remote.PushInfo.ERROR):
                    print(f"❌ Erro ao enviar para o GitHub: {push_info[0].summary}")
                else:
                    print("✨ SUCESSO! Repositório atualizado no GitHub pessoal.")
                    
                    # Formata o link direto do commit
                    raw_url = origem.url
                    base_url = raw_url.replace('.git', '').replace('git@github.com:', 'https://github.com/')
                    print(f"🔗 Link do Commit: {base_url}/commit/{novo_commit.hexsha}")
            else:
                print("✨ Nenhuma alteração pendente. Seu repositório já está 100% atualizado.")

        except Exception as e:
            print(f"💥 Falha na execução do fluxo: {e}")


if __name__ == "__main__":
    # Permite passar mensagem personalizada via linha de comando
    # Exemplo: python git_sync_pro.py "ajuste queries otif" "feat"
    msg = sys.argv[1] if len(sys.argv) > 1 else "atualizacao de scripts databricks"
    c_type = sys.argv[2] if len(sys.argv) > 2 else "feat"

    bot = GitSyncPro()
    bot.execute_flow(commit_type=c_type, message=msg)