import git
import mysql.connector
import os
import re
from datetime import datetime

# --- CONFIGURAÇÕES ---
REPO_PATH = os.path.dirname(os.path.abspath(__file__)) 
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'faturamento_multimarcas_dw'
}

class OrionGitPro:
    def __init__(self):
        self._force_unlock_git()
        
        try:
            self.repo = git.Repo(REPO_PATH)
        except git.exc.InvalidGitRepositoryError:
            print(f"🗂️ Inicializando novo repositório Git em: {REPO_PATH}")
            self.repo = git.Repo.init(REPO_PATH)

        self.sql_folder = os.path.join(REPO_PATH, 'scripts_sql')
        if not os.path.exists(self.sql_folder):
            os.makedirs(self.sql_folder)

    def _force_unlock_git(self):
        """Remove arquivos de trava que o OneDrive costuma prender."""
        git_dir = os.path.join(REPO_PATH, '.git')
        if not os.path.exists(git_dir): return

        trash_list = [
            os.path.join(git_dir, 'index.lock'),
            os.path.join(git_dir, 'refs/heads/main.lock'),
            os.path.join(git_dir, 'refs/heads/master.lock')
        ]

        for item in trash_list:
            if os.path.exists(item):
                try:
                    os.remove(item)
                    print(f"🧹 Limpeza OneDrive: {os.path.basename(item)} removido.")
                except:
                    pass

    def backup_procedures(self):
        """Extrai Procedures do MySQL."""
        print("🗄️ Extraindo Procedures do banco de dados...")
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            cursor.execute("SHOW PROCEDURE STATUS WHERE Db = %s", (DB_CONFIG['database'],))
            procedures = [p[1] for p in cursor.fetchall()]

            for p_name in procedures:
                cursor.execute(f"SHOW CREATE PROCEDURE `{p_name}`")
                content = cursor.fetchone()[2]
                
                clean_filename = re.sub(r'[\\/*?:"<>|]', "", p_name).replace(" ", "_")
                file_path = os.path.join(self.sql_folder, f"{clean_filename}.sql")
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content + ";")
            
            conn.close()
            return len(procedures)
        except Exception as e:
            print(f"❌ Erro ao conectar no Banco: {e}")
            return 0

    def update_readme_stats(self, total_procs):
        """Atualiza a data de sincronização no README."""
        readme_path = os.path.join(REPO_PATH, 'README.md')
        if not os.path.exists(readme_path):
            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write("# Repositório de Procedures\n")

        now = datetime.now().strftime('%d/%m/%Y %H:%M')
        
        with open(readme_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        with open(readme_path, 'w', encoding='utf-8') as f:
            found_sync = False
            for line in lines:
                if "Última Sincronização:" in line or "Sync:" in line:
                    f.write(f"**Última Sincronização:** {now} | **Procedures:** {total_procs}\n")
                    found_sync = True
                else:
                    f.write(line)
            
            if not found_sync:
                f.write(f"\n\n---\n**Última Sincronização:** {now} | **Procedures:** {total_procs}\n")

    def execute_flow(self, commit_type="feat", message="sync automatico"):
        """Ciclo completo de automação com correção de upstream."""
        try:
            total = self.backup_procedures()
            if total == 0: return

            self.update_readme_stats(total)

            # Detecta branch atual (main ou master)
            current_branch = self.repo.active_branch.name
            
            self.repo.git.add(all=True)
            
            if self.repo.is_dirty(untracked_files=True):
                full_msg = f"{commit_type}: {message} ({total} procs)"
                novo_commit = self.repo.index.commit(full_msg)
                print(f"✅ Commit local gerado: [{novo_commit.hexsha[:7]}]")
                
                print(f"🚀 Enviando para o servidor remoto (Branch: {current_branch})...")
                
                # AJUSTE AQUI: Define o upstream explicitamente no push
                origem = self.repo.remote(name='origin')
                push_info = origem.push(refspec=f'{current_branch}:{current_branch}', set_upstream=True)
                
                if push_info[0].flags & git.remote.PushInfo.ERROR:
                    print(f"❌ Erro no Push: {push_info[0].summary}")
                else:
                    print(f"✨ SUCESSO! Base de commits atualizada no GitHub.")
                    # Tenta gerar o link amigável
                    base_url = origem.url.replace('.git', '').replace('git@github.com:', 'https://github.com/')
                    print(f"🔗 Link: {base_url}/commit/{novo_commit.hexsha}")
            else:
                print("✨ Nenhuma mudança detectada. O repositório já está atualizado.")

        except Exception as e:
            print(f"💥 Falha crítica no fluxo: {e}")

if __name__ == "__main__":
    bot = OrionGitPro()
    bot.execute_flow()