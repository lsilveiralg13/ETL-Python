import git
import mysql.connector
import os
import re
import shutil
from datetime import datetime

# --- CONFIGURAÇÕES ---
REPO_PATH = r'C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\Scripts Python'
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'faturamento_multimarcas_dw'
}

class OrionGitPro:
    def __init__(self):
        self._force_unlock_git() # Limpa travas do OneDrive antes de começar
        
        try:
            self.repo = git.Repo(REPO_PATH)
        except git.exc.InvalidGitRepositoryError:
            print(f"🗂️ Inicializando novo repositório Git em: {REPO_PATH}")
            self.repo = git.Repo.init(REPO_PATH)

        self.sql_folder = os.path.join(REPO_PATH, 'scripts_sql')
        if not os.path.exists(self.sql_folder):
            os.makedirs(self.sql_folder)

    def _force_unlock_git(self):
        """Remove arquivos de trava e pastas de rebase que o OneDrive costuma prender."""
        git_dir = os.path.join(REPO_PATH, '.git')
        if not os.path.exists(git_dir):
            return

        # Lista de arquivos/pastas comuns que travam o Git no Windows/OneDrive
        trash_list = [
            os.path.join(git_dir, 'rebase-merge'),
            os.path.join(git_dir, 'rebase-apply'),
            os.path.join(git_dir, 'index.lock')
        ]

        for item in trash_list:
            if os.path.exists(item):
                try:
                    if os.path.isdir(item):
                        shutil.rmtree(item, ignore_errors=True)
                    else:
                        os.remove(item)
                    print(f"🧹 Limpeza Anti-OneDrive: {os.path.basename(item)} removido.")
                except Exception as e:
                    print(f"⚠️ Aviso: Não consegui remover {item}. Feche o OneDrive e tente de novo.")

    def backup_procedures(self):
        """Conecta ao MySQL e extrai o código de todas as Procedures."""
        print("🗄️ Extraindo Procedures do banco de dados...")
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

    def check_secrets(self):
        """Verifica se há senhas expostas nos arquivos SQL gerados."""
        print("🛡️ Verificando segurança...")
        pattern = re.compile(r'password\s*=\s*[\'"].+[\'"]|root@localhost', re.IGNORECASE)
        
        for root, _, files in os.walk(self.sql_folder):
            for file in files:
                with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                    if pattern.search(f.read()): return False
        return True

    def update_readme(self, total_procs):
        """Atualiza o README com estatísticas do projeto."""
        readme_path = os.path.join(REPO_PATH, 'README.md')
        now = datetime.now().strftime('%d/%m/%Y %H:%M')
        content = f"# Dashboard de Inadimplência\n\n- **Procedures:** {total_procs}\n- **Sync:** {now}\n\n*Auto-update by OrionBot*"
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write(content)

    def execute_flow(self, commit_type="feat", message="sync procedures"):
        """Ciclo completo: Limpeza -> Backup -> README -> Force Push."""
        try:
            total = self.backup_procedures()
            
            if not self.check_secrets():
                print("❌ ERRO: Senhas detectadas nos scripts SQL!")
                return

            self.update_readme(total)

            # Sincronização Git
            if self.repo.is_dirty(untracked_files=True) or True: # Força o check
                self.repo.git.add(all=True)
                
                # Só faz o commit se houver mudanças reais
                if self.repo.index.diff("HEAD"):
                    full_msg = f"{commit_type}: {message} ({total} procs)"
                    self.repo.index.commit(full_msg)
                
                print(f"🚀 Enviando para o GitHub (Force Push)...")
                # O comando abaixo resolve o erro de 'failed to push'
                self.repo.git.push('origin', 'main', '--force')
                print("✅ Sucesso absoluto! Tudo no GitHub.")
            else:
                print("✨ Sem alterações para subir.")

        except Exception as e:
            print(f"💥 Falha no fluxo: {e}")

if __name__ == "__main__":
    bot = OrionGitPro()
    bot.execute_flow()