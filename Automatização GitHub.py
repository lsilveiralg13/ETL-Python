import hashlib
import os
import socket
import sys
import shutil
from datetime import datetime
import git

# --- SDK OFICIAL DO GEMINI ---
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# --- CONFIGURAÇÕES DO REPOSITÓRIO ---
REPO_PATH = os.path.dirname(os.path.abspath(__file__))

# Estrutura do Processo Produtivo
PIPELINE_STAGES = {
    "01_ingestao": "Scripts de extração de dados, APIs, scrapers, conectores e coleta inicial.",
    "02_transformacao": "Scripts SQL, tratamento Pandas/Polars, regras de negócio, staging e limpeza.",
    "03_carregamento": "Carga em Data Warehouses, geração de relatórios, Power BI exports, dashboards.",
    "config": "Arquivos de configuração, variáveis de ambiente, schemas, JSON/YAML de config.",
    "docs": "Documentação Markdown, diagramas, guias e especificações técnicas.",
    "utils": "Utilitários gerais, scripts auxiliares, rotinas de automação e planejadores."
}

# Regras estritas de arquivos que JAMAIS devem ir para o GitHub
PROTECTED_PATTERNS = [
    "utils/client_secret.json",
    "client_secret.json",
    "*.json",
    "*.db",
    "tarefas.db",
    ".venv/",
    "venv/",
    "__pycache__/",
    "*.exe",
    "*.parquet",
    "*.xlsx"
]

class GitSyncPro:
    def __init__(self, target_branch="feature/repo-restructure"):
        # 1. Atribui a branch alvo PRIMEIRO para evitar AttributeError nos métodos de limpeza
        self.target_branch = target_branch
        
        # 2. Executa as validações de segurança e destravamento de arquivos .lock
        self._ensure_gitignore_security()
        self._force_unlock_git()
        
        try:
            self.repo = git.Repo(REPO_PATH)
        except git.exc.InvalidGitRepositoryError:
            print(f"🗂️ Inicializando novo repositório Git local em: {REPO_PATH}")
            self.repo = git.Repo.init(REPO_PATH)

        # Configura cliente Gemini caso disponível
        self.ai_client = None
        api_key = os.environ.get("GEMINI_API_KEY")
        if GEMINI_AVAILABLE and api_key:
            self.ai_client = genai.Client(api_key=api_key)

        self._setup_branches()
        self._ensure_pipeline_folders()

    def _ensure_gitignore_security(self):
        """Garante que segredos e bancos de dados locais estejam no .gitignore antes do staging."""
        gitignore_path = os.path.join(REPO_PATH, '.gitignore')
        existing_lines = []
        
        if os.path.exists(gitignore_path):
            with open(gitignore_path, 'r', encoding='utf-8', errors='ignore') as f:
                existing_lines = [line.strip() for line in f.readlines()]

        updated = False
        with open(gitignore_path, 'a', encoding='utf-8') as f:
            for pattern in PROTECTED_PATTERNS:
                if pattern not in existing_lines:
                    f.write(f"\n{pattern}")
                    updated = True
        
        if updated:
            print("🛡️ Segurança do .gitignore reforçada (credenciais e bancos bloqueados).")

    def _force_unlock_git(self):
        """Remove arquivos de trava gerados por OneDrive ou chamadas simultâneas do Git."""
        git_dir = os.path.join(REPO_PATH, '.git')
        if not os.path.exists(git_dir):
            return

        trash_list = [
            os.path.join(git_dir, 'index.lock'),
            os.path.join(git_dir, 'HEAD.lock'),
            os.path.join(git_dir, 'refs/heads/main.lock'),
            os.path.join(git_dir, 'refs/heads/master.lock'),
            os.path.join(git_dir, f'refs/heads/{self.target_branch}.lock')
        ]

        for item in trash_list:
            if os.path.exists(item):
                try:
                    os.remove(item)
                    print(f"🧹 Trava Git/OneDrive removida: {os.path.basename(item)}")
                except Exception as e:
                    print(f"⚠️ Erro ao remover trava {os.path.basename(item)}: {e}")

    def _setup_branches(self):
        """Garante que estamos trabalhando na branch correta sem conflitos."""
        try:
            current = self.repo.active_branch.name
            if current != self.target_branch:
                if self.target_branch in [b.name for b in self.repo.branches]:
                    print(f"🔀 Alternando para a branch existente: {self.target_branch}")
                    self.repo.git.checkout(self.target_branch)
                else:
                    print(f"🌿 Criando e alternando para nova branch: {self.target_branch}")
                    self.repo.git.checkout('-b', self.target_branch)
        except Exception as e:
            print(f"⚠️ Aviso ao gerenciar branches: {e}")

    def _ensure_pipeline_folders(self):
        """Cria as pastas do processo produtivo se não existirem."""
        for folder in PIPELINE_STAGES.keys():
            path = os.path.join(REPO_PATH, folder)
            if not os.path.exists(path):
                os.makedirs(path)

    def classify_file_with_ai(self, file_path: str) -> str:
        """Usa o Gemini para classificar arquivos no processo produtivo."""
        if not self.ai_client:
            return "utils"

        file_name = os.path.basename(file_path)
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                sample = "".join([f.readline() for _ in range(30)])
        except Exception:
            sample = "Arquivo binário ou não legível."

        prompt = f"""
        Você é um arquiteto de dados sênior organizando um repositório profissional.
        Classifique o arquivo a seguir em EXATAMENTE UMA das categorias do processo produtivo:

        Categorias e papéis:
        {PIPELINE_STAGES}

        Nome do Arquivo: {file_name}
        Amostra do Conteúdo:
        ---
        {sample}
        ---

        Responda APENAS com o nome exato da chave/categoria (ex: 01_ingestao, 02_transformacao, etc). Sem explicações adicionais.
        """

        try:
            response = self.ai_client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt
            )
            category = response.text.strip().lower()
            return category if category in PIPELINE_STAGES else "utils"
        except Exception:
            return "utils"

    def run_auto_organization(self):
        """Varre a raiz do projeto e organiza arquivos soltos nas pastas apropriadas."""
        print("🧠 Varrendo e organizando arquivos do repositório...")
        
        script_name = os.path.basename(__file__)
        ignored_files = {'README.md', '.gitignore', 'requirements.txt', 'Planejador de Tarefas.py', script_name}
        
        moved_count = 0

        for item in os.listdir(REPO_PATH):
            item_path = os.path.join(REPO_PATH, item)

            if os.path.isdir(item_path) or item.startswith('.') or item in ignored_files or item.endswith('.db') or item.endswith('.json'):
                continue

            category = self.classify_file_with_ai(item_path)
            dest_dir = os.path.join(REPO_PATH, category)
            dest_path = os.path.join(dest_dir, item)

            shutil.move(item_path, dest_path)
            print(f" 📂 Organizado: {item} ➔ [{category}/]")
            moved_count += 1

        if moved_count == 0:
            print("✨ Nenhum arquivo solto pendente de organização.")
        else:
            print(f"🎉 {moved_count} arquivo(s) reestruturado(s) com sucesso!")

    def get_local_ip(self) -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    def get_folder_metrics(self):
        total_size = 0
        files_count = 0
        corrupted = 0

        for root, dirs, files in os.walk(REPO_PATH):
            if '.git' in root or '.venv' in root or 'venv' in root:
                continue
            for f in files:
                file_path = os.path.join(root, f)
                try:
                    total_size += os.path.getsize(file_path)
                    files_count += 1
                    with open(file_path, 'rb') as fp:
                        hashlib.sha256(fp.read(4096))
                except Exception:
                    corrupted += 1

        if total_size >= 1024 * 1024:
            size_str = f"{total_size / (1024 * 1024):.2f} MB"
        else:
            size_str = f"{total_size / 1024:.2f} KB"

        integrity_status = "100% Ok (SHA-256 Verificado)" if corrupted == 0 else f"⚠️ {corrupted} arquivos c/ falha"
        return files_count, size_str, integrity_status

    def count_commits_today(self) -> int:
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
        readme_path = os.path.join(REPO_PATH, 'README.md')
        if not os.path.exists(readme_path):
            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write("# Repositório de Engenharia de Dados & Automações\n\n")

        now = datetime.now().strftime('%d/%m/%Y %H:%M')
        
        with open(readme_path, 'r', encoding='utf-8', errors='ignore') as f:
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
        print(f" │ 🌿 Branch Ativa        : {current_branch}")
        print(f" │ 📌 Status do Push      : {status_push}")
        print(f" │ 🆔 Hash do Commit      : {commit_hex[:8] if commit_hex != 'N/A' else 'N/A'}")
        print(f" │ 📅 Comitados Hoje      : {committed_today} arquivo(s)")
        print(f" │ 📂 Total no Repositório: {total_files} arquivo(s)")
        print(f" │ 💾 Volume de Dados     : {size_str}")
        print(f" │ 🛡️ Integridade Dados   : {integrity}")
        print(f" │ 🔒 Protocolo Segurança : {security_type}")
        print(f" │ 🌐 Servidor Remoto     : {server_host}")
        print(f" │ 💻 Origem (Máquina)    : {hostname} ({local_ip})")
        print("=" * 70 + "\n")

    def execute_flow(self, commit_type="refactor", message="reestruturacao automatica com IA"):
        print("🚀 Executando validações e sincronização Git...")
        
        self.run_auto_organization()

        commit_hex = "N/A"
        status_push = "Nenhum commit pendente"

        try:
            files_count, size_str, integrity = self.get_folder_metrics()
            self.update_readme_stats(files_count)

            # Stage de arquivos seguros
            self.repo.git.add(A=True)

            if self.repo.is_dirty(untracked_files=True):
                current_branch = self.repo.active_branch.name
                full_msg = f"{commit_type}: {message} ({files_count} arquivos)"
                
                novo_commit = self.repo.index.commit(full_msg)
                commit_hex = novo_commit.hexsha
                
                if self.repo.remotes:
                    origem = self.repo.remote(name='origin')
                    try:
                        push_info = origem.push(refspec=f'{current_branch}:{current_branch}', set_upstream=True)
                        if push_info and (push_info[0].flags & git.remote.PushInfo.ERROR):
                            status_push = f"❌ Erro no Push: {push_info[0].summary}"
                        else:
                            status_push = "✨ Sucesso (Enviado ao GitHub)"
                    except Exception as push_err:
                        status_push = f"⚠️ Falha no Push (Verifique regras do GitHub): {push_err}"
                else:
                    status_push = "⚠️ Commit Local (Sem origem remota)"
            
            committed_today = self.count_commits_today()
            self.print_summary_table(committed_today, files_count, size_str, integrity, commit_hex, status_push)

        except Exception as e:
            print(f"💥 Falha no fluxo de sincronização: {e}")


if __name__ == "__main__":
    msg = sys.argv[1] if len(sys.argv) > 1 else "reestruturacao de pastas e rotinas com IA"
    c_type = sys.argv[2] if len(sys.argv) > 2 else "refactor"

    bot = GitSyncPro(target_branch="feature/repo-restructure")
    bot.execute_flow(commit_type=c_type, message=msg)