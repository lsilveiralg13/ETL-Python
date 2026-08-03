import os
import sys
import datetime
from pathlib import Path
import importlib.metadata
from typing import Dict, List, Tuple


class DependencyManager:
    """
    Gerenciador robusto para mapear, encapsular e exportar dependências
    do ambiente Python para arquivos na raiz do projeto.
    """

    def __init__(self, output_dir: Path = None):
        # Define a raiz como o diretório onde este script está localizado
        self.root_dir = output_dir or Path(__file__).resolve().parent

    def get_installed_packages(self) -> List[Tuple[str, str]]:
        """
        Mapeia todas as bibliotecas instaladas no ambiente atual e suas versões.
        Retorna uma lista ordenada de tuplas: (nome_do_pacote, versão).
        """
        packages = []
        # Utiliza a API oficial de metadados do Python (substitui o pkg_resources legado)
        for dist in importlib.metadata.distributions():
            name = dist.metadata["Name"]
            version = dist.version
            if name and version:
                packages.append((name, version))
        
        # Ordena alfabeticamente ignorando maiúsculas/minúsculas
        return sorted(packages, key=lambda x: x[0].lower())

    def save_requirements_txt(self, packages: List[Tuple[str, str]], filename: str = "requirements.txt") -> Path:
        """
        Salva as dependências no formato padrão de requisitos (pip format).
        Perfeito para ser lido por rotinas de CI/CD e commit diário.
        """
        file_path = self.root_dir / filename
        
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                for name, version in packages:
                    f.write(f"{name}=={version}\n")
            print(f"✅ Arquivo de requisitos gerado em: {file_path}")
            return file_path
        except Exception as e:
            print(f"❌ Erro ao salvar {filename}: {str(e)}")
            raise

    def save_dependencies_md(self, packages: List[Tuple[str, str]], filename: str = "DEPENDENCIES.md") -> Path:
        """
        Salva as dependências em um arquivo Markdown estruturado e visual,
        pronto para documentação de repositório (Git).
        """
        file_path = self.root_dir / filename
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        md_content = [
            "# 📦 Mapeamento de Dependências do Projeto\n",
            f"> **Última Atualização:** `{now}`  ",
            f"> **Versão do Python:** `{sys.version.split()[0]}`  ",
            f"> **Total de Pacotes:** `{len(packages)}`  \n",
            "---\n",
            "## 📋 Lista de Bibliotecas Instaladas\n",
            "| Biblioteca | Versão Instalada | Status |",
            "| :--- | :---: | :---: |"
        ]

        for name, version in packages:
            md_content.append(f"| **`{name}`** | `{version}` | `Ativo` |")

        md_content.append("\n---\n")
        md_content.append("*Arquivo gerado automaticamente pela rotina de automação Python.*")

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(md_content))
            print(f"✅ Documentação Markdown gerada em: {file_path}")
            return file_path
        except Exception as e:
            print(f"❌ Erro ao salvar {filename}: {str(e)}")
            raise

    def run_export_pipeline(self) -> Dict[str, str]:
        """
        Executa a esteira completa de extração e salvamento de arquivos.
        """
        print("🔍 Mapeando pacotes instalados no ambiente...")
        packages = self.get_installed_packages()

        req_path = self.save_requirements_txt(packages)
        md_path = self.save_dependencies_md(packages)

        return {
            "requirements_txt": str(req_path),
            "dependencies_md": str(md_path),
            "total_packages": len(packages)
        }


# --- EXECUÇÃO E INTEGRAÇÃO ---
if __name__ == "__main__":
    # Instancia o gerenciador
    manager = DependencyManager()
    
    # Executa a rotina
    summary = manager.run_export_pipeline()
    
    print("\n--- RESUMO DA EXECUÇÃO ---")
    print(f"📦 Pacotes Mapeados: {summary['total_packages']}")
    print(f"📄 Requisitos: {summary['requirements_txt']}")
    print(f"📝 Documentação: {summary['dependencies_md']}")
    print("🚀 Arquivos prontos na raiz para o seu script de commit em massa!")