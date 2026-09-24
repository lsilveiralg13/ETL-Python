import os
import sys
import pkg_resources

def reescrever_requirements_raiz():
    # Identifica o caminho absoluto da pasta raiz do projeto
    diretorio_raiz = os.path.dirname(os.path.abspath(__file__))
    caminho_requirements = os.path.join(diretorio_raiz, "requirements.txt")

    # Mapeia todas as bibliotecas instaladas no ambiente Python ativo
    instaladas = sorted([d.project_name for d in pkg_resources.working_set])

    # Modulos padrao do Python e utilitarios que NAO devem ir para o requirements.txt
    nativas_std = {
        "python", "pip", "setuptools", "wheel", "argparse", "asyncio",
        "json", "os", "sys", "re", "time", "datetime", "io", "math",
        "concurrent", "typing", "functools"
    }

    # Filtra mantendo apenas pacotes de terceiros
    deps_filtradas = [pkg for pkg in instaladas if pkg.lower() not in nativas_std]

    # Reescreve o requirements.txt na raiz de forma flexivel (compativel com qualquer maquina/nuvem)
    with open(caminho_requirements, "w", encoding="utf-8") as f:
        f.write("# Dependencias da plataforma Vetra geradas automaticamente\n")
        f.write("# Formato flexivel para compatibilidade multi-SO e Streamlit Cloud\n\n")
        for dep in deps_filtradas:
            f.write(f"{dep}\n")

    print(f"✅ {len(deps_filtradas)} bibliotecas salvas com sucesso em:\n   {caminho_requirements}")

if __name__ == "__main__":
    reescrever_requirements_raiz()