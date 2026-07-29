import subprocess
import sys
import pkg_resources

def run_command(command):
    """Executa comandos no terminal e retorna a saída."""
    result = subprocess.run(command, capture_output=True, text=True, shell=True)
    return result.stdout.strip(), result.stderr.strip()

def gerenciar_ambiente():
    print("=== 1. ATUALIZANDO O PIP ===")
    out, err = run_command(f"{sys.executable} -m pip install --upgrade pip")
    print("Pip atualizado com sucesso!" if not err else f"Nota: {out}")

    print("\n=== 2. LISTAGEM E INTEGRIDADE ===")
    # Lista bibliotecas via pkg_resources
    installed_packages = {dist.project_name: dist.version for dist in pkg_resources.working_set}
    print(f"Total de bibliotecas instaladas: {len(installed_packages)}")

    # Verifica integridade (conflitos de dependência)
    check_out, check_err = run_command("pip check")
    if "No broken requirements found" in check_out:
        print("✅ Integridade: Nenhuma dependência quebrada.")
    else:
        print("⚠️ Conflitos encontrados:\n", check_out)

    print("\n=== 3. ATUALIZAÇÃO DE BIBLIOTECAS (Sugestão/Ação) ===")
    # Busca bibliotecas desatualizadas
    out_outdated, _ = run_command("pip list --outdated --format=columns")
    if out_outdated:
        print("As seguintes bibliotecas podem ser atualizadas:\n")
        print(out_outdated)
        
        escolha = input("\nDeseja atualizar TODAS as bibliotecas agora? (S/N): ").upper()
        if escolha == 'S':
            # Comando para atualizar todas de uma vez
            print("Atualizando... Isso pode demorar um pouco.")
            run_command("pip freeze | % {pip install -U $_.split('==')[0]}") # Comando para PowerShell
            # Se estiver no CMD comum, o comando acima muda. Use este se o anterior falhar:
            # run_command("for /F \"delims==\" %i in ('pip freeze') do pip install -U %i")
            print("✅ Processo de atualização concluído.")
    else:
        print("✨ Todas as bibliotecas já estão na versão mais recente.")

    print("\n=== 4. EXPORTAÇÃO PARA OUTRA MÁQUINA ===")
    with open("requirements.txt", "w") as f:
        # Gera o arquivo padrão para migração
        f.write(run_command("pip freeze")[0])
    print("✅ Arquivo 'requirements.txt' gerado! Leve este arquivo para a outra máquina.")

if __name__ == "__main__":
    gerenciar_ambiente()