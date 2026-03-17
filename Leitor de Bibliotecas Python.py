import os
import ast

pasta_projeto = "."

bibliotecas = set()

# bibliotecas nativas do python (principais)
stdlib = {
    "os","sys","math","time","datetime","json","re","subprocess",
    "collections","itertools","functools","pathlib","threading",
    "multiprocessing","asyncio","logging","random","statistics",
    "typing","unittest","sqlite3","hashlib","shutil","glob"
}

for raiz, dirs, arquivos in os.walk(pasta_projeto):

    if ".git" in raiz:
        continue

    for arquivo in arquivos:

        if arquivo.endswith(".py"):

            caminho = os.path.join(raiz, arquivo)

            try:
                with open(caminho, "r", encoding="utf-8") as f:
                    arvore = ast.parse(f.read())
            except:
                continue

            for node in ast.walk(arvore):

                if isinstance(node, ast.Import):

                    for n in node.names:
                        lib = n.name.split(".")[0]
                        if lib not in stdlib:
                            bibliotecas.add(lib)

                elif isinstance(node, ast.ImportFrom):

                    if node.module:
                        lib = node.module.split(".")[0]
                        if lib not in stdlib:
                            bibliotecas.add(lib)

# gerar requirements.txt
with open("requirements.txt", "w") as f:

    for lib in sorted(bibliotecas):
        f.write(lib + "\n")

print("\n✅ requirements.txt gerado com sucesso!\n")
print("Bibliotecas detectadas:\n")

for lib in sorted(bibliotecas):
    print(lib)