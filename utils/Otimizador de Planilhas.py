import re
from pathlib import Path
import shutil
import xlwings as xw

# ====== CONFIGURAÇÕES ======
CAMINHO_ARQUIVO = r"C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\Scripts Python\Controle de Agendamentos - Showroom Inverno 2026.xlsx"  # <-- AJUSTE AQUI

NOME_ABA_DIAGNOSTICO = "Controle de Agendamentos - Showroom Inverno 2026 - Diagnósticov"

# Apenas essas abas serão analisadas:
ABAS_ANALISAR = [
    "BASE",
    "TABELA",
    "INADIMPLÊNCIA",
    "PARCEIROS MULTIMARCAS",
    "ERIKHA",
    "GLENDA",
    "ISABELLA",
    "JOSIANE",
    "LUCIANA",
    "MARCELA",
    "ROBERTA",
    "NOVOS CLIENTES",
    "ACOMPANHANTES",
    "PRIMEIRO PEDIDO",
    "FATURAMENTO HISTÓRICO",
]

# ====== PADRÕES (REGEX) ======

# Coluna inteira: $A:$A, A:A, 'BASE'!$S:$S, BASE!A:A, etc.
PADRAO_COLUNA_INTEIRA = re.compile(
    r"((?:'[^']+'!)?)\$?(?P<col>[A-Z]+):\$?(?P=col)",
    re.IGNORECASE
)

# Linha inteira: $1:$1, 1:1, 'BASE'!$3:$3, etc.
PADRAO_LINHA_INTEIRA = re.compile(
    r"((?:'[^']+'!)?)\$?(?P<row>\d+):\$?(?P=row)",
    re.IGNORECASE
)

# Funções voláteis/pesadas (pt-BR e en-US)
PADRAO_FUNCOES_VOLATEIS = re.compile(
    r"\b(HOJE|AGORA|DESLOC|INDIRETO|TODAY|NOW|OFFSET|INDIRECT)\b",
    re.IGNORECASE
)


def main():
    caminho_original = Path(CAMINHO_ARQUIVO)

    if not caminho_original.exists():
        print(f"Arquivo não encontrado: {caminho_original}")
        return

    # Backup do arquivo original (só por segurança)
    backup = caminho_original.with_suffix(".backup" + caminho_original.suffix)
    if not backup.exists():
        shutil.copy2(caminho_original, backup)
        print(f"Backup criado em: {backup}")

    # Novo arquivo com diagnóstico (mesma extensão do original)
    novo_caminho = caminho_original.with_name(
        caminho_original.stem + "_diagnostico" + caminho_original.suffix
    )

    # Abre o Excel via xlwings
    app = xw.App(visible=False)
    app.display_alerts = False
    app.screen_updating = False

    try:
        wb = app.books.open(str(caminho_original))

        diagnosticos = []

        print("Iniciando diagnóstico de fórmulas...")

        # Percorre apenas as abas indicadas
        for sh in wb.sheets:
            if sh.name not in ABAS_ANALISAR:
                continue

            print(f"  Analisando aba: {sh.name}")
            used = sh.used_range

            # Percorre todas as células usadas
            for cell in used:
                formula = cell.formula
                if not formula:
                    continue  # célula sem fórmula

                endereco = cell.get_address(row_absolute=False, column_absolute=False)

                # 1) Coluna inteira
                for match in PADRAO_COLUNA_INTEIRA.finditer(formula):
                    referencia = match.group(0)
                    diagnosticos.append({
                        "Planilha": sh.name,
                        "Celula": endereco,
                        "Tipo_Problema": "Coluna inteira",
                        "Detalhe": referencia,
                        "Formula": formula
                    })

                # 2) Linha inteira
                for match in PADRAO_LINHA_INTEIRA.finditer(formula):
                    referencia = match.group(0)
                    diagnosticos.append({
                        "Planilha": sh.name,
                        "Celula": endereco,
                        "Tipo_Problema": "Linha inteira",
                        "Detalhe": referencia,
                        "Formula": formula
                    })

                # 3) Funções voláteis / pesadas
                for match in PADRAO_FUNCOES_VOLATEIS.finditer(formula):
                    funcao = match.group(1)
                    diagnosticos.append({
                        "Planilha": sh.name,
                        "Celula": endereco,
                        "Tipo_Problema": "Função volátil/pesada",
                        "Detalhe": funcao.upper(),
                        "Formula": formula
                    })

        # Remove aba de diagnóstico antiga, se existir
        for sh in wb.sheets:
            if sh.name == NOME_ABA_DIAGNOSTICO:
                sh.delete()
                break

        # Cria nova aba de diagnóstico
        ws_diag = wb.sheets.add(NOME_ABA_DIAGNOSTICO, before=wb.sheets[0])

        # Cabeçalhos
        cabecalhos = ["Planilha", "Celula", "Tipo_Problema", "Detalhe", "Formula"]
        ws_diag.range("A1").value = cabecalhos

        # Corpo
        if diagnosticos:
            linhas = [
                [
                    d["Planilha"],
                    d["Celula"],
                    d["Tipo_Problema"],
                    d["Detalhe"],
                    d["Formula"]
                ]
                for d in diagnosticos
            ]
            ws_diag.range("A2").value = linhas
            ws_diag.autofit()
        else:
            ws_diag.range("A2").value = "Nenhum problema encontrado pelos padrões configurados."

        # Salva novo arquivo com diagnóstico
        wb.save(str(novo_caminho))
        wb.close()

        print()
        print(f"Diagnóstico concluído. Ocorrências encontradas: {len(diagnosticos)}")
        print(f"Arquivo com diagnóstico salvo em: {novo_caminho}")

    finally:
        app.display_alerts = True
        app.screen_updating = True
        app.quit()


if __name__ == "__main__":
    main()
