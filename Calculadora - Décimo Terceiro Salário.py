# ============================================
# 🧠 Cálculo completo do 13º salário (Brasil)
# Inclui:
# - 13º bruto proporcional
# - 1ª parcela
# - INSS (tabela oficial)
# - IRRF (base e deduções)
# - 2ª parcela com descontos
# ============================================

def calcular_inss(valor):
    """
    Cálculo oficial INSS progressivo.
    Valores de 2024 (podemos atualizar para 2025).
    """
    faixas = [
        (1412.00, 0.075),
        (2666.68, 0.09),
        (4000.03, 0.12),
        (7786.02, 0.14),
    ]
    
    inss = 0
    restante = valor

    for limite, aliquota in faixas:
        faixa = min(restante, limite)
        inss += faixa * aliquota
        restante -= faixa
        if restante <= 0:
            break

    # Teto de desconto
    return min(inss, 908.85)


def calcular_irrf(base):
    """
    Tabela IRRF 2024.
    Base = valor_bruto - INSS (no caso do 13º)
    """
    if base <= 2259.20:
        return 0
    elif base <= 2826.65:
        return base * 0.075 - 169.44
    elif base <= 3751.05:
        return base * 0.15 - 381.44
    elif base <= 4664.68:
        return base * 0.225 - 662.77
    else:
        return base * 0.275 - 896.00


def decimo_terceiro(salario, meses_trabalhados):
    # Valor bruto proporcional
    bruto = (salario / 12) * meses_trabalhados

    # 1ª parcela → 50% sem descontos
    primeira = bruto / 2

    # INSS é calculado sobre o valor bruto total
    inss = calcular_inss(bruto)

    # Base do IRRF no 13º: bruto - INSS
    base_irrf = bruto - inss
    irrf = calcular_irrf(base_irrf)

    # 2ª parcela = bruto - primeira - descontos
    segunda = bruto - primeira - inss - irrf

    return {
        "bruto": bruto,
        "primeira_parcela": primeira,
        "inss": inss,
        "base_irrf": base_irrf,
        "irrf": irrf,
        "segunda_parcela": segunda,
        "total_receber": primeira + segunda
    }


# ============================================
# 📌 Exemplo de uso
# ============================================

salario = float(input("Informe o salário bruto mensal: R$ "))
meses = int(input("Informe os meses trabalhados no ano (1 a 12): "))

resultado = decimo_terceiro(salario, meses)

print("\n========== RESULTADO DO 13º ==========")
print(f"Bruto total........: R$ {resultado['bruto']:.2f}")
print(f"1ª parcela.........: R$ {resultado['primeira_parcela']:.2f}")
print(f"INSS...............: R$ {resultado['inss']:.2f}")
print(f"Base IRRF..........: R$ {resultado['base_irrf']:.2f}")
print(f"IRRF...............: R$ {resultado['irrf']:.2f}")
print(f"2ª parcela.........: R$ {resultado['segunda_parcela']:.2f}")
print("---------------------------------------")
print(f"Total a receber....: R$ {resultado['total_receber']:.2f}")
print("=======================================\n")
