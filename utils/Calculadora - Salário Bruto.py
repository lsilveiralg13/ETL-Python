import datetime

def calcular_salario_bruto(
    salario_bruto: float,
    desconta_inss: bool,
    optante_vt: bool,
    valor_ticket_diario: float,
    dias_uteis_mes: int
):
    """
    Calcula o salário bruto, INSS, vale-transporte e vale-refeição.

    Args:
        salario_bruto (float): O valor do salário bruto.
        desconta_inss (bool): True para descontar INSS, False caso contrário.
        optante_vt (bool): True para descontar vale-transporte, False caso contrário.
        valor_ticket_diario (float): Valor diário do vale-refeição.
        dias_uteis_mes (int): Quantidade de dias úteis no mês.

    Returns:
        dict: Um dicionário com os valores calculados do salário.
    """
    
    # Validação de entradas
    if salario_bruto < 0 or valor_ticket_diario < 0 or dias_uteis_mes < 0:
        return {"erro": "Todos os valores devem ser números positivos."}

    # Desconto do INSS
    desconto_inss = 0
    if desconta_inss:
        if salario_bruto <= 1412.00:
            desconto_inss = salario_bruto * 0.075
        elif salario_bruto <= 2666.68:
            desconto_inss = salario_bruto * 0.09
        elif salario_bruto <= 4000.03:
            desconto_inss = salario_bruto * 0.12
        elif salario_bruto <= 7786.02:
            desconto_inss = salario_bruto * 0.14
        else:
            desconto_inss = 7786.02 * 0.14  # Teto do INSS

    # Desconto do Vale-Transporte
    desconto_vt = 0
    if optante_vt:
        desconto_vt = salario_bruto * 0.06
    
    # Desconto do Vale-Refeição
    desconto_vr = (valor_ticket_diario * dias_uteis_mes) * 0.20
    
    # Cálculo do salário líquido
    salario_liquido = salario_bruto - desconto_inss - desconto_vt - desconto_vr

    return {
        "salario_bruto": round(salario_bruto, 2),
        "desconto_inss": round(desconto_inss, 2),
        "desconto_vale_transporte": round(desconto_vt, 2),
        "desconto_vale_refeicao": round(desconto_vr, 2),
        "salario_liquido": round(salario_liquido, 2)
    }

# --- Início da interface do terminal ---
if __name__ == "__main__":
    print("--- Calculadora de Salário Bruto e Líquido ---")
    try:
        salario_bruto = float(input("Digite o salário bruto: "))
        desconta_inss = input("Houve desconto de INSS? (sim/não): ").lower() == 'sim'
        optante_vt = input("É optante pelo vale-transporte? (sim/não): ").lower() == 'sim'
        valor_ticket_diario = float(input("Digite o valor diário do ticket/vale-refeição: "))
        dias_uteis_mes = int(input("Digite a quantidade de dias úteis no mês: "))

        resultado = calcular_salario_bruto(
            salario_bruto,
            desconta_inss,
            optante_vt,
            valor_ticket_diario,
            dias_uteis_mes
        )

        print("\n--- Detalhes do Salário ---")
        if "erro" in resultado:
            print(resultado["erro"])
        else:
            print(f"Salário Bruto: R$ {resultado['salario_bruto']:.2f}")
            print(f"Desconto INSS: R$ {resultado['desconto_inss']:.2f}")
            print(f"Desconto Vale-Transporte: R$ {resultado['desconto_vale_transporte']:.2f}")
            print(f"Desconto Vale-Refeição: R$ {resultado['desconto_vale_refeicao']:.2f}")
            print("---")
            print(f"Salário Líquido: R$ {resultado['salario_liquido']:.2f}")

    except ValueError:
        print("Entrada inválida. Por favor, digite números no formato correto.")

