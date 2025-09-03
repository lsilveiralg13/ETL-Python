import datetime

def calcular_rescisao(
    salario_bruto: float,
    data_admissao: str,
    data_rescisao: str,
    tipo_desligamento: str,
    aviso_previo_indenizado: bool = False,
    ferias_vencidas: float = 0,
    valor_emprestimo: float = 0,
    parcelas_totais: int = 0,
    parcelas_pagas: int = 0
):
    """
    Calcula os principais valores de uma rescisão de contrato de trabalho.

    Args:
        salario_bruto (float): O salário bruto do funcionário.
        data_admissao (str): Data de admissão no formato 'DD-MM-AAAA'.
        data_rescisao (str): Data da rescisão no formato 'DD-MM-AAAA'.
        tipo_desligamento (str): Tipo de desligamento ('sem justa causa', 'com justa causa', 'pedido de demissão').
        aviso_previo_indenizado (bool): True se o aviso prévio for indenizado, False caso contrário.
        ferias_vencidas (float): Número de meses de férias vencidas a serem pagas.
        valor_emprestimo (float): Valor total do empréstimo consignado.
        parcelas_totais (int): Quantidade total de parcelas do empréstimo.
        parcelas_pagas (int): Quantidade de parcelas do empréstimo já pagas.

    Returns:
        dict: Um dicionário com os valores calculados da rescisão.
    """

    try:
        data_admissao = datetime.datetime.strptime(data_admissao, '%d-%m-%Y').date()
        data_rescisao = datetime.datetime.strptime(data_rescisao, '%d-%m-%Y').date()
    except ValueError:
        return {"erro": "Formato de data inválido. Use 'DD-MM-AAAA'."}

    # Saldo de Salário
    dias_trabalhados_no_mes = (data_rescisao - data_rescisao.replace(day=1)).days + 1
    saldo_salario = (salario_bruto / 30) * dias_trabalhados_no_mes

    # Contagem de meses de trabalho para 13º e férias
    dias_trabalhados = (data_rescisao - data_admissao).days + 1
    meses_trabalhados = (dias_trabalhados // 30)
    
    # Inicialização de valores
    decimo_terceiro_proporcional = 0
    ferias_proporcionais = 0
    ferias_vencidas_total = 0
    aviso_previo_valor = 0
    
    # Lógica baseada no tipo de desligamento
    if tipo_desligamento == 'sem justa causa':
        # 13º Salário Proporcional
        decimo_terceiro_proporcional = (salario_bruto / 12) * meses_trabalhados
        # Férias Proporcionais
        ferias_proporcionais_valor = (salario_bruto / 12) * meses_trabalhados
        um_terco_ferias_proporcionais = ferias_proporcionais_valor / 3
        ferias_proporcionais = ferias_proporcionais_valor + um_terco_ferias_proporcionais
        # Férias Vencidas
        ferias_vencidas_valor = (salario_bruto / 12) * ferias_vencidas * 12
        um_terco_ferias_vencidas = ferias_vencidas_valor / 3
        ferias_vencidas_total = ferias_vencidas_valor + um_terco_ferias_vencidas
        # Aviso Prévio Indenizado
        if aviso_previo_indenizado:
            aviso_previo_valor = salario_bruto
    
    elif tipo_desligamento == 'pedido de demissão':
        # 13º Salário Proporcional
        decimo_terceiro_proporcional = (salario_bruto / 12) * meses_trabalhados
        # Férias Proporcionais
        ferias_proporcionais_valor = (salario_bruto / 12) * meses_trabalhados
        um_terco_ferias_proporcionais = ferias_proporcionais_valor / 3
        ferias_proporcionais = ferias_proporcionais_valor + um_terco_ferias_proporcionais
        # Férias Vencidas
        ferias_vencidas_valor = (salario_bruto / 12) * ferias_vencidas * 12
        um_terco_ferias_vencidas = ferias_vencidas_valor / 3
        ferias_vencidas_total = ferias_vencidas_valor + um_terco_ferias_vencidas

    elif tipo_desligamento == 'com justa causa':
        # Saldo de salário e férias vencidas
        ferias_vencidas_valor = (salario_bruto / 12) * ferias_vencidas * 12
        um_terco_ferias_vencidas = ferias_vencidas_valor / 3
        ferias_vencidas_total = ferias_vencidas_valor + um_terco_ferias_vencidas

    # Empréstimo Consignado
    emprestimo_a_abater = 0
    if valor_emprestimo > 0 and parcelas_totais > parcelas_pagas:
        valor_parcela = valor_emprestimo / parcelas_totais
        parcelas_restantes = parcelas_totais - parcelas_pagas
        emprestimo_a_abater = valor_parcela * parcelas_restantes

    # Total Bruto da Rescisão
    total_bruto = saldo_salario + decimo_terceiro_proporcional + ferias_proporcionais + ferias_vencidas_total + aviso_previo_valor
    
    # Total Líquido da Rescisão (subtraindo o empréstimo)
    total_liquido = total_bruto - emprestimo_a_abater

    return {
        "saldo_salario": round(saldo_salario, 2),
        "decimo_terceiro_proporcional": round(decimo_terceiro_proporcional, 2),
        "ferias_proporcionais": round(ferias_proporcionais, 2),
        "ferias_vencidas": round(ferias_vencidas_total, 2),
        "aviso_previo_indenizado": round(aviso_previo_valor, 2),
        "emprestimo_a_abater": round(emprestimo_a_abater, 2),
        "total_bruto": round(total_bruto, 2),
        "total_liquido": round(total_liquido, 2)
    }

# --- Início da interface do terminal ---
if __name__ == "__main__":
    print("--- Calculadora de Rescisão ---")
    try:
        salario = float(input("Digite o salário bruto: "))
        data_admissao = input("Digite a data de admissão (DD-MM-AAAA): ")
        data_rescisao = input("Digite a data da rescisão (DD-MM-AAAA): ")
        
        tipos_validos = ['sem justa causa', 'com justa causa', 'pedido de demissão']
        tipo_desligamento = ''
        while tipo_desligamento not in tipos_validos:
            tipo_desligamento = input(f"Digite o tipo de desligamento {tipos_validos}: ").lower()
            if tipo_desligamento not in tipos_validos:
                print("Tipo de desligamento inválido. Por favor, escolha uma das opções.")

        aviso_previo_indenizado = False
        if tipo_desligamento == 'sem justa causa':
            aviso_previo_str = input("Houve aviso prévio indenizado? (sim/não): ").lower()
            aviso_previo_indenizado = aviso_previo_str == 'sim'
        
        ferias_vencidas = int(input("Digite o número de meses de férias vencidas a serem pagas: "))
        
        # Coletando informações sobre o empréstimo consignado
        tem_emprestimo = input("Houve empréstimo consignado a ser abatido? (sim/não): ").lower()
        valor_emprestimo = 0
        parcelas_totais = 0
        parcelas_pagas = 0
        if tem_emprestimo == 'sim':
            valor_emprestimo = float(input("Digite o valor total do empréstimo: "))
            parcelas_totais = int(input("Digite o número total de parcelas do empréstimo: "))
            parcelas_pagas = int(input("Digite o número de parcelas já pagas: "))

        rescisao = calcular_rescisao(
            salario,
            data_admissao,
            data_rescisao,
            tipo_desligamento,
            aviso_previo_indenizado,
            ferias_vencidas,
            valor_emprestimo,
            parcelas_totais,
            parcelas_pagas
        )

        print("\n--- Detalhes da Rescisão ---")
        if "erro" in rescisao:
            print(rescisao["erro"])
        else:
            print(f"Tipo de desligamento: {tipo_desligamento.title()}")
            for item, valor in rescisao.items():
                print(f"{item.replace('_', ' ').title()}: R$ {valor:.2f}")

    except ValueError:
        print("Entrada inválida. Por favor, digite números e datas no formato correto.")
