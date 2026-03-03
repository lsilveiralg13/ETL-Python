import datetime

def calcular_rescisao(
    salario_bruto: float,
    data_admissao: str,
    data_rescisao: str,
    tipo_desligamento: str,
    aviso_previo_indenizado: bool = False,
    ferias_vencidas: float = 0,
    emprestimos: list = None # AGORA RECEBE UMA LISTA DE DICTs
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
        emprestimos (list): Lista de dicionários, onde cada dicionário representa um empréstimo:
                            [{'valor_total': float, 'parcelas_totais': int, 'parcelas_pagas': int}, ...]

    Returns:
        dict: Um dicionário com os valores calculados da rescisão.
    """

    if emprestimos is None:
        emprestimos = []

    try:
        data_admissao = datetime.datetime.strptime(data_admissao, '%d-%m-%Y').date()
        data_rescisao = datetime.datetime.strptime(data_rescisao, '%d-%m-%Y').date()
    except ValueError:
        return {"erro": "Formato de data inválido. Use 'DD-MM-AAAA'."}

    # Saldo de Salário
    # Considera-se o mês como 30 dias para o cálculo do salário dia a dia.
    dias_trabalhados_no_mes = data_rescisao.day
    saldo_salario = (salario_bruto / 30) * dias_trabalhados_no_mes

    # Contagem de meses de trabalho para 13º e férias
    # A contagem de meses de 15 dias ou mais em um mês conta como mês completo (Avos)
    # Aqui, simplificamos contando o número de meses cheios completos desde a admissão.
    from dateutil.relativedelta import relativedelta
    data_fim_referencia = data_rescisao
    
    # Cálculo do avo de 13º salário (mês trabalhado ou fração igual/superior a 15 dias)
    # Simplificando a lógica de 'meses_trabalhados' para 'avos' de 13º/férias.
    # Esta é uma simplificação, a lógica real de avos é mais complexa e exige a contagem de dias exata no mês de rescisão.
    # Aqui, faremos a contagem de meses completos + avo (se o último mês tiver 15 dias ou mais).
    data_referencia_13 = data_admissao.replace(month=1, day=1) if data_admissao.month != 1 else data_admissao

    delta = relativedelta(data_rescisao, data_referencia_13)
    meses_trabalhados = delta.years * 12 + delta.months + (1 if data_rescisao.day >= 15 and data_rescisao.day < 30 else 0)


    # Inicialização de valores
    decimo_terceiro_proporcional = 0
    ferias_proporcionais_total = 0
    ferias_vencidas_total = 0
    aviso_previo_valor = 0
    
    # Lógica baseada no tipo de desligamento
    # O cálculo de férias e 13º por avos é o mesmo para sem justa causa e pedido de demissão.
    if tipo_desligamento in ['sem justa causa', 'pedido de demissão']:
        # 13º Salário Proporcional (Avos)
        decimo_terceiro_proporcional = (salario_bruto / 12) * meses_trabalhados
        
        # Férias Proporcionais (Avos) + 1/3 Constitucional
        ferias_proporcionais_valor = (salario_bruto / 12) * meses_trabalhados
        um_terco_ferias_proporcionais = ferias_proporcionais_valor / 3
        ferias_proporcionais_total = ferias_proporcionais_valor + um_terco_ferias_proporcionais
        
        # Férias Vencidas (valor total de meses vencidos + 1/3)
        ferias_vencidas_valor = salario_bruto * ferias_vencidas
        um_terco_ferias_vencidas = ferias_vencidas_valor / 3
        ferias_vencidas_total = ferias_vencidas_valor + um_terco_ferias_vencidas
        
        if tipo_desligamento == 'sem justa causa' and aviso_previo_indenizado:
            # Aviso Prévio Indenizado (Simplificado para 1 salário base, sem projetar o tempo de serviço)
            aviso_previo_valor = salario_bruto
    
    elif tipo_desligamento == 'com justa causa':
        # Com justa causa, o funcionário só recebe Saldo de Salário e Férias Vencidas + 1/3
        ferias_vencidas_valor = salario_bruto * ferias_vencidas
        um_terco_ferias_vencidas = ferias_vencidas_valor / 3
        ferias_vencidas_total = ferias_vencidas_valor + um_terco_ferias_vencidas
        # Não recebe 13º Proporcional nem Férias Proporcionais.
        
    # --- Cálculo do Abatimento dos Empréstimos Consignados ---
    emprestimo_total_a_abater = 0
    for emp in emprestimos:
        valor_total = emp.get('valor_total', 0)
        parcelas_totais = emp.get('parcelas_totais', 0)
        parcelas_pagas = emp.get('parcelas_pagas', 0)
        
        if valor_total > 0 and parcelas_totais > 0 and parcelas_totais > parcelas_pagas:
            # Simplificação: Abate o saldo devedor restante
            valor_parcela = valor_total / parcelas_totais
            parcelas_restantes = parcelas_totais - parcelas_pagas
            emprestimo_total_a_abater += valor_parcela * parcelas_restantes

    # Total Bruto da Rescisão
    total_bruto = saldo_salario + decimo_terceiro_proporcional + ferias_proporcionais_total + ferias_vencidas_total + aviso_previo_valor
    
    # Total Líquido da Rescisão (subtraindo o empréstimo)
    total_liquido = total_bruto - emprestimo_total_a_abater

    return {
        "saldo_salario": round(saldo_salario, 2),
        "decimo_terceiro_proporcional": round(decimo_terceiro_proporcional, 2),
        "ferias_proporcionais": round(ferias_proporcionais_total, 2),
        "ferias_vencidas": round(ferias_vencidas_total, 2),
        "aviso_previo_indenizado": round(aviso_previo_valor, 2),
        "emprestimo_total_a_abater": round(emprestimo_total_a_abater, 2),
        "total_bruto": round(total_bruto, 2),
        "total_liquido": round(total_liquido, 2)
    }

# --- Início da interface do terminal ---
if __name__ == "__main__":
    from dateutil.relativedelta import relativedelta # Necessário para a lógica de avos

    print("--- Calculadora de Rescisão CLT ---")
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
        
        # --- NOVO BLOCO: Coletando Múltiplos Empréstimos Consignados ---
        
        emprestimos_list = []
        try:
            num_emprestimos = int(input("Quantos empréstimos consignados CLT foram realizados? "))
        except ValueError:
            num_emprestimos = 0
            
        for i in range(num_emprestimos):
            print(f"\n--- Detalhes do Empréstimo {i + 1} ---")
            
            valor_emprestimo = float(input("Qual é o valor total do empréstimo? R$ "))
            parcelas_totais = int(input("Qual é o número total de parcelas? "))
            parcelas_pagas = int(input("Quantas parcelas foram pagas? "))
            
            emprestimos_list.append({
                'valor_total': valor_emprestimo,
                'parcelas_totais': parcelas_totais,
                'parcelas_pagas': parcelas_pagas
            })

        # --- FIM DO NOVO BLOCO ---

        rescisao = calcular_rescisao(
            salario,
            data_admissao,
            data_rescisao,
            tipo_desligamento,
            aviso_previo_indenizado,
            ferias_vencidas,
            emprestimos_list
        )

        print("\n--- Detalhes da Rescisão ---")
        if "erro" in rescisao:
            print(rescisao["erro"])
        else:
            print(f"Tipo de desligamento: {tipo_desligamento.title()}")
            for item, valor in rescisao.items():
                print(f"{item.replace('_', ' ').title()}: R$ {valor:.2f}")

    except ValueError as e:
        print(f"Entrada inválida. Por favor, digite números e datas no formato correto. Erro: {e}")