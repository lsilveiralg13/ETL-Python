import datetime
from dateutil.relativedelta import relativedelta

def calcular_rescisao_final(
    salario_bruto: float,
    data_admissao: str,
    data_rescisao: str,
    tipo_desligamento: str,
    status_aviso: str,
    ferias_vencidas_meses: float
):
    try:
        dt_adm = datetime.datetime.strptime(data_admissao, '%d-%m-%Y').date()
        dt_res = datetime.datetime.strptime(data_rescisao, '%d-%m-%Y').date()
    except ValueError:
        return {"erro": "Formato de data inválido. Use 'DD-MM-AAAA'."}

    # --- 1. PROVENTOS ---
    saldo_salario = (salario_bruto / 30) * dt_res.day

    # 13º Proporcional
    inicio_ano = datetime.date(dt_res.year, 1, 1)
    base_13 = max(dt_adm, inicio_ano)
    delta_13 = relativedelta(dt_res, base_13)
    avos_13 = delta_13.months + (1 if dt_res.day >= 15 else 0)
    decimo_terceiro = (salario_bruto / 12) * avos_13

    # Férias Proporcionais + 1/3
    aniv_contrato = dt_adm.replace(year=dt_res.year)
    if aniv_contrato > dt_res:
        aniv_contrato = dt_adm.replace(year=dt_res.year - 1)
    delta_f = relativedelta(dt_res, aniv_contrato)
    avos_f = delta_f.months + (1 if dt_res.day >= 15 else 0)
    ferias_prop = ((salario_bruto / 12) * avos_f) * (4/3)

    # Férias Vencidas + 1/3
    ferias_venc_valor = (salario_bruto * ferias_vencidas_meses) * (4/3)

    # Aviso Prévio Indenizado
    aviso_provento = salario_bruto if status_aviso == 'indenizado' else 0

    total_bruto = saldo_salario + decimo_terceiro + ferias_prop + ferias_venc_valor + aviso_provento

    # --- 2. DESCONTOS ---
    # Regra de Isenção IRRF < 4800
    irrf = 0
    if salario_bruto >= 4800:
        irrf = (total_bruto * 0.15) # Alíquota média simplificada para o exemplo

    # Aviso Prévio Descontado
    aviso_desconto = salario_bruto if status_aviso == 'descontado' else 0

    # REGRA DOS 35% (Teto máximo de desconto permitido por lei sobre as verbas rescisórias)
    teto_consignado_35 = total_bruto * 0.35

    # --- 3. RESULTADO LÍQUIDO (Considerando o desconto máximo do empréstimo) ---
    total_liquido = total_bruto - irrf - aviso_desconto - teto_consignado_35

    return {
        "Saldo de Salário": saldo_salario,
        "13º Proporcional": decimo_terceiro,
        "Férias Totais (+1/3)": ferias_prop + ferias_venc_valor,
        "Aviso Prévio (Recebido)": aviso_provento,
        "---": 0, # Separador visual
        "TOTAL BRUTO": total_bruto,
        "(-) IRRF (Isento < 4800)": irrf,
        "(-) Aviso Prévio (Descontado)": aviso_desconto,
        "(-) TETO EMPRÉSTIMO (35%)": teto_consignado_35,
        "--- ": 0, # Separador visual
        "TOTAL LÍQUIDO ESTIMADO": max(0, total_liquido)
    }

if __name__ == "__main__":
    print("\n" + "="*50)
    print(" CALCULADORA DE RESCISÃO - MARGEM 35% ")
    print("="*50)
    
    sal = float(input("Salário Mensal Bruto: R$ "))
    adm = input("Data Admissão (DD-MM-AAAA): ")
    res = input("Data Rescisão (DD-MM-AAAA): ")
    
    print("\nTipo de Desligamento:\n[1] Sem Justa Causa\n[2] Com Justa Causa\n[3] Pedido de Demissão")
    tipo_op = input("Escolha: ")
    tipo_final = {"1": "sem justa causa", "2": "com justa causa", "3": "pedido de demissão"}.get(tipo_op, "1")

    status_aviso = "trabalhado"
    if tipo_final != "com justa causa":
        print("\nStatus do Aviso Prévio:\n[1] Trabalhado\n[2] Indenizado\n[3] Descontado")
        av_op = input("Escolha: ")
        status_aviso = "trabalhado" if av_op == "1" else "indenizado" if av_op == "2" else "descontado"

    f_venc = float(input("\nMeses de férias vencidas: "))

    resultado = calcular_rescisao_final(sal, adm, res, tipo_final, status_aviso, f_venc)

    print("\n" + "-"*50)
    print(f" RESUMO FINAL - {tipo_final.upper()} ")
    print("-"*50)
    for k, v in resultado.items():
        if k.startswith("---"):
            print("-" * 50)
            continue
        print(f"{k:.<38} R$ {v:>10.2f}")
    print("-"*50)
    print("Obs: O valor líquido considera o desconto máximo de 35% permitido por lei.")