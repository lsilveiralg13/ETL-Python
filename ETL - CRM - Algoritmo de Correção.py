import xlwings as xw
from datetime import datetime, date, timedelta
from pathlib import Path

# =========================
# CONFIGURAÇÕES GERAIS
# =========================

ARQUIVO_CRM = Path("11 - CRM MM - Nov.25.xlsx")

SDR_ABAS = ["ANDRÉ", "DUDA", "RAIANE", "ROBERTA", "SCARLAT", "TARVYLLA"]

# Colunas fixas conforme o CRM
COL_DATA = "C"          # DATA
COL_NOME = "E"          # Nome do Cliente
COL_DATA_PROX = "U"     # Data do Próximo Contato
COL_STATUS_AGENDA = "V" # STATUS DA AGENDA
COL_SLA = "W"           # SLA ATENDIMENTO
COL_STATUS_LEAD = "X"   # STATUS DO LEAD
COL_PRIORIDADE = "Y"    # PRIORIDADE (nova)
COL_MOTIVO = "Z"        # MOTIVO DO STATUS (nova)


# =========================
# FUNÇÕES AUXILIARES
# =========================

def to_date(value):
    """Converte valor de célula para date, se possível."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        # Em geral xlwings já retorna datetime, mas deixo proteção
        try:
            base = date(1899, 12, 30)
            return base + timedelta(days=int(value))
        except Exception:
            return None
    if isinstance(value, str):
        txt = value.strip()
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(txt, fmt).date()
            except Exception:
                pass
    return None


def normalizar_texto(valor):
    if valor is None:
        return ""
    return str(valor).strip().upper()


def obter_ultima_linha(ws):
    """Retorna última linha usada na aba."""
    try:
        return ws.used_range.last_cell.row
    except Exception:
        return ws.range("A" + str(ws.cells.last_cell.row)).end("up").row


def garantir_colunas_prioridade_motivo(ws):
    """
    Garante que as colunas Y e Z existam com os cabeçalhos corretos
    e aplica validação de dados (picklist) nas linhas de dados.
    """
    # Cabeçalhos
    ws.range(f"{COL_PRIORIDADE}1").value = "PRIORIDADE"
    ws.range(f"{COL_MOTIVO}1").value = "MOTIVO DO STATUS"

    # Copiar estilo da coluna X1 para Y1 e Z1 (opcional, só estética)
    try:
        cab_status = ws.range(f"{COL_STATUS_LEAD}1")
        cab_status.api.Copy()
        ws.range(f"{COL_PRIORIDADE}1").api.PasteSpecial(Paste=-4104)  # xlPasteFormats
        ws.range(f"{COL_MOTIVO}1").api.PasteSpecial(Paste=-4104)
    except Exception:
        # Se der qualquer coisa, ignora – é só estética
        pass

    max_row = obter_ultima_linha(ws)

    # Validação de dados (PRIORIDADE)
    try:
        rng_prioridade = ws.range(f"{COL_PRIORIDADE}2:{COL_PRIORIDADE}{max_row}")
        rng_prioridade.api.Validation.Delete()
        rng_prioridade.api.Validation.Add(
            Type=3,  # xlValidateList
            AlertStyle=1,  # xlValidAlertStop
            Operator=1,
            Formula1="ALTA,MÉDIA,BAIXA"
        )
    except Exception:
        pass

    # Validação de dados (MOTIVO)
    try:
        rng_motivo = ws.range(f"{COL_MOTIVO}2:{COL_MOTIVO}{max_row}")
        rng_motivo.api.Validation.Delete()
        rng_motivo.api.Validation.Add(
            Type=3,  # xlValidateList
            AlertStyle=1,
            Operator=1,
            Formula1="SEM INTERESSE,SEM PERFIL,DESISTIU,NÃO RESPONDE,NÃO ATENDEU,OUTROS"
        )
    except Exception:
        pass


# =========================
# PROCESSAMENTO POR ABA
# =========================

def processar_aba(ws):
    hoje = date.today()
    max_row = obter_ultima_linha(ws)

    # Garante existência de PRIORIDADE e MOTIVO + validação
    garantir_colunas_prioridade_motivo(ws)

    for row in range(2, max_row + 1):

        # Nome do Cliente (gatilho de linha ativa)
        cel_nome = ws.range(f"{COL_NOME}{row}")
        nome_val = cel_nome.value

        if nome_val is None or str(nome_val).strip() == "":
            continue  # linha vazia / sem lead

        # -------- 1) DATA (C) – congelar / preencher se linha nova --------
        cel_data = ws.range(f"{COL_DATA}{row}")
        data_val = to_date(cel_data.value)

        if data_val is None:
            # Nova linha (nome preenchido e sem data) -> grava hoje
            data_val = hoje
            cel_data.value = hoje

        # Dias desde o último contato
        dias_sem_contato = (hoje - data_val).days if data_val is not None else None

        # -------- 2) PRIORIDADE (Y) --------
        cel_prior = ws.range(f"{COL_PRIORIDADE}{row}")
        prioridade_val = normalizar_texto(cel_prior.value)

        # -------- 3) DATA DO PRÓXIMO CONTATO (U) --------
        cel_prox = ws.range(f"{COL_DATA_PROX}{row}")
        prox_val = cel_prox.value

        if (prox_val is None or str(prox_val).strip() == "") and prioridade_val != "":
            delta = None
            if prioridade_val == "ALTA":
                delta = 3
            elif prioridade_val in ("MÉDIA", "MEDIA"):
                delta = 7
            elif prioridade_val == "BAIXA":
                delta = 15

            if delta is not None:
                cel_prox.value = hoje + timedelta(days=delta)

        # -------- 4) STATUS DA AGENDA (V) --------
        cel_status_agenda = ws.range(f"{COL_STATUS_AGENDA}{row}")
        if dias_sem_contato is not None:
            if dias_sem_contato >= 4:
                cel_status_agenda.value = "ATRASADO"
            else:
                cel_status_agenda.value = "AGUARDAR PROX. CONTATO"

        # -------- 5) SLA ATENDIMENTO (W) --------
        cel_sla = ws.range(f"{COL_SLA}{row}")
        sla_val = cel_sla.value
        if data_val is not None and (sla_val is None or str(sla_val).strip() == ""):
            cel_sla.value = data_val + timedelta(days=2)

        # -------- 6) STATUS DO LEAD (X) – LEAD ATIVO / LEAD MORTO --------
        cel_status_lead = ws.range(f"{COL_STATUS_LEAD}{row}")
        cel_motivo = ws.range(f"{COL_MOTIVO}{row}")
        motivo_txt = normalizar_texto(cel_motivo.value)

        lead_status = None

        # LEAD MORTO por motivo
        if any(p in motivo_txt for p in ["SEM INTERESSE", "SEM PERFIL", "DESISTIU"]):
            lead_status = "LEAD MORTO"
        else:
            # LEAD MORTO por tempo
            if dias_sem_contato is not None and dias_sem_contato > 45:
                lead_status = "LEAD MORTO"
            else:
                # LEAD ATIVO pelas regras de prioridade + tempo
                if dias_sem_contato is not None:
                    if prioridade_val == "ALTA" and dias_sem_contato <= 10:
                        lead_status = "LEAD ATIVO"
                    elif prioridade_val in ("MÉDIA", "MEDIA") and dias_sem_contato <= 20:
                        lead_status = "LEAD ATIVO"
                    elif prioridade_val == "BAIXA" and dias_sem_contato <= 30:
                        lead_status = "LEAD ATIVO"

        if lead_status is not None:
            cel_status_lead.value = lead_status


# =========================
# MAIN
# =========================

def main():
    print("===================================================")
    print("🚀 ATUALIZAÇÃO DE DATAS E STATUS - CRM MULTIMARCAS (xlwings)")
    print(f"Arquivo: {ARQUIVO_CRM}")
    print("===================================================")

    if not ARQUIVO_CRM.exists():
        print("❌ Arquivo não encontrado. Verifique o nome/caminho e rode de dentro da pasta correta.")
        return

    # Excel invisível (conforme você pediu)
    app = xw.App(visible=False, add_book=False)
    try:
        wb = app.books.open(str(ARQUIVO_CRM))

        for aba in SDR_ABAS:
            if aba not in [s.name for s in wb.sheets]:
                print(f"⚠ Aba '{aba}' não encontrada, ignorando...")
                continue

            print(f"\n▶ Processando aba: {aba}...")
            ws = wb.sheets[aba]
            processar_aba(ws)
            print(f"✅ Aba '{aba}' atualizada.")

        wb.save()
        wb.close()
        print("\n🎉 Atualização concluída com sucesso!")
        print("Datas, agenda, SLA e STATUS DO LEAD foram atualizados.")
        print("Colunas PRIORIDADE e MOTIVO DO STATUS criadas com validação de dados.")

    finally:
        app.quit()


if __name__ == "__main__":
    main()
