import pandas as pd
import numpy as np
from PIL import Image, ImageDraw, ImageFont 
import os

# --- CONFIGURAÇÕES ---
ARQUIVO = r"C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\Scripts Python\BASE DE DADOS.xlsx"

MESES_EXTENSO = {
    1: "JANEIRO", 2: "FEVEREIRO", 3: "MARÇO", 4: "ABRIL", 5: "MAIO", 6: "JUNHO",
    7: "JULHO", 8: "AGOSTO", 9: "SETEMBRO", 10: "OUTUBRO", 11: "NOVEMBRO", 12: "DEZEMBRO"
}

# Metas e Vigências
METAS_FAT_TOTAL = {12: 2092860.0}
METAS_VEND_KPI = {12: {'conv': 22.0}}
METAS_SDR_TOTAL = {12: {'lojas': 25, 'cad': 30, 'conv': 80.0}}

VIG_SDR = {"SCARLETSANTOS": (1,12), "IZABELARODRIGUE": (1,5), "ROBERTAPEREIRA": (1,12), "TARVYLLASANTOS": (1,12), "MARIAMAINARDES": (1,7), "MARIAGUIMARES": (1,12), "RAIANEMARTINS": (1,12), "ANDRELEOCADIO": (5,12)}
VIG_VEND = {"ERIKHA": (1,12), "GLENDASOUZA": (1,12), "ISABELLASILVA": (1,12), "LAVINIAMIRANDA": (1,2), "JOSIANEVIEIRA": (3,12), "LUCIANAPEREIRA": (1,12), "MARCELAVAZ": (1,12), "NELIANE": (1,11), "ROBERTAPEREIRA": (12,12)}

def formatar_brl(valor):
    return f"{valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def desenhar_grafico(df, col_valor, titulo, meta, meta_display, arq_nome, prefix="", sufixo=""):
    if df.empty: return
    df_plot = df.sort_values(by=col_valor, ascending=False)
    largura, recuo_x = 1100, 320
    altura = 160 + (len(df_plot) * 65)
    img = Image.new('RGB', (largura, altura), (255,255,255))
    draw = ImageDraw.Draw(img)
    try:
        f_sub = ImageFont.truetype("arial.ttf", 34); f_lab = ImageFont.truetype("arial.ttf", 24); f_val = ImageFont.truetype("arialbd.ttf", 22)
    except:
        f_sub = f_lab = f_val = ImageFont.load_default()

    draw.text((40, 35), f"{titulo} | Meta: {meta_display}", fill=(0, 80, 0), font=f_sub)
    y, max_v = 110, max(df_plot[col_valor].max() if not df_plot.empty else 0, meta * 1.1, 1)
    espaco = largura - recuo_x - 220

    for row in df_plot.itertuples():
        val = getattr(row, col_valor)
        cor = (0, 128, 0) if val >= meta else (200, 0, 0)
        larg_b = (val / max_v) * espaco
        draw.rectangle([recuo_x, y, recuo_x + larg_b, y + 40], fill=cor)
        nome = str(row[1])[:20]
        draw.text((recuo_x - 15 - draw.textbbox((0,0), nome, font=f_lab)[2], y + 5), nome, (0,0,0), font=f_lab)
        
        texto_exibicao = f"{prefix}{formatar_brl(val)}{sufixo}" if prefix == "R$ " else f"{val:.1f}{sufixo}" if sufixo == "%" else f"{int(val)}"
        draw.text((recuo_x + larg_b + 15, y + 7), texto_exibicao, (0,0,0), font=f_val)
        y += 65
    img.save(arq_nome)

def processar_orion():
    mes_num = int(input("Mês (1-12): ")); ano = str(input("Ano (AAAA): "))
    mes_extenso = MESES_EXTENSO.get(mes_num)
    padrao_mes = f"-{mes_num:02d}-"

    df_ped = pd.read_excel(ARQUIVO, sheet_name="PEDIDO")
    df_cad = pd.read_excel(ARQUIVO, sheet_name="CADASTRO")
    df_fato = pd.read_excel(ARQUIVO, sheet_name="FATURADO")

    # --- SDR ---
    df_ped['DATA_STR'] = df_ped.iloc[:, 0].astype(str)
    p_mes = df_ped[(df_ped['DATA_STR'].str.contains(ano)) & (df_ped.iloc[:, 20].astype(str).str.strip().str.upper() == mes_extenso)].copy()
    p_mes.iloc[:, 18] = pd.to_numeric(p_mes.iloc[:, 18], errors='coerce').fillna(0)
    sdr_prod = p_mes.groupby('Nome SDR').agg(Lojas_Pedido=(df_ped.columns[18], 'sum')).reset_index()

    df_cad['DATA_CAD_STR'] = df_cad.iloc[:, 24].astype(str)
    c_mes = df_cad[(df_cad['DATA_CAD_STR'].str.contains(ano)) & (df_cad['DATA_CAD_STR'].str.contains(padrao_mes))].copy()
    sdr_cad = c_mes.groupby('SDR').size().reset_index(name='Lojas_Cadastradas')

    for d, c in [(sdr_prod, 'Nome SDR'), (sdr_cad, 'SDR')]: d[c] = d[c].astype(str).str.strip().str.upper()
    tab_sdr = pd.merge(sdr_prod, sdr_cad, left_on='Nome SDR', right_on='SDR', how='outer').fillna(0)
    tab_sdr['Final_SDR'] = np.where(tab_sdr['Nome SDR'] != "0", tab_sdr['Nome SDR'], tab_sdr['SDR'])
    lista_s_ativas = [v for v, (i, f) in VIG_SDR.items() if i <= mes_num <= f]
    tab_sdr = tab_sdr[tab_sdr['Final_SDR'].isin(lista_s_ativas)]
    tab_sdr['Conv_SDR'] = (tab_sdr['Lojas_Pedido'] / tab_sdr['Lojas_Cadastradas'] * 100).replace([np.inf, -np.inf], 0).fillna(0)

    # --- VENDEDORES ---
    df_fato['DATA_FATO_STR'] = df_fato['Dt. Neg.'].astype(str)
    v_mes = df_fato[(df_fato['DATA_FATO_STR'].str.contains(ano)) & (df_fato['DATA_FATO_STR'].str.contains(padrao_mes))].copy()
    q_v = v_mes.groupby('Apelido (Vendedor)').agg(Faturado=('Vlr. Nota', 'sum'), Clientes=('Parceiro', 'nunique')).reset_index()
    
    base_ativa = df_cad[df_cad['BASE DE ATIVOS'].astype(str).str.contains('BASE ATIVA', case=False, na=False)]
    ativos_v = base_ativa.groupby('Apelido (Vendedor)').size().reset_index(name='Total_Base')
    
    lista_v_ativas = [v for v, (i, f) in VIG_VEND.items() if i <= mes_num <= f]
    tab_vendas = pd.merge(q_v, ativos_v, on='Apelido (Vendedor)', how='left').fillna(0)
    tab_vendas['Apelido_Upper'] = tab_vendas['Apelido (Vendedor)'].astype(str).str.strip().str.upper()
    tab_vendas = tab_vendas[tab_vendas['Apelido_Upper'].isin(lista_v_ativas)]
    tab_vendas['Conv_Vendas'] = (tab_vendas['Clientes'] / tab_vendas['Total_Base'] * 100).fillna(0)

    # --- METAS ---
    meta_v_fat = np.ceil(METAS_FAT_TOTAL.get(mes_num, 0) / len(lista_v_ativas)) if lista_v_ativas else 0
    meta_s_lojas = np.ceil(METAS_SDR_TOTAL[12]['lojas'] / len(lista_s_ativas)) if lista_s_ativas else 0
    meta_s_cad = np.ceil(METAS_SDR_TOTAL[12]['cad'] / len(lista_s_ativas)) if lista_s_ativas else 0

    # --- GERAÇÃO ---
    desenhar_grafico(tab_vendas, 'Faturado', "VENDAS: FATURAMENTO", meta_v_fat, f"R$ {formatar_brl(meta_v_fat)}", "v_fat.png", prefix="R$ ")
    desenhar_grafico(tab_vendas, 'Conv_Vendas', "VENDAS: % CONV. BASE", METAS_VEND_KPI[12]['conv'], f"{METAS_VEND_KPI[12]['conv']}%", "v_conv.png", sufixo="%")
    desenhar_grafico(tab_sdr, 'Lojas_Pedido', "SDR: LOJAS C/ PEDIDO", meta_s_lojas, str(int(meta_s_lojas)), "s_lojas.png")
    desenhar_grafico(tab_sdr, 'Lojas_Cadastradas', "SDR: NOVOS CADASTROS", meta_s_cad, str(int(meta_s_cad)), "s_cad.png")
    desenhar_grafico(tab_sdr, 'Conv_SDR', "SDR: % CONV. PROSPECÇÃO", METAS_SDR_TOTAL[12]['conv'], f"{METAS_SDR_TOTAL[12]['conv']}%", "s_conv.png", sufixo="%")

    print(f"\n[ÓRION] Processado com sucesso.")
    for f in ["v_fat.png", "v_conv.png", "s_lojas.png", "s_cad.png", "s_conv.png"]: 
        if os.path.exists(f): os.startfile(f)

if __name__ == "__main__":
    processar_orion()