import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont 
import pywhatkit as kit
import os

# --- CONFIGURAÇÕES ---
ARQUIVO = r"C:\Users\lucasbarros\OneDrive - CTC FRANCHISING S A\Área de Trabalho\Scripts Python\BASE DE DADOS.xlsx"
DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = "root", "root", "localhost", 3306, "dvwarehouse"
WHATSAPP_GROUP_ID = "DQda6YiLMXTLbI2CfRfQTr"

METAS_FAT = {
    1: 1515000.0, 2: 1944586.0, 3: 2309969.2, 4: 2790608.0, 5: 2360000.0, 6: 2002000.0,
    7: 2525302.15, 8: 3027135.9, 9: 3211560.0, 10: 2439450.0, 11: 3211488.0, 12: 2092860.0
}

METAS_KPIS = {
    1: {'conv': 32.6, 'tm': 5500.0}, 2: {'conv': 32.6, 'tm': 7000.0},
    3: {'conv': 32.6, 'tm': 7000.0}, 4: {'conv': 32.6, 'tm': 6000.0},
    5: {'conv': 22.5, 'tm': 6500.0}, 6: {'conv': 25.0, 'tm': 7428.6},
    7: {'conv': 28.0, 'tm': 7428.65}, 8: {'conv': 25.0, 'tm': 6481.2},
    9: {'conv': 25.0, 'tm': 6845.0}, 10: {'conv': 27.0, 'tm': 7500.0},
    11: {'conv': 29.5, 'tm': 8400.0}, 12: {'conv': 22.0, 'tm': 7000.0}
}

VIGENCIA = {
    "ERIKHA": (1, 12), "GLENDASOUZA": (1, 12), "ISABELLASILVA": (1, 12),
    "LAVINIAMIRANDA": (1, 2), "JOSIANEVIEIRA": (3, 12), "LUCIANAPEREIRA": (1, 12),
    "MARCELAVAZ": (1, 12), "NELIANE": (1, 11), "ROBERTAPEREIRA": (12, 12)
}

def calcular_dias_uteis_totais(ano, mes):
    fim = pd.Period(f"{ano}-{mes}").days_in_month
    datas = pd.date_range(start=f"{ano}-{mes}-01", end=f"{ano}-{mes}-{fim}")
    return len(datas[datas.dayofweek < 5])

def processar_orion():
    print("\n" + "="*30)
    mes_ref = int(input("Digite o MÊS desejado (1-12): "))
    ano_ref = int(input("Digite o ANO desejado (ex: 2025): "))
    print("="*30 + "\n")

    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # 1. Leitura e ETL
    df_fato = pd.read_excel(ARQUIVO, sheet_name="FATURADO")
    df_cad = pd.read_excel(ARQUIVO, sheet_name="CADASTRO")
    df_fato['Apelido (Vendedor)'] = df_fato['Apelido (Vendedor)'].astype(str).str.strip().str.upper()
    df_cad['Apelido (Vendedor)'] = df_cad['Apelido (Vendedor)'].astype(str).str.strip().str.upper()
    df_fato['Dt. Neg.'] = pd.to_datetime(df_fato['Dt. Neg.'], errors='coerce')
    vendas = df_fato[(df_fato['Dt. Neg.'].dt.month == mes_ref) & (df_fato['Dt. Neg.'].dt.year == ano_ref)].copy()
    
    col_ab = df_cad.columns[27]
    ativos_base = df_cad[df_cad[col_ab].astype(str).str.strip().str.upper() == 'BASE ATIVA'].groupby('Apelido (Vendedor)')['Cód. Parceiro'].nunique().reset_index()
    ativos_base.columns = ['Apelido (Vendedor)', 'Qtd_Ativos']

    quadro = vendas.groupby('Apelido (Vendedor)').agg(Faturado=('Vlr. Nota', 'sum'), Unicos=('Parceiro', 'nunique')).reset_index()
    vendedoras_ativas = [v for v, (i, f) in VIGENCIA.items() if i <= mes_ref <= f]
    quadro = quadro[quadro['Apelido (Vendedor)'].isin(vendedoras_ativas)]
    quadro = pd.merge(quadro, ativos_base, on='Apelido (Vendedor)', how='left').fillna(0)
    
    m_glob = METAS_FAT.get(mes_ref, 0)
    m_kpi = METAS_KPIS.get(mes_ref)
    
    quadro['Taxa_Conv'] = (quadro['Unicos'] / quadro['Qtd_Ativos'] * 100).replace([np.inf, -np.inf], 0).round(1)
    quadro['TM'] = (quadro['Faturado'] / quadro['Unicos']).replace([np.inf, -np.inf], 0).round(2)
    quadro = quadro.sort_values(by='Faturado', ascending=False)

    # 2. Geração da Imagem
    largura = 800
    altura = 1100
    img = Image.new('RGB', (largura, altura), color=(255, 255, 255))
    draw = ImageDraw.Draw(img)
    
    try:
        font_title = ImageFont.truetype("arial.ttf", 36)
        font_body = ImageFont.truetype("arial.ttf", 18)
        font_bold = ImageFont.truetype("arialbd.ttf", 20)
    except:
        font_title = font_body = font_bold = ImageFont.load_default()

    # Título Centralizado
    texto_titulo = "🚀 DROP GESTÃO À VISTA - MULTIMARCAS 🚀"
    bbox = draw.textbbox((0, 0), texto_titulo, font=font_title)
    w_titulo = bbox[2] - bbox[0]
    draw.text(((largura - w_titulo) / 2, 40), texto_titulo, fill=(0, 0, 0), font=font_title)
    
    # Card de Resumo
    realizado = quadro['Faturado'].sum()
    dias_u = calcular_dias_uteis_totais(ano_ref, mes_ref)
    y_card = 120
    draw.rectangle([40, y_card, 760, y_card+160], outline=(0, 128, 0), width=3)
    draw.text((60, y_card+15), f"📅 Período: {mes_ref:02d}/{ano_ref}", fill=(0,0,0), font=font_body)
    draw.text((60, y_card+45), f"💰 Meta Global: R$ {m_glob:,.2f}", fill=(0,0,0), font=font_body)
    draw.text((60, y_card+75), f"📈 Realizado: R$ {realizado:,.2f}", fill=(0,0,0), font=font_body)
    draw.text((60, y_card+105), f"🎯 Meta Diária: R$ {(m_glob/dias_u):,.2f}", fill=(0,0,0), font=font_body)
    draw.text((60, y_card+130), f"📉 Falta Faturar: R$ {max(0, m_glob-realizado):,.2f}", fill=(200,0,0), font=font_bold)

    # Ranking
    y_rank = 310
    draw.text((40, y_rank), "🏆 DETALHE POR VENDEDORA", fill=(0, 100, 0), font=font_bold)
    y_rank += 50
    
    for r in quadro.itertuples(index=False):
        c_status = (0, 128, 0) if r[4] >= m_kpi['conv'] else (200, 0, 0)
        t_status = (0, 128, 0) if r[5] >= m_kpi['tm'] else (200, 0, 0)
        f_status = (0, 128, 0) if r[1] >= (m_glob/7) else (200, 0, 0)

        draw.text((40, y_rank), f"• {r[0]}", fill=f_status, font=font_bold)
        draw.text((60, y_rank+28), f"Faturado: R$ {r[1]:,.2f} | Únicos: {int(r[2])} | Ativos: {int(r[3])}", fill=(60,60,60), font=font_body)
        draw.text((60, y_rank+53), f"Conv: {r[4]}% (Meta {m_kpi['conv']}%)", fill=c_status, font=font_body)
        draw.text((380, y_rank+53), f"TM: R$ {r[5]:,.2f} (Meta R${m_kpi['tm']:,.0f})", fill=t_status, font=font_body)
        
        y_rank += 110 # Espaço entre blocos

    # 3. Preview e Exportação
    img_path = "drop_orion_final.png"
    img.save(img_path)
    
    print("\n[ÓRION] Gerando PREVIEW da imagem...")
    img.show() # ABRE A IMAGEM PARA VOCÊ VER
    
    # 4. Envio
    if input("\nA imagem está correta? Deseja abrir o WhatsApp para enviar? (S/N): ").upper() == 'S':
        print("\nO WhatsApp abrirá. Arraste 'drop_orion_final.png' para o grupo.")
        kit.sendwhatmsg_to_group_instantly(WHATSAPP_GROUP_ID, "Gestão à Vista Multimarcas atualizada:", wait_time=15)

if __name__ == "__main__":
    processar_orion()