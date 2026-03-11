import easyocr
import pandas as pd

def converter_cidades_alinhado(imagem_path):
    print(f"⌛ Analisando {imagem_path} com ancoragem de colunas...")
    reader = easyocr.Reader(['pt'])
    
    # Obtém o resultado com as caixas delimitadoras (coordinates)
    resultado = reader.readtext(imagem_path)
    
    elementos = []
    for (bbox, texto, prob) in resultado:
        # Pega o ponto inicial X e o centro Y
        x_min = bbox[0][0]
        y_centro = (bbox[0][1] + bbox[2][1]) / 2
        elementos.append({'texto': texto.strip().upper(), 'x': x_min, 'y': y_centro})

    # 1. Agrupar por linhas (Y) com tolerância
    elementos.sort(key=lambda e: e['y'])
    linhas_brutas = []
    if not elementos: return

    margem_y = 15 
    linha_atual = [elementos[0]]
    for i in range(1, len(elementos)):
        if abs(elementos[i]['y'] - linha_atual[-1]['y']) < margem_y:
            linha_atual.append(elementos[i])
        else:
            linhas_brutas.append(linha_atual)
            linha_atual = [elementos[i]]
    linhas_brutas.append(linha_atual)

    # 2. Separar por Colunas usando o eixo X
    # Na sua imagem, o Estado começa bem na esquerda (ex: x < 50)
    # Vamos definir um divisor baseado na média dos elementos
    dados_finais = []
    
    for linha in linhas_brutas:
        # Inicializa a linha do Excel vazia
        row = {'Estado': '', 'Cidade': ''}
        
        for item in linha:
            # Lógica: se o texto tem 2 letras e está à esquerda, é Estado.
            # Se o texto é longo ou está mais à direita, é Cidade.
            if len(item['texto']) <= 3 and item['x'] < 100: # Ajuste o 100 se necessário
                row['Estado'] = item['texto']
            else:
                # Se já houver algo na cidade (ex: nomes compostos lidos separados), concatena
                if row['Cidade']:
                    row['Cidade'] += " " + item['texto']
                else:
                    row['Cidade'] = item['texto']
        
        dados_finais.append(row)

    # 3. Exportar para Excel
    df = pd.DataFrame(dados_finais)
    df = df[['Estado', 'Cidade']] # Garante a ordem das colunas
    
    df.to_excel("Cidades.xlsx", index=False)
    print("✅ Arquivo 'Cidades.xlsx' gerado com sucesso exatamente como na imagem!")

# Executar
converter_cidades_alinhado("Cidades.jpeg")