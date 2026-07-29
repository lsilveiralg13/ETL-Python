import pandas as pd
import os

def diagnosticar_tabela():
    # Caminho fixo conforme seu ambiente
    diretorio = r'C:\Users\lucas.barros\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho\Scripts Python'
    
    # Listar arquivos para você escolher
    arquivos = [f for f in os.listdir(diretorio) if f.endswith(('.csv', '.xlsx', '.parquet'))]
    
    print("\n--- Analisador de Performance de Dados ---")
    for i, arq in enumerate(arquivos):
        print(f"[{i}] {arq}")
    
    try:
        idx = int(input("\nSelecione o arquivo para diagnóstico: "))
        arquivo_alvo = os.path.join(diretorio, arquivos[idx])
    except:
        return

    print(f"\nCarregando {arquivos[idx]} para análise...")
    
    # Carregamento (usando a lógica de encodings que definimos antes)
    if arquivo_alvo.endswith('.csv'):
        try:
            df = pd.read_csv(arquivo_alvo, encoding='utf-8-sig', sep=None, engine='python')
        except:
            df = pd.read_csv(arquivo_alvo, encoding='ISO-8859-1', sep=None, engine='python')
    elif arquivo_alvo.endswith('.parquet'):
        df = pd.read_parquet(arquivo_alvo)
    else:
        df = pd.read_excel(arquivo_alvo)

    # --- INÍCIO DO DIAGNÓSTICO ---
    
    print("\n" + "="*50)
    print("RESUMO DE DIAGNÓSTICO")
    print("="*50)
    
    # 1. Uso Total de Memória
    uso_memoria_total = df.memory_usage(deep=True).sum() / (1024**2)
    print(f"🔹 Uso total em RAM: {uso_memoria_total:.2f} MB")
    print(f"🔹 Total de Linhas: {len(df)}")
    print(f"🔹 Total de Colunas: {len(df.columns)}")

    # 2. Top 5 Colunas mais "Pesadas"
    print("\n📊 TOP 5 COLUNAS POR CONSUMO DE MEMÓRIA:")
    uso_colunas = df.memory_usage(deep=True).drop(index='Index') / (1024**2)
    print(uso_colunas.sort_values(ascending=False).head(5))

    # 3. Análise de Cardinalidade (O vilão do Power BI e Parquet)
    print("\n🔍 ANÁLISE DE CARDINALIDADE (Valores Únicos):")
    cardinalidade = df.nunique().sort_values(ascending=False)
    print(cardinalidade.head(5))
    print("💡 Dica: Colunas com muitos valores únicos (IDs, Timestamps) dificultam a compressão.")

    # 4. Sugestões de Otimização
    print("\n💡 SUGESTÕES DE OTIMIZAÇÃO:")
    
    for col in df.columns:
        # Se for objeto (string) mas tiver poucos valores únicos, sugere 'category'
        if df[col].dtype == 'object':
            num_unique = df[col].nunique()
            if num_unique < len(df) * 0.5: # Se menos de 50% são únicos
                print(f"- Coluna '{col}': Converter para CATEGORY (Economia estimada: ~80%)")
        
        # Se for float64, sugere reduzir para float32
        if df[col].dtype == 'float64':
            print(f"- Coluna '{col}': Converter para FLOAT32 ou INT se não houver decimais.")

    print("\n" + "="*50)

if __name__ == "__main__":
    diagnosticar_tabela()