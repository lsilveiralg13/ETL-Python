import pandas as pd
from sqlalchemy import create_engine, text
import warnings
import pymysql
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor 
from sklearn.metrics import mean_absolute_error
import locale 

warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

# Lista definitiva dos vendedores ativos para filtro no Python
# INCLUINDO LAVINIAMIRANDA para que as features dela possam ser usadas como proxy.
VENDEDORES_ATIVOS = {'ERIKHA', 'GLENDASOUZA', 'ISABELLASILVA', 'JOSIANEVIEIRA', 'LUCIANAPEREIRA', 'MARCELAVAZ', 'NELIANE', 'LAVINIAMIRANDA'}

# Vendedores que serão efetivamente previstos (o restante é apenas para extrair features proxy)
VENDEDORES_PARA_PREVER = {'ERIKHA', 'GLENDASOUZA', 'ISABELLASILVA', 'JOSIANEVIEIRA', 'LUCIANAPEREIRA', 'MARCELAVAZ', 'NELIANE'}


# --- CONFIGURAÇÃO LOCALE (Para R$) ---
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8') 
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil')
    except locale.Error:
        pass 

# --- CONFIGURAÇÃO MYSQL ---
MYSQL_CONFIG = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'port': 3306,
    'database': 'dvwarehouse'
}
TABELA_FEATURES = 'features_predicao_vendedor'
TABELA_PREDICAO_DESTINO = 'predicao_faturamento_vendedor' 

# --- FUNÇÕES AUXILIARES ---

def formatar_valor_reais(valor):
    """Garante a formatação R$ x.xxx,xx mesmo se o locale falhar."""
    try:
        return locale.currency(valor, grouping=True, symbol=True)
    except Exception:
        # Fallback manual robusto (ponto para milhar, vírgula para decimal)
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# --- FASE 2: CARREGAMENTO ---

def carregar_features_para_modelo(config, tabela_features):
    db_url = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    try:
        engine = create_engine(db_url)
        query_select = f"SELECT * FROM {tabela_features} WHERE Faturamento_Lag_1 IS NOT NULL;"
        df_modelo = pd.read_sql(query_select, con=engine)
        print(f"[SUCESSO] {len(df_modelo)} linhas de dados carregadas do MySQL.")
        return df_modelo
    except Exception as e:
        print(f"\n[ERRO MYSQL] Falha ao carregar a VIEW '{tabela_features}'. Verifique se ela foi criada e se a coluna Media_6_Meses existe: {e}")
        return None


# --- FASE 3: MODELAGEM E PREDIÇÃO (COM Proxy para JOSIANEVIEIRA) ---

def treinar_e_prever_por_vendedor(df_modelo):
    """
    Treina o modelo com Random Forest e aplica a lógica de proxy para JOSIANEVIEIRA se necessário.
    """
    VENDEDOR_ALVO = 'JOSIANEVIEIRA'
    VENDEDOR_PROXY = 'LAVINIAMIRANDA'
    
    # Filtra todos os ativos, incluindo o proxy, para treinamento e extração de features
    df_filtrado = df_modelo[df_modelo['vendedor_apelido'].isin(VENDEDORES_ATIVOS)].copy()
    resultados_finais = []
    
    # Vendedores para prever (apenas os 7 originais)
    vendedores = VENDEDORES_PARA_PREVER 
    
    print(f"\n[MODELAGEM] Iniciando treinamento para {len(vendedores)} vendedores ATIVOS com Random Forest...")

    # Pré-filtra o DataFrame do proxy para uso posterior
    df_proxy = df_filtrado[df_filtrado['vendedor_apelido'] == VENDEDOR_PROXY]
    
    if df_proxy.empty:
        print(f"\n[AVISO] Não foi possível encontrar dados para a vendedora proxy {VENDEDOR_PROXY}. A lógica de substituição para {VENDEDOR_ALVO} será ignorada.")

    for i, vendedor in enumerate(vendedores):
        df_vendedor = df_filtrado[df_filtrado['vendedor_apelido'] == vendedor].copy()
        
        if len(df_vendedor) < 6: 
            print(f"  -> Pulando {vendedor}: Dados insuficientes ({len(df_vendedor)}).")
            continue
            
        # 1. TREINAMENTO
        # O treinamento é feito com os dados históricos REAIS de CADA vendedora.
        Y = df_vendedor['Valor_Target']
        X = df_vendedor[['Faturamento_Lag_1', 'Media_3_Meses', 'Media_6_Meses', 'Ano', 'Mes']] 
        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, shuffle=False)
        
        model = RandomForestRegressor(n_estimators=100, random_state=42) 
        model.fit(X_train, Y_train)
        
        Y_pred = model.predict(X_test)
        mae = mean_absolute_error(Y_test, Y_pred)
        
        # 2. PREPARAÇÃO DA PREVISÃO E LÓGICA DE PROXY
        
        ultima_linha = df_vendedor.iloc[-1]
        mes_proximo = ultima_linha['Mes'] % 12 + 1
        ano_proximo = ultima_linha['Ano'] if mes_proximo != 1 else ultima_linha['Ano'] + 1
        
        # Valores padrão (reais da vendedora atual)
        lag_1 = ultima_linha['Valor_Target']
        media_3 = df_vendedor['Valor_Target'].iloc[-3:].mean()
        media_6 = df_vendedor['Valor_Target'].iloc[-6:].mean()
        
        # >>> INÍCIO DA LÓGICA CONDICIONAL (PROXY) <<<
        if vendedor == VENDEDOR_ALVO and not df_proxy.empty:
            
            # Condição: se o Faturamento Lag 1 (último mês real) dela for ZERO ou NULL
            if pd.isnull(lag_1) or lag_1 == 0:
                print(f"  -> {vendedor}: Faturamento_Lag_1 zerado/ausente. Usando features de {VENDEDOR_PROXY} como proxy para a previsão.")
                
                # Encontra a última linha da LAVINIAMIRANDA (proxy)
                ultima_linha_proxy = df_proxy.iloc[-1]
                
                # Puxa o Faturamento Lag 1 e as médias do proxy para o input da previsão
                lag_1 = ultima_linha_proxy['Valor_Target']
                media_3 = df_proxy['Valor_Target'].iloc[-3:].mean()
                media_6 = df_proxy['Valor_Target'].iloc[-6:].mean()
        # >>> FIM DA LÓGICA CONDICIONAL <<<

        
        dados_predicao = pd.DataFrame({
            'Faturamento_Lag_1': [lag_1],
            'Media_3_Meses': [media_3],
            'Media_6_Meses': [media_6],
            'Ano': [ano_proximo],
            'Mes': [mes_proximo]
        })
        previsao_valor = model.predict(dados_predicao)[0]
        
        # Armazena o Resultado (Arredondado)
        resultados_finais.append({
            'data_previsao': pd.to_datetime(f'{ano_proximo}-{mes_proximo:02d}-01'),
            'vendedor_apelido': vendedor,
            'valor_predito': round(previsao_valor, 2), 
            'modelo_mae': round(mae, 2)
        })
        
        # Exibição formatada no console
        print(f"  -> Treinado {i+1}/{len(vendedores)}. {vendedor}: Prev. {formatar_valor_reais(previsao_valor)}")

    df_previsao = pd.DataFrame(resultados_finais)
    
    # Adiciona a linha de SOMA TOTAL
    soma_total = df_previsao['valor_predito'].sum()
    mae_medio = df_previsao['modelo_mae'].mean()
    data_ref = df_previsao['data_previsao'].iloc[0]
    
    df_total = pd.DataFrame([{
        'data_previsao': data_ref,
        'vendedor_apelido': 'TOTAL_GERAL', 
        'valor_predito': round(soma_total, 2),
        'modelo_mae': round(mae_medio, 2)
    }])
    
    df_final = pd.concat([df_previsao, df_total], ignore_index=True)
    
    print("\n-----------------------------------------------------")
    print(f"PREVISÃO TOTAL GERAL: {formatar_valor_reais(soma_total)}")
    print("-----------------------------------------------------")
    
    return df_final

# --- FASE 4: PERSISTÊNCIA NO MYSQL (COM SQL PURO) ---

def salvar_predicao_mysql(df_previsao, config, tabela_destino):
    """
    Deleta as previsões da data atual e insere as novas usando comandos SQL puros.
    """
    db_url = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    
    data_a_inserir = df_previsao['data_previsao'].iloc[0].strftime('%Y-%m-%d')
    sql_delete = text(f"DELETE FROM {tabela_destino} WHERE DATE(data_previsao) = :data_a_deletar")
    
    try:
        engine = create_engine(db_url)
        
        with engine.begin() as connection:
            
            # 1. Executa o DELETE (Limpa duplicatas do mês atual)
            connection.execute(sql_delete, {'data_a_deletar': data_a_inserir})
            
            # 2. Insere os novos dados usando SQL puro
            sql_insert_base = f"INSERT INTO {tabela_destino} (data_previsao, vendedor_apelido, valor_predito, modelo_mae) VALUES (:data_previsao, :vendedor_apelido, :valor_predito, :modelo_mae)"
            
            for index, row in df_previsao.iterrows():
                params = {
                    'data_previsao': row['data_previsao'].strftime('%Y-%m-%d %H:%M:%S'),
                    'vendedor_apelido': row['vendedor_apelido'],
                    'valor_predito': row['valor_predito'],
                    'modelo_mae': row['modelo_mae']
                }
                connection.execute(text(sql_insert_base), params)
            
            print(f"\n[SUCESSO] Previsões atualizadas e salvas em '{tabela_destino}'.")
            
    except Exception as e:
        print(f"\n[ERRO FATAL] Falha ao salvar a previsão final: {e}")


# --- FLUXO PRINCIPAL DE EXECUÇÃO ---

if __name__ == "__main__":
    
    df_modelo = carregar_features_para_modelo(MYSQL_CONFIG, TABELA_FEATURES)
    
    if df_modelo is not None and not df_modelo.empty:
        
        df_previsao = treinar_e_prever_por_vendedor(df_modelo) 
        
        if df_previsao is not None and not df_previsao.empty:
            
            salvar_predicao_mysql(df_previsao, MYSQL_CONFIG, TABELA_PREDICAO_DESTINO)
            
            print("\nFLUXO DE PREDIÇÃO POR VENDEDORA ATIVA CONCLUÍDO.")