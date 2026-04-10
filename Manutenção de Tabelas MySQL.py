import sqlalchemy
from sqlalchemy import create_engine, text

# --- Configurações de Conexão ---
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'belmicro'

# Tabelas que aparecem no seu print com "0" índices
TABELAS_PARA_CORRIGIR = [
    'staging_telecontrol',
    'staging_estoque_belmicro',
    'staging_reparos',
    'staging_ocorrencias',
    'staging_reparos_tvs',
    'staging_producao_pcs'
]

def aplicar_indices_pk():
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    try:
        with engine.begin() as conn:
            print(f"🔧 Ajustando índices no banco: {DB_NAME}\n")
            
            for tabela in TABELAS_PARA_CORRIGIR:
                print(f"📦 Verificando {tabela}...")
                
                # 1. Verifica se a coluna 'id' existe
                res = conn.execute(text(f"SHOW COLUMNS FROM {tabela} LIKE 'id'")).fetchone()
                
                if not res:
                    # Caso o ID não exista, cria ele como PK e Auto Increment
                    print(f"  -> Criando coluna 'id' como PRIMARY KEY...")
                    conn.execute(text(f"""
                        ALTER TABLE {tabela} 
                        ADD COLUMN id INT NOT NULL AUTO_INCREMENT FIRST, 
                        ADD PRIMARY KEY (id)
                    """))
                else:
                    # Se o ID já existe mas não é PK (por isso o '0' no seu print)
                    print(f"  -> Coluna 'id' detectada. Convertendo para PRIMARY KEY...")
                    try:
                        conn.execute(text(f"ALTER TABLE {tabela} MODIFY COLUMN id INT NOT NULL AUTO_INCREMENT PRIMARY KEY"))
                    except Exception as e:
                        # Caso já seja PK e você só queira garantir o índice
                        conn.execute(text(f"ALTER TABLE {tabela} ADD PRIMARY KEY (id)"))
                
                print(f"  ✅ {tabela} agora deve exibir '1' na contagem de índices.")

            print(f"\n🚀 Todas as tabelas foram processadas!")

    except Exception as e:
        print(f"❌ ERRO: {e}")

if __name__ == "__main__":
    aplicar_indices_pk()