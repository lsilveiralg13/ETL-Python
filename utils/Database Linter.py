import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import track

console = Console()

def manage_single_backup(engine, table_name):
    """
    Cria o backup 'old_' e limpa qualquer rastro de bkp_ antigo.
    """
    backup_simples = f"old_{table_name}"
    # Se a tabela processada já for um backup (ex: bkp_tabela), 
    # extraímos o nome real dela para o backup novo não ficar 'old_bkp_tabela'
    nome_limpo = table_name.replace('bkp_', '').replace('old_', '')
    backup_final = f"old_{nome_limpo}"
    
    try:
        with engine.connect() as conn:
            # 1. Cria o backup novo a partir da tabela atual (mesmo que ela se chame bkp_...)
            conn.execute(text(f"DROP TABLE IF EXISTS `{backup_final}`"))
            conn.execute(text(f"CREATE TABLE `{backup_final}` AS SELECT * FROM `{table_name}`"))
            
            # 2. Se a tabela que acabamos de processar era uma com nome 'sujo' (bkp_...),
            # e conseguimos criar o 'old_', agora podemos apagar a suja com segurança.
            if table_name.startswith('bkp_'):
                conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
            
            conn.commit()
            return f"[green]Backup criado: {backup_final}[/green]"
    except Exception as e:
        return f"[red]Falha no backup: {str(e)}[/red]"

def auto_fix_structure(engine, table_name, columns, pk_cols):
    """Garante PK e ID."""
    actions = []
    cols_names = [c['name'].lower() for c in columns]
    
    try:
        with engine.connect() as conn:
            if not pk_cols:
                if 'id' in cols_names:
                    conn.execute(text(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (`id`)"))
                    actions.append("ID definido como PK.")
                else:
                    conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `id_dli` BIGINT AUTO_INCREMENT PRIMARY KEY FIRST"))
                    actions.append("PK 'id_dli' criada.")
            conn.commit()
            return actions if actions else ["Estrutura OK"]
    except Exception as e:
        return [f"[red]Erro: {str(e)}[/red]"]

def audit_and_fix_database(db_url):
    try:
        engine = create_engine(db_url)
        inspector = inspect(engine)
        all_tables = inspector.get_table_names()

        # AJUSTE NO FILTRO: 
        # Agora permitimos processar tabelas que começam com 'bkp_' para que possamos
        # transformá-las em 'old_'. Só ignoramos as que já são 'old_'.
        tables_to_process = [t for t in all_tables if not t.startswith('old_')]

        console.print(Panel.fit("[bold white on blue] DLI - REPARAÇÃO E CONVERSÃO DE BACKUPS [/bold white on blue]", 
                                subtitle="Convertendo bkp_ em old_ e limpando o Schema"))

        for table_name in track(tables_to_process, description="Processando tabelas..."):
            resumo_table = Table(title=f"\n[bold cyan]Tabela: {table_name}[/bold cyan]")
            resumo_table.add_column("Ação")
            resumo_table.add_column("Status")

            # 1. Cria o Backup 'old_' a partir da tabela encontrada (seja ela bkp_ ou normal)
            bkp_info = manage_single_backup(engine, table_name)

            # 2. Se for uma tabela de dados (não era bkp_), aplicamos as correções de estrutura
            reparo_estrutural = ["Ignorado (é tabela de backup)"]
            if not table_name.startswith('bkp_'):
                columns = inspector.get_columns(table_name)
                pk = inspector.get_pk_constraint(table_name)
                reparo_estrutural = auto_fix_structure(engine, table_name, columns, pk['constrained_columns'])
            
            resumo_table.add_row("Backup/Conversão", bkp_info)
            resumo_table.add_row("PK/Integridade", ", ".join(reparo_estrutural))

            console.print(resumo_table)

        console.print(Panel("[bold green]✅ CONCLUÍDO! Backups convertidos para 'old_' e nomes sujos removidos.[/bold green]"))

    except OperationalError as e:
        console.print(f"[red]Erro de Conexão: {e}[/red]")

# --- CONFIGURAÇÃO ---
DB_URL = 'mysql+pymysql://root:root@localhost:3306/faturamento_multimarcas_dw'

if __name__ == "__main__":
    audit_and_fix_database(DB_URL)