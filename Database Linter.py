import os
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import track

console = Console()

def audit_database(db_url):
    try:
        # Tenta criar a conexão
        engine = create_engine(db_url)
        inspector = inspect(engine)
        suggestions = []

        console.print(Panel.fit("[bold blue]Auditoria de Integridade - Data Warehouse[/bold blue]", subtitle="Análise de Estrutura e Dados"))

        # Obtém a lista de tabelas
        tables = inspector.get_table_names()
        
        if not tables:
            console.print("[yellow]Nenhuma tabela encontrada no banco de dados informado.[/yellow]")
            return

        for table_name in track(tables, description="Analisando tabelas..."):
            # Tabela visual para o terminal
            resumo_table = Table(title=f"\n[bold magenta]Tabela: {table_name}[/bold magenta]")
            resumo_table.add_column("Critério", style="cyan")
            resumo_table.add_column("Status", justify="center")
            resumo_table.add_column("Diagnóstico", style="yellow")

            # 1. Verificação de Chave Primária (PK)
            pk = inspector.get_pk_constraint(table_name)
            if pk['constrained_columns']:
                resumo_table.add_row("Chave Primária", "[green]OK[/green]", f"Definida em: {', '.join(pk['constrained_columns'])}")
            else:
                resumo_table.add_row("Chave Primária", "[red]FALHA[/red]", "Tabela sem identificador único.")
                suggestions.append({"tipo": "ESTRUTURA", "msg": f"Tabela '{table_name}' não possui PK. Isso pode causar duplicidade."})

            # 2. Verificação de Índices
            indexes = inspector.get_indexes(table_name)
            if indexes:
                resumo_table.add_row("Indexação", "[green]OK[/green]", f"{len(indexes)} índice(s) ativo(s).")
            else:
                resumo_table.add_row("Indexação", "[yellow]ALERTA[/yellow]", "Sem índices de busca.")
                suggestions.append({"tipo": "PERFORMANCE", "msg": f"Tabela '{table_name}' sem índices. Consultas podem ficar lentas."})

            # 3. Análise de Dados (Amostra de 500 linhas)
            try:
                df = pd.read_sql(text(f"SELECT * FROM {table_name} LIMIT 500"), engine.connect())
                
                # Checar Nulos
                null_counts = df.isnull().sum()
                critical_nulls = null_counts[null_counts > (len(df) * 0.3)]
                
                if critical_nulls.empty:
                    resumo_table.add_row("Densidade de Dados", "[green]OK[/green]", "Dados bem preenchidos.")
                else:
                    resumo_table.add_row("Densidade de Dados", "[red]CRÍTICO[/red]", f"{len(critical_nulls)} colunas com +30% nulos.")
                    for col in critical_nulls.index:
                        suggestions.append({"tipo": "DADOS", "msg": f"Coluna '{col}' em '{table_name}' tem alto índice de vazios."})
            
            except Exception as e:
                resumo_table.add_row("Leitura de Dados", "[red]ERRO[/red]", str(e))

            console.print(resumo_table)

        # Painel Final de Recomendações
        if suggestions:
            sug_text = "\n".join([f"• [bold yellow]{s['tipo']}:[/bold yellow] {s['msg']}" for s in suggestions])
            console.print(Panel(sug_text, title="[bold red]📋 Plano de Ação Recomendado[/bold red]", border_style="red"))
        else:
            console.print(Panel("[bold green]✅ Tudo certo! O banco segue as melhores práticas.[/bold green]"))

    except OperationalError as e:
        console.print(Panel(f"[red]Erro de Conexão:[/red]\n{e}", title="Falha de Autenticação", border_style="red"))

# --- CONFIGURAÇÃO DE ACESSO ---
# Substitua root e a senha pelos seus dados do MySQL
USUARIO = 'root'
SENHA = 'root' 
HOST = 'localhost'
PORTA = '3306'
BANCO = 'faturamento_multimarcas_dw'

DB_URL = f'mysql+pymysql://{USUARIO}:{SENHA}@{HOST}:{PORTA}/{BANCO}'

if __name__ == "__main__":
    audit_database(DB_URL)