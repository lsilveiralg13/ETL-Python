import os
import requests
from typing import Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- BIBLIOTECAS DE ESTILIZAÇÃO DO TERMINAL ---
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

# Inicializa o console visual do Rich
console = Console()


class DatabricksDiagnosticTool:
    def __init__(
        self,
        databricks_host: Optional[str] = None,
        databricks_token: Optional[str] = None,
        timeout: int = 10,
        max_retries: int = 3
    ):
        self.host = (databricks_host or os.getenv("DATABRICKS_HOST", "")).rstrip("/")
        if self.host.startswith("https://"):
            self.host = self.host.replace("https://", "")
            
        self.token = databricks_token or os.getenv("DATABRICKS_TOKEN", "")
        self.timeout = timeout

        self.session = requests.Session()
        retries = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def check_azure_databricks_platform_status(
        self, 
        service_id: str = "5d49ec10226b9e13cb6a422e"
    ) -> Dict[str, Any]:
        url = f"https://status.azuredatabricks.net/1.0/status/{service_id}"
        
        with console.status("[bold blue]Consultando status global do Azure Databricks...", spinner="dots"):
            try:
                response = self.session.get(url, timeout=self.timeout)
                status_code = response.status_code

                if status_code == 200:
                    return {
                        "status": "OPERACIONAL",
                        "code": status_code,
                        "details": "Todos os sistemas operando normalmente.",
                        "raw": response.json()
                    }
                else:
                    return {
                        "status": "INSTÁVEL",
                        "code": status_code,
                        "details": response.text,
                        "raw": {}
                    }
            except Exception as e:
                return {
                    "status": "ERRO DE CONEXÃO",
                    "code": "N/A",
                    "details": str(e),
                    "raw": {}
                }

    def cancel_sql_statement(self, sql_statement_id: str) -> Dict[str, Any]:
        if not self.host or not self.token:
            return {
                "status": "CONFIGURAÇÃO INVÁLIDA",
                "code": "N/A",
                "details": "DATABRICKS_HOST ou DATABRICKS_TOKEN não informados."
            }

        url = f"https://{self.host}/api/2.0/sql/statements/{sql_statement_id}/cancel"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        with console.status(f"[bold cyan]Cancelando SQL Statement [yellow]'{sql_statement_id}'[/yellow]...", spinner="dots"):
            try:
                response = self.session.post(url, headers=headers, timeout=self.timeout)
                code = response.status_code

                if code == 200:
                    return {
                        "status": "CANCELADO COM SUCESSO",
                        "code": code,
                        "details": "A instrução SQL foi interrompida no workspace."
                    }
                elif code == 404:
                    return {
                        "status": "NÃO ENCONTRADO / JÁ FINALIZADO",
                        "code": code,
                        "details": "O ID do statement não existe ou a query já havia terminado."
                    }
                elif code == 401:
                    return {
                        "status": "FALHA DE AUTENTICAÇÃO",
                        "code": code,
                        "details": "Token PAT inválido ou expirado."
                    }
                else:
                    return {
                        "status": "FALHA NO CANCELAMENTO",
                        "code": code,
                        "details": response.text
                    }
            except Exception as e:
                return {
                    "status": "ERRO DE REDE",
                    "code": "N/A",
                    "details": str(e)
                }

    def print_pretty_report(self, statement_id: Optional[str] = None):
        """Imprime um dashboard visual limpo no terminal."""
        
        # Cabeçalho
        console.print(
            Panel.fit(
                "[bold white]🔍 DIAGNÓSTICO DE INFRAESTRUTURA & EXECUÇÃO - DATABRICKS[/bold white]",
                style="bold magenta",
                border_style="bright_blue"
            )
        )

        # 1. Checa Plataforma
        platform_res = self.check_azure_databricks_platform_status()
        
        # 2. Checa Cancelamento (se houver ID)
        sql_res = self.cancel_sql_statement(statement_id) if statement_id else None

        # Montagem da Tabela Visual
        table = Table(
            title="[bold]Resumo dos Testes[/bold]", 
            box=box.ROUNDED, 
            header_style="bold cyan",
            expand=True
        )
        
        table.add_column("Módulo de Teste", style="white", justify="left")
        table.add_column("Status / Resultado", justify="center")
        table.add_column("Código HTTP", justify="center")
        table.add_column("Detalhamento Operacional", style="dim")

        # Linha 1: Azure Platform Status
        p_status = platform_res["status"]
        p_color = "green" if p_status == "OPERACIONAL" else "red"
        table.add_row(
            "🌐 Azure Databricks Status API",
            f"[{p_color}][bold]{p_status}[/bold][/{p_color}]",
            str(platform_res["code"]),
            platform_res["details"]
        )

        # Linha 2: SQL Cancellation Status (opcional)
        if sql_res:
            s_status = sql_res["status"]
            if "SUCESSO" in s_status:
                s_color = "green"
            elif "NÃO ENCONTRADO" in s_status:
                s_color = "yellow"
            else:
                s_color = "red"

            table.add_row(
                f"🛑 SQL Statement Cancel\n[dim]ID: {statement_id}[/dim]",
                f"[{s_color}][bold]{s_status}[/bold][/{s_color}]",
                str(sql_res["code"]),
                sql_res["details"]
            )

        # Exibe a Tabela
        console.print(table)
        console.print("\n")


# --- EXECUÇÃO ---
if __name__ == "__main__":
    # Instancia a ferramenta
    diag = DatabricksDiagnosticTool(
        databricks_host="adb-123456789.11.azuredatabricks.net",
        databricks_token="dapixxxxxxxxxxxxxxxxxxxxxxxx"
    )

    # Roda o relatório formatado para o terminal
    diag.print_pretty_report(statement_id="01ee82f4-8a8f-1234-abcd-1234567890ab")