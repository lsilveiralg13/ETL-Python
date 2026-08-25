import math
import re
from datetime import date, datetime
from dataclasses import dataclass, field
from typing import List, Dict
import pandas as pd
from tabulate import tabulate
import pdfplumber

# ==========================================
# 1. MODELO DE DADOS
# ==========================================

@dataclass
class PeriodoContribuicao:
    data_inicio: date
    data_fim: date
    eh_especial: bool = False
    fator_conversao: float = 1.0

@dataclass
class Segurado:
    nome: str
    data_nascimento: date
    sexo: str
    salario_media: float = 0.0  # Média dos salários pós-julho/1994
    historico: List[PeriodoContribuicao] = field(default_factory=list)
    questionario_social: Dict[str, any] = field(default_factory=dict)

# ==========================================
# 2. PARSER AUTOMÁTICO DE CNIS (PDF)
# ==========================================

class CNISParser:
    """Extrai os vínculos do PDF do extrato do CNIS."""
    
    @staticmethod
    def extrair_periodos_pdf(caminho_pdf: str) -> List[Dict]:
        vinculos = []
        try:
            with pdfplumber.open(caminho_pdf) as pdf:
                texto_completo = "\n".join([page.extract_text() or "" for page in pdf.pages])
                
            padrao_datas = r'(\d{2}/\d{2}/\d{4})\s*a\s*(\d{2}/\d{2}/\d{4})'
            matches = re.findall(padrao_datas, texto_completo)
            
            for dt_ini_str, dt_fim_str in matches:
                try:
                    dt_ini = datetime.strptime(dt_ini_str, "%d/%m/%Y").date()
                    dt_fim = datetime.strptime(dt_fim_str, "%d/%m/%Y").date()
                    if dt_fim >= dt_ini:
                        vinculos.append({"inicio": dt_ini, "fim": dt_fim})
                except ValueError:
                    continue
                    
        except Exception as e:
            print(f"⚠️ Erro ao processar o arquivo PDF: {e}")
            
        return vinculos

# ==========================================
# 3. MOTOR DE CÁLCULO E VALOR DA APOSENTADORIA
# ==========================================

class MotorCalculoPrevidenciario:
    def __init__(self, segurado: Segurado):
        self.segurado = segurado

    def calcular_tempo_total_dias(self) -> int:
        dias_totais = 0
        for p in self.segurado.historico:
            dias = (p.data_fim - p.data_inicio).days + 1
            if p.eh_especial:
                dias = math.floor(dias * p.fator_conversao)
            dias_totais += dias
        return dias_totais

    def obter_idade_anos(self) -> float:
        dias = (date.today() - self.segurado.data_nascimento).days
        return round(dias / 365.25, 2)

    def calcular_aliquota_beneficio(self) -> float:
        """Calcula a porcentagem da média a receber (60% + 2% por ano excedente)."""
        tempo_anos = self.calcular_tempo_total_dias() / 365.25
        limite_anos = 20 if self.segurado.sexo == 'M' else 15
        
        anos_excedentes = max(0, math.floor(tempo_anos - limite_anos))
        aliquota = 0.60 + (anos_excedentes * 0.02)
        return min(aliquota, 1.0)  # Limite máximo padrão ou sem limite em regras específicas

    def consolidar_cenarios(self) -> pd.DataFrame:
        tempo_anos = round(self.calcular_tempo_total_dias() / 365.25, 2)
        idade_anos = self.obter_idade_anos()
        pontos = round(idade_anos + tempo_anos, 1)
        
        ano_atual = date.today().year
        meta_pontos = (103 if self.segurado.sexo == 'M' else 93) + (ano_atual - 2026)
        meta_tempo = 35 if self.segurado.sexo == 'M' else 30
        meta_idade = 65.0 if self.segurado.sexo == 'M' else 62.0

        # Alíquota geral
        pct_padrao = self.calcular_aliquota_beneficio()
        valor_padrao = self.segurado.salario_media * pct_padrao
        
        # Pedágio 100% garante 100% do salário de benefício
        valor_pedagio_100 = self.segurado.salario_media * 1.00

        cenarios = [
            {
                "Regra": "1. Pontos",
                "Status": "Atingido" if (pontos >= meta_pontos and tempo_anos >= meta_tempo) else "Em Andamento",
                "Alíquota": f"{round(pct_padrao * 100, 1)}%",
                "Valor Estimado": f"R$ {valor_padrao:,.2f}",
                "Exigência": f"{meta_pontos} pts e {meta_tempo} anos tempo"
            },
            {
                "Regra": "2. Idade Mínima Progressiva",
                "Status": "Atingido" if (idade_anos >= meta_idade and tempo_anos >= meta_tempo) else "Em Andamento",
                "Alíquota": f"{round(pct_padrao * 100, 1)}%",
                "Valor Estimado": f"R$ {valor_padrao:,.2f}",
                "Exigência": f"{meta_idade} anos idade e {meta_tempo} anos tempo"
            },
            {
                "Regra": "3. Pedágio 100%",
                "Status": "Atingido" if (idade_anos >= (60.0 if self.segurado.sexo == 'M' else 57.0) and tempo_anos >= meta_tempo) else "Em Andamento",
                "Alíquota": "100.0%",
                "Valor Estimado": f"R$ {valor_pedagio_100:,.2f}",
                "Exigência": f"Idade {(60.0 if self.segurado.sexo == 'M' else 57.0)} + 100% pedágio"
            }
        ]
        return pd.DataFrame(cenarios)

# ==========================================
# 4. CONSULTA INTERATIVA
# ==========================================

def ler_data(mensagem: str) -> date:
    while True:
        try:
            entrada = input(f"{mensagem} (DD/MM/AAAA): ").strip()
            return datetime.strptime(entrada, "%d/%m/%Y").date()
        except ValueError:
            print("❌ Formato inválido! Use DD/MM/AAAA.")

def iniciar_questionario():
    print("=" * 65)
    print("   SIMULADOR PREVIDENCIÁRIO - TEMPO DE SERVIÇO E VALOR DE RMI   ")
    print("=" * 65)
    
    # 1. Dados Pessoais
    nome = input("\n> Nome do Segurado: ").strip()
    data_nascimento = ler_data("> Data de Nascimento")
    
    sexo = input("> Sexo [M/F]: ").strip().upper()
    while sexo not in ['M', 'F']:
        sexo = input("  Digite apenas M ou F: ").strip().upper()

    # Média Salarial para Cálculo de Valores
    try:
        salario_media = float(input("> Informe a média salarial aproximada das contribuições (R$): ").replace('.', '').replace(',', '.'))
    except ValueError:
        salario_media = 3000.0
        print("💡 Valor não reconhecido. Assumindo R$ 3.000,00 como média padrão.")

    # 2. Entrada dos Períodos
    print("\n" + "-" * 45)
    print("  CARREGAMENTO DOS PERÍODOS DE TRABALHO")
    print("-" * 45)
    print("1 - Importar arquivo PDF do CNIS (Extrato do Meu INSS)")
    print("2 - Digitar períodos manualmente")
    
    opcao_carga = input("\n> Escolha o modo de carga (1 ou 2): ").strip()
    historico = []

    if opcao_carga == "1":
        caminho_pdf = input("> Digite o nome ou caminho do arquivo PDF do CNIS (ex: cnis.pdf): ").strip()
        vinculos_extraidos = CNISParser.extrair_periodos_pdf(caminho_pdf)
        
        if vinculos_extraidos:
            print(f"\n✅ {len(vinculos_extraidos)} vínculos encontrados no PDF e importados!")
            for v in vinculos_extraidos:
                historico.append(PeriodoContribuicao(
                    data_inicio=v['inicio'],
                    data_fim=v['fim']
                ))
        else:
            print("⚠️ Nenhum vínculo lido. Alternando para o modo manual.")
            opcao_carga = "2"

    if opcao_carga != "1" or not historico:
        i = 1
        while True:
            print(f"\n📌 Período #{i}")
            data_inicio = ler_data("  Data de Início")
            data_fim = ler_data("  Data de Fim")

            historico.append(PeriodoContribuicao(
                data_inicio=data_inicio,
                data_fim=data_fim
            ))

            if input("\n> Adicionar outro período? [S/N]: ").strip().upper() != 'S':
                break
            i += 1

    # 3. Processamento
    segurado = Segurado(
        nome=nome,
        data_nascimento=data_nascimento,
        sexo=sexo,
        salario_media=salario_media,
        historico=historico
    )

    motor = MotorCalculoPrevidenciario(segurado)
    
    # 4. Resultado Final
    tempo_dias = motor.calcular_tempo_total_dias()
    tempo_anos = round(tempo_dias / 365.25, 2)
    idade = motor.obter_idade_anos()
    
    print("\n" + "=" * 65)
    print("                   DIAGNÓSTICO E ESTIMATIVA DE VALOR            ")
    print("=" * 65)
    print(f"Segurado: {segurado.nome} | Sexo: {segurado.sexo} | Idade: {idade} anos")
    print(f"Tempo Total: {tempo_anos} anos ({tempo_dias} dias)")
    print(f"Média Salarial Base: R$ {salario_media:,.2f}")
    print("-" * 65)
    
    df_resultado = motor.consolidar_cenarios()
    print(tabulate(df_resultado, headers='keys', tablefmt='grid', showindex=False))
    print("\n")

if __name__ == "__main__":
    iniciar_questionario()