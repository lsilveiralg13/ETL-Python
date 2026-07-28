Cap. Total Dinâmica = 

\-- 1. Captura o contexto de tempo da linha atual do gráfico/tabela

VAR \_MesLinhaVisual = MAX('D\_Calendario'\[Mês Número])

VAR \_AnoAtual = MAX('D\_Calendario'\[Ano])

VAR \_PlantaFiltro = SELECTEDVALUE('D\_Planta'\[Planta])



\-- 2. Descobre o último mês que de fato tem produção

VAR \_UltimoMesComDados = 

&#x20;   MAXX(

&#x20;       FILTER(

&#x20;           ALL('D\_Calendario'),

&#x20;           'D\_Calendario'\[Ano] = \_AnoAtual \&\&

&#x20;           \[Qtd\_Produzida\_Dinamica] > 0

&#x20;       ),

&#x20;       'D\_Calendario'\[Mês Número]

&#x20;   )



RETURN

\-- 3. Validação do Teto Dinâmico

IF(

&#x20;   \_MesLinhaVisual > \_UltimoMesComDados,

&#x20;   BLANK(),

&#x20;   

&#x20;   -- Caso contrário, calcula as capacidades pontuais travando estritamente o mês e ano via variáveis

&#x20;   VAR \_CapacAMPontual = 

&#x20;       CALCULATE(

&#x20;           SUM('F\_CapacidadeProdutivaManaus'\[CAPACIDADE]),

&#x20;           'D\_Calendario'\[Mês Número] = \_MesLinhaVisual,

&#x20;           'D\_Calendario'\[Ano] = \_AnoAtual,

&#x20;           ALL('D\_Calendario') -- Limpa qualquer outro filtro temporal que infle o valor

&#x20;       )



&#x20;   VAR \_CapacMGPontual = 

&#x20;       CALCULATE(

&#x20;           SUM('F\_CapacidadeProdutivaContagem'\[CAPACIDADE]),

&#x20;           'D\_Calendario'\[Mês Número] = \_MesLinhaVisual,

&#x20;           'D\_Calendario'\[Ano] = \_AnoAtual,

&#x20;           ALL('D\_Calendario') -- Limpa qualquer outro filtro temporal que infle o valor

&#x20;       )



&#x20;   RETURN

&#x20;   -- 4. Retorna o valor baseado no contexto de Planta

&#x20;   SWITCH(

&#x20;       \_PlantaFiltro,

&#x20;       "MANAUS/AM", \_CapacAMPontual,

&#x20;       "CONTAGEM/MG", \_CapacMGPontual,

&#x20;       COALESCE(\_CapacAMPontual, 0) + COALESCE(\_CapacMGPontual, 0)

&#x20;   )

)









