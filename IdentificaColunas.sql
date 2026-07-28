SELECT 
    TABLE_SCHEMA AS [Schema],
    TABLE_NAME AS [Nome da Tabela],
    COLUMN_NAME AS [Nome da Coluna],
    DATA_TYPE AS [Tipo de Dados],
    ORDINAL_POSITION AS [Ordem]
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'producao'
  AND TABLE_NAME IN (
        'fato_ordem_producao_ciclo_seriemp',
		'fato_ordem_producao_seriepa_ciclo',
		'dim_produto_receita_componente',
		'fato_ordem_producao_separacao_item',
		'fato_ordem_producao_separacao_cabecalho',
		'fato_materia_prima_serie',
		'dim_posto_trabalho_old',
		'fato_apontamento_produto_old',
		'fato_apontamento_old',
		'dim_processo_producao_old',
		'dim_planta_manufatura_old',
		'dim_configuracao_atividade_old',
		'dim_alocacao_posto_trabalho_old'
    )
ORDER BY
    TABLE_NAME,
    ORDINAL_POSITION;