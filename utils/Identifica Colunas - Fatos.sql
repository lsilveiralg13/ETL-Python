SELECT
    TABLE_NAME AS [Tabela],
    COLUMN_NAME AS [Coluna]
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'producao'
AND TABLE_NAME IN (
    'dim_alocacao_posto_trabalho',
    'dim_configuracao_atividade',
    'dim_planta_manufatura',
    'dim_posto_trabalho',
    'dim_processo_producao',
    'fato_apontamento',
    'fato_apontamento_produto',
    'fato_atividade_op',
    'fato_controle_apontamento',
    'fato_historico_status_serie',
    'fato_instancia_item_nota',
    'fato_ordem_producao',
    'fato_ordem_producao_item',
    'fato_produto_seriepa',
    'fato_rastreabilidade_serie_mp'
)
AND COLUMN_NAME LIKE '%%'
ORDER BY TABLE_NAME, COLUMN_NAME;