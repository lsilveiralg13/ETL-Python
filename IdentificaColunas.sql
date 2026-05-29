SELECT 
    TABLE_NAME AS [Nome da Tabela],
    COLUMN_NAME AS [Nome da Coluna],
    DATA_TYPE AS [Tipo de Dados],
    ORDINAL_POSITION AS [Ordem]
FROM 
    INFORMATION_SCHEMA.COLUMNS
WHERE 
    TABLE_NAME IN (
        'dim_alocacao_posto_trabaho', -- Nota: verifique se há um 'l' faltando em 'trabalho' no seu banco físico, na imagem parece grafado assim.
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
ORDER BY 
    TABLE_NAME, 
    ORDINAL_POSITION;