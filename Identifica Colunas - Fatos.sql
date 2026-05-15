SELECT
    TABLE_NAME AS [Tabela],
    COLUMN_NAME AS [Coluna]
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'belmicro'
AND TABLE_NAME IN (
    'fato_itens',
    'fato_operacoes',
    'fato_itens_notas_expedidas'
)
AND COLUMN_NAME LIKE '%%'
ORDER BY TABLE_NAME, COLUMN_NAME;