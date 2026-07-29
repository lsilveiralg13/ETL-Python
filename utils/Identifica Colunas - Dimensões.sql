SELECT
    TABLE_NAME AS [Tabela],
    COLUMN_NAME AS [Coluna]
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'cadastros'
AND TABLE_NAME IN (
    'dim_barros',
    'dim_cidades',
    'dim_componentes',
    'dim_enderecos',
    'dim_grupo_produtos',
    'dim_lojas_mkt',
    'dim_parceiros',
    'dim_produtos',
    'dim_projetos',
    'dim_regioes',
    'dim_uf',
    'dim_vendedor'
)
AND COLUMN_NAME LIKE '%%'
ORDER BY TABLE_NAME, COLUMN_NAME;