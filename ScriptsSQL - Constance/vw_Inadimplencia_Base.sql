CREATE OR REPLACE VIEW vw_inadimplencia_base AS
SELECT 
    vendedor, 
    Ativos, 
    QTD,
    Valor,
    Inadimplencia,
    Limite_Credito
FROM (
    WITH cte_ativos AS (
        SELECT 
            vendedor,
            COUNT(*) AS ativos_num
        FROM staging_financeiro_multimarcas
        WHERE base_ativos = 'BASE ATIVA'
          AND regua_cadastro = '>=90D'
          AND vendedor NOT IN ('ALEX SANDRO')
        GROUP BY vendedor
    ),
    cte_inadimplencia AS (
        SELECT 
            vendedor,
            SUM(CASE WHEN inadimplente = 'S' AND maior_atraso >= 1 AND maior_atraso <= 31 THEN total_em_aberto ELSE 0 END) AS valor_num,
            COUNT(CASE WHEN inadimplente = 'S' AND maior_atraso >= 1 AND maior_atraso <= 31 THEN 1 END) AS qtd_num,
            SUM(limite_total) AS limite_num
        FROM staging_financeiro_multimarcas
        WHERE vendedor NOT IN ('ALEX SANDRO', '<SEM VENDEDOR>')
        GROUP BY vendedor
    )
    SELECT 
        i.vendedor,
        IFNULL(a.ativos_num, 0) AS Ativos,
        i.qtd_num AS QTD,
        CONCAT('R$ ', FORMAT(i.valor_num, 2, 'de_DE')) AS Valor,
        CONCAT(ROUND((i.qtd_num / NULLIF(a.ativos_num, 0)) * 100, 2), '%') AS Inadimplencia,
        CONCAT('R$ ', FORMAT(i.limite_num, 2, 'de_DE')) AS Limite_Credito,
        (i.qtd_num / NULLIF(a.ativos_num, 0)) AS perc_ordenacao,
        0 AS linha_total
    FROM cte_inadimplencia i
    LEFT JOIN cte_ativos a ON i.vendedor = a.vendedor
    UNION ALL
    SELECT 
        '--- TOTAL DA EQUIPE ---',
        SUM(a.ativos_num),
        SUM(i.qtd_num),
        CONCAT('R$ ', FORMAT(SUM(i.valor_num), 2, 'de_DE')),
        CONCAT(ROUND((SUM(i.qtd_num) / NULLIF(SUM(a.ativos_num), 0)) * 100, 2), '%'),
        CONCAT('R$ ', FORMAT(SUM(i.limite_num), 2, 'de_DE')),
        (SUM(i.qtd_num) / NULLIF(SUM(a.ativos_num), 0)) AS perc_ordenacao,
        1 AS linha_total
    FROM cte_inadimplencia i
    LEFT JOIN cte_ativos a ON i.vendedor = a.vendedor
) AS resultado
ORDER BY linha_total ASC, perc_ordenacao DESC;