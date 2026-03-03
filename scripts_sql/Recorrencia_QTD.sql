CREATE DEFINER=`root`@`localhost` PROCEDURE `Recorrencia_QTD`()
BEGIN

SELECT Vendedor, `0`, `1`, `2`, `>=3`, Total
FROM (
    WITH base_vendedores AS (
        SELECT 
            vendedor,
            SUM(CASE WHEN grupo_recorrencia = '0' THEN qtd_total ELSE 0 END) AS q0,
            SUM(CASE WHEN grupo_recorrencia = '1' THEN qtd_total ELSE 0 END) AS q1,
            SUM(CASE WHEN grupo_recorrencia = '2' THEN qtd_total ELSE 0 END) AS q2,
            SUM(CASE WHEN grupo_recorrencia = '>=3' THEN qtd_total ELSE 0 END) AS q3,
            SUM(CASE WHEN grupo_recorrencia IN ('1', '2', '>=3') THEN qtd_total ELSE 0 END) AS q_total
        FROM staging_recorrencia_multimarcas
        WHERE grupo_status = 'BASE ATIVA'
          AND regua_cadastro = '>=90D'
          AND vendedor != 'ALEX SANDRO'
        GROUP BY vendedor
    )
    SELECT 
        vendedor AS Vendedor,
        q0 AS `0`,
        q1 AS `1`,
        q2 AS `2`,
        q3 AS `>=3`,
        q_total AS Total,
        q_total AS valor_ordenacao, 
        0 AS ordem 
    FROM base_vendedores

    UNION ALL

    SELECT 
        '--- TOTAL DA EQUIPE ---' AS Vendedor,
        SUM(q0) AS `0`,
        SUM(q1) AS `1`,
        SUM(q2) AS `2`,
        SUM(q3) AS `>=3`,
        SUM(q_total) AS Total,
        -1 AS valor_ordenacao,
        1 AS ordem 
    FROM base_vendedores
) AS resultado_final
ORDER BY ordem ASC, valor_ordenacao DESC;

END;