CREATE DEFINER=`root`@`localhost` PROCEDURE `Recorrencia_%`()
BEGIN

SELECT Vendedor, `0`, `1`, `2`, `>=3`, Total
FROM (
    WITH base_vendedores AS (
        SELECT 
            vendedor,
            (SUM(CASE WHEN grupo_recorrencia = '0' THEN qtd_total ELSE 0 END) / SUM(qtd_total) * 100) AS p0,
            (SUM(CASE WHEN grupo_recorrencia = '1' THEN qtd_total ELSE 0 END) / SUM(qtd_total) * 100) AS p1,
            (SUM(CASE WHEN grupo_recorrencia = '2' THEN qtd_total ELSE 0 END) / SUM(qtd_total) * 100) AS p2,
            (SUM(CASE WHEN grupo_recorrencia = '>=3' THEN qtd_total ELSE 0 END) / SUM(qtd_total) * 100) AS p3,
            (SUM(CASE WHEN grupo_recorrencia IN ('1', '2', '>=3') THEN qtd_total ELSE 0 END) / SUM(qtd_total) * 100) AS p_total
        FROM staging_recorrencia_multimarcas
        WHERE grupo_status = 'BASE ATIVA'
          AND regua_cadastro = '>=90D'
          AND vendedor != 'ALEX SANDRO'
        GROUP BY vendedor
    )
    -- Parte dos Vendedores
    SELECT 
        vendedor AS Vendedor,
        CONCAT(ROUND(p0, 2), '%') AS `0`,
        CONCAT(ROUND(p1, 2), '%') AS `1`,
        CONCAT(ROUND(p2, 2), '%') AS `2`,
        CONCAT(ROUND(p3, 2), '%') AS `>=3`,
        CONCAT(ROUND(p_total, 2), '%') AS Total,
        p_total AS valor_ordenacao, -- Usado para o ranking interno
        0 AS ordem 
    FROM base_vendedores

    UNION ALL

    -- Linha de MÉDIA FINAL
    SELECT 
        '--- MÉDIA DA EQUIPE ---' AS Vendedor,
        CONCAT(ROUND(AVG(p0), 2), '%') AS `0`,
        CONCAT(ROUND(AVG(p1), 2), '%') AS `1`,
        CONCAT(ROUND(AVG(p2), 2), '%') AS `2`,
        CONCAT(ROUND(AVG(p3), 2), '%') AS `>=3`,
        CONCAT(ROUND(AVG(p_total), 2), '%') AS Total,
        -1 AS valor_ordenacao,
        1 AS ordem 
    FROM base_vendedores
) AS resultado_final
ORDER BY ordem ASC, valor_ordenacao DESC;
END;