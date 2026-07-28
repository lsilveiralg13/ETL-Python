CREATE DEFINER=`root`@`localhost` PROCEDURE `Recorrencia_%_UF`()
BEGIN

SELECT UF, `0`, `1`, `2`, `>=3`, Total
FROM (
    WITH base_uf AS (
        SELECT 
            uf,
            (SUM(CASE WHEN grupo_recorrencia = '0' THEN qtd_total ELSE 0 END) / SUM(qtd_total) * 100) AS p0,
            (SUM(CASE WHEN grupo_recorrencia = '1' THEN qtd_total ELSE 0 END) / SUM(qtd_total) * 100) AS p1,
            (SUM(CASE WHEN grupo_recorrencia = '2' THEN qtd_total ELSE 0 END) / SUM(qtd_total) * 100) AS p2,
            (SUM(CASE WHEN grupo_recorrencia = '>=3' THEN qtd_total ELSE 0 END) / SUM(qtd_total) * 100) AS p3,
            (SUM(CASE WHEN grupo_recorrencia IN ('1', '2', '>=3') THEN qtd_total ELSE 0 END) / SUM(qtd_total) * 100) AS p_total
        FROM staging_recorrencia_multimarcas
        WHERE grupo_status = 'BASE ATIVA'
          AND regua_cadastro = '>=90D'
          AND vendedor NOT IN ('ALEX SANDRO', 'MARCELAVAZ')
        GROUP BY uf
    )
    SELECT 
        uf AS UF,
        CONCAT(ROUND(p0, 2), '%') AS `0`,
        CONCAT(ROUND(p1, 2), '%') AS `1`,
        CONCAT(ROUND(p2, 2), '%') AS `2`,
        CONCAT(ROUND(p3, 2), '%') AS `>=3`,
        CONCAT(ROUND(p_total, 2), '%') AS Total,
        p_total AS valor_ordenacao,
        0 AS ordem 
    FROM base_uf

    UNION ALL

    SELECT 
        '--- MÉDIA BRASIL ---' AS UF,
        CONCAT(ROUND(AVG(NULLIF(p0, 0)), 2), '%') AS `0`,
        CONCAT(ROUND(AVG(NULLIF(p1, 0)), 2), '%') AS `1`,
        CONCAT(ROUND(AVG(NULLIF(p2, 0)), 2), '%') AS `2`,
        CONCAT(ROUND(AVG(NULLIF(p3, 0)), 2), '%') AS `>=3`,
        CONCAT(ROUND(AVG(NULLIF(p_total, 0)), 2), '%') AS Total,
        -1 AS valor_ordenacao,
        1 AS ordem 
    FROM base_uf
) AS resultado_final
ORDER BY ordem ASC, valor_ordenacao DESC;

END;