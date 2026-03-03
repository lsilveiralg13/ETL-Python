CREATE DEFINER=`root`@`localhost` PROCEDURE `Recorrencia_R$_30D`()
BEGIN

SELECT vendedor, `0`, `1`, `2`, `>=3`, Total
FROM (
    WITH base_calculo AS (
        SELECT 
            srm.vendedor,
            srm.grupo_recorrencia,
            sfm.valor_faturado
        FROM staging_faturamento_multimarcas sfm
        INNER JOIN staging_recorrencia_multimarcas srm ON sfm.codigo_parceiro = srm.codigo_parceiro
        WHERE sfm.status_nfe = 'Aprovada'
          -- Filtro para pegar apenas o mês atual
          AND MONTH(sfm.data_negociacao) = MONTH(CURDATE())
          AND YEAR(sfm.data_negociacao) = YEAR(CURDATE())
          AND srm.vendedor NOT IN ('ALEX SANDRO', 'MARCELAVAZ')
    ),
    consolidado AS (
        SELECT
            vendedor,
            SUM(CASE WHEN grupo_recorrencia = '0' THEN valor_faturado ELSE 0 END) AS v0,
            SUM(CASE WHEN grupo_recorrencia = '1' THEN valor_faturado ELSE 0 END) AS v1,
            SUM(CASE WHEN grupo_recorrencia = '2' THEN valor_faturado ELSE 0 END) AS v2,
            SUM(CASE WHEN grupo_recorrencia = '>=3' THEN valor_faturado ELSE 0 END) AS v3,
            SUM(CASE WHEN grupo_recorrencia IN ('1', '2', '>=3') THEN valor_faturado ELSE 0 END) AS vTotal
        FROM base_calculo
        GROUP BY vendedor
    )
    SELECT
        vendedor,
        CONCAT('R$ ', FORMAT(v0, 2, 'de_DE')) AS `0`,
        CONCAT('R$ ', FORMAT(v1, 2, 'de_DE')) AS `1`,
        CONCAT('R$ ', FORMAT(v2, 2, 'de_DE')) AS `2`,
        CONCAT('R$ ', FORMAT(v3, 2, 'de_DE')) AS `>=3`,
        CONCAT('R$ ', FORMAT(vTotal, 2, 'de_DE')) AS Total,
        vTotal AS valor_ordenado,
        0 AS ordem
    FROM consolidado

    UNION ALL
    
    SELECT
        '---TOTAL DA EQUIPE---',
        CONCAT('R$ ', FORMAT(SUM(v0), 2, 'de_DE')),
        CONCAT('R$ ', FORMAT(SUM(v1), 2, 'de_DE')),
        CONCAT('R$ ', FORMAT(SUM(v2), 2, 'de_DE')),
        CONCAT('R$ ', FORMAT(SUM(v3), 2, 'de_DE')),
        CONCAT('R$ ', FORMAT(SUM(vTotal), 2, 'de_DE')),
        -1,
        1
    FROM consolidado
) AS resultado_final
ORDER BY ordem ASC, valor_ordenado DESC;

END;