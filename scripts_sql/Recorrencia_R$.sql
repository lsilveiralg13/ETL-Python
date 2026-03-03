CREATE DEFINER=`root`@`localhost` PROCEDURE `Recorrencia_R$`()
BEGIN

SELECT vendedor, `0`, `1`, `2`, `>=3`, Total
FROM (
    WITH base_financeira AS (
        SELECT
            srm.vendedor,
            SUM(CASE WHEN srm.grupo_recorrencia = '0' THEN IFNULL(sfm.valor_faturado, 0) ELSE 0 END) AS v0,
            SUM(CASE WHEN srm.grupo_recorrencia = '1' THEN IFNULL(sfm.valor_faturado, 0) ELSE 0 END) AS v1,
            SUM(CASE WHEN srm.grupo_recorrencia = '2' THEN IFNULL(sfm.valor_faturado, 0) ELSE 0 END) AS v2,
            SUM(CASE WHEN srm.grupo_recorrencia = '>=3' THEN IFNULL(sfm.valor_faturado, 0) ELSE 0 END) AS v3,
            SUM(CASE WHEN srm.grupo_recorrencia IN ('1', '2', '>=3') THEN IFNULL(sfm.valor_faturado, 0) ELSE 0 END) AS vTotal
        FROM staging_recorrencia_multimarcas srm
        LEFT JOIN staging_faturamento_multimarcas sfm ON srm.codigo_parceiro = sfm.codigo_parceiro 
            AND sfm.data_negociacao >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
        WHERE srm.grupo_status = 'BASE ATIVA'
          AND srm.regua_cadastro = '>=90D'
          AND srm.vendedor != 'ALEX SANDRO'
        GROUP BY srm.vendedor
    )
    SELECT
        vendedor AS vendedor,
        CONCAT('R$ ', FORMAT(v0, 2, 'de_DE')) AS `0`,
        CONCAT('R$ ', FORMAT(v1, 2, 'de_DE')) AS `1`,
        CONCAT('R$ ', FORMAT(v2, 2, 'de_DE')) AS `2`,
        CONCAT('R$ ', FORMAT(v3, 2, 'de_DE')) AS `>=3`,
        CONCAT('R$ ', FORMAT(vTotal, 2, 'de_DE')) AS Total,
        vTotal AS valor_ordenado,
        0 AS ordem
    FROM base_financeira

    UNION ALL
    
    SELECT
        '---TOTAL DA EQUIPE---' AS vendedor,
        CONCAT('R$ ', FORMAT(SUM(v0), 2, 'de_DE')) AS `0`,
        CONCAT('R$ ', FORMAT(SUM(v1), 2, 'de_DE')) AS `1`,
        CONCAT('R$ ', FORMAT(SUM(v2), 2, 'de_DE')) AS `2`,
        CONCAT('R$ ', FORMAT(SUM(v3), 2, 'de_DE')) AS `>=3`,
        CONCAT('R$ ', FORMAT(SUM(vTotal), 2, 'de_DE')) AS Total,
        -1 AS valor_ordenado,
        1 AS ordem
    FROM base_financeira
) AS resultado_final
ORDER BY ordem ASC, valor_ordenado DESC;

END;