CREATE DEFINER=`root`@`localhost` PROCEDURE `Recorrencia_TKM`()
BEGIN

SELECT vendedor, `0`, `1`, `2`, `>=3`, Total
FROM (
    WITH base_tkm AS (
        SELECT
            srm.vendedor,
            SUM(CASE WHEN srm.grupo_recorrencia = '0' THEN IFNULL(sfm.valor_faturado, 0) ELSE 0 END) / 
                NULLIF(SUM(CASE WHEN srm.grupo_recorrencia = '0' THEN 1 ELSE 0 END), 0) AS tkm0,
            
            SUM(CASE WHEN srm.grupo_recorrencia = '1' THEN IFNULL(sfm.valor_faturado, 0) ELSE 0 END) / 
                NULLIF(SUM(CASE WHEN srm.grupo_recorrencia = '1' THEN 1 ELSE 0 END), 0) AS tkm1,
            
            SUM(CASE WHEN srm.grupo_recorrencia = '2' THEN IFNULL(sfm.valor_faturado, 0) ELSE 0 END) / 
                NULLIF(SUM(CASE WHEN srm.grupo_recorrencia = '2' THEN 1 ELSE 0 END), 0) AS tkm2,
            
            SUM(CASE WHEN srm.grupo_recorrencia = '>=3' THEN IFNULL(sfm.valor_faturado, 0) ELSE 0 END) / 
                NULLIF(SUM(CASE WHEN srm.grupo_recorrencia = '>=3' THEN 1 ELSE 0 END), 0) AS tkm3,
            
            SUM(CASE WHEN srm.grupo_recorrencia IN ('1', '2', '>=3') THEN IFNULL(sfm.valor_faturado, 0) ELSE 0 END) / 
                NULLIF(SUM(CASE WHEN srm.grupo_recorrencia IN ('1', '2', '>=3') THEN 1 ELSE 0 END), 0) AS tkmTotal
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
        CONCAT('R$ ', FORMAT(IFNULL(tkm0, 0), 2, 'de_DE')) AS `0`,
        CONCAT('R$ ', FORMAT(IFNULL(tkm1, 0), 2, 'de_DE')) AS `1`,
        CONCAT('R$ ', FORMAT(IFNULL(tkm2, 0), 2, 'de_DE')) AS `2`,
        CONCAT('R$ ', FORMAT(IFNULL(tkm3, 0), 2, 'de_DE')) AS `>=3`,
        CONCAT('R$ ', FORMAT(IFNULL(tkmTotal, 0), 2, 'de_DE')) AS Total,
        tkmTotal AS valor_ordenado,
        0 AS ordem
    FROM base_tkm

    UNION ALL
    
    SELECT
        '---TKM MÉDIO DA EQUIPE---' AS vendedor,
        CONCAT('R$ ', FORMAT(IFNULL(AVG(tkm0), 0), 2, 'de_DE')) AS `0`,
        CONCAT('R$ ', FORMAT(IFNULL(AVG(tkm1), 0), 2, 'de_DE')) AS `1`,
        CONCAT('R$ ', FORMAT(IFNULL(AVG(tkm2), 0), 2, 'de_DE')) AS `2`,
        CONCAT('R$ ', FORMAT(IFNULL(AVG(tkm3), 0), 2, 'de_DE')) AS `>=3`,
        CONCAT('R$ ', FORMAT(IFNULL(AVG(tkmTotal), 0), 2, 'de_DE')) AS Total,
        -1 AS valor_ordenado,
        1 AS ordem
    FROM base_tkm
) AS resultado_final
ORDER BY ordem ASC, valor_ordenado DESC;

END;