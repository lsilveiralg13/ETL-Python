CREATE DEFINER=`root`@`localhost` PROCEDURE `Recorrencia_R$_30D`()
BEGIN
    -- 1. Definindo qual mês exibir (Fallback Inteligente)
    SET @mes_alvo = MONTH(CURDATE());
    SET @ano_alvo = YEAR(CURDATE());

    -- Se não houver faturamento no mês atual, retroage 1 mês
    IF (SELECT COUNT(*) FROM staging_faturamento_multimarcas 
        WHERE MONTH(data_negociacao) = @mes_alvo AND YEAR(data_negociacao) = @ano_alvo) = 0 THEN
        SET @mes_alvo = MONTH(CURDATE() - INTERVAL 1 MONTH);
        SET @ano_alvo = YEAR(CURDATE() - INTERVAL 1 MONTH);
    END IF;

    -- 2. Execução da Query com o mês definido acima
    SELECT vendedor, `0`, `1`, `2`, `>=3`, Total
    FROM (
        WITH base_calculo AS (
            SELECT  
                srm.vendedor,
                TRIM(srm.grupo_recorrencia) AS grupo_recorrencia,
                sfm.valor_faturado
            FROM staging_faturamento_multimarcas sfm
            INNER JOIN staging_recorrencia_multimarcas srm ON sfm.codigo_parceiro = srm.codigo_parceiro
            WHERE sfm.status_nfe = 'Aprovada'
              AND MONTH(sfm.data_negociacao) = @mes_alvo
              AND YEAR(sfm.data_negociacao) = @ano_alvo
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
            CONCAT('---TOTAL EQUIPE (MÊS ', @mes_alvo, ')---'),
            CONCAT('R$ ', FORMAT(IFNULL(SUM(v0),0), 2, 'de_DE')),
            CONCAT('R$ ', FORMAT(IFNULL(SUM(v1),0), 2, 'de_DE')),
            CONCAT('R$ ', FORMAT(IFNULL(SUM(v2),0), 2, 'de_DE')),
            CONCAT('R$ ', FORMAT(IFNULL(SUM(v3),0), 2, 'de_DE')),
            CONCAT('R$ ', FORMAT(IFNULL(SUM(vTotal),0), 2, 'de_DE')),
            -1,
            1
        FROM consolidado
    ) AS resultado_final
    ORDER BY ordem ASC, valor_ordenado DESC;

END;