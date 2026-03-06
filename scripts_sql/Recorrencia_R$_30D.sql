CREATE DEFINER=`root`@`localhost` PROCEDURE `Recorrencia_R$_30D`(
    IN p_mes INT, -- Parâmetro de Mês (Ex: 2)
    IN p_ano INT  -- Parâmetro de Ano (Ex: 2026)
)
BEGIN
    DECLARE v_mes_alvo INT;
    DECLARE v_ano_alvo INT;

    -- 1. LÓGICA DE DEFINIÇÃO DO PERÍODO (Fallback Inteligente)
    -- Se os parâmetros forem passados, usa eles. Se forem NULL, usa a data atual.
    SET v_mes_alvo = COALESCE(p_mes, MONTH(CURDATE()));
    SET v_ano_alvo = COALESCE(p_ano, YEAR(CURDATE()));

    -- Se não houver faturamento no período definido, retroage 1 mês automaticamente
    IF (SELECT COUNT(*) FROM staging_faturamento_multimarcas 
        WHERE MONTH(data_negociacao) = v_mes_alvo AND YEAR(data_negociacao) = v_ano_alvo) = 0 
        AND p_mes IS NULL THEN -- Só retroage se o usuário não forçou um mês específico
        
        SET v_mes_alvo = MONTH(CURDATE() - INTERVAL 1 MONTH);
        SET v_ano_alvo = YEAR(CURDATE() - INTERVAL 1 MONTH);
    END IF;

    -- 2. EXECUÇÃO DA QUERY
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
              AND MONTH(sfm.data_negociacao) = v_mes_alvo
              AND YEAR(sfm.data_negociacao) = v_ano_alvo
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
            CONCAT('---TOTAL EQUIPE (MÊS ', v_mes_alvo, '/', v_ano_alvo, ')---'),
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