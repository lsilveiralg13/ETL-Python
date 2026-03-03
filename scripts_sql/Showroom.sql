CREATE DEFINER=`root`@`localhost` PROCEDURE `Showroom`(
    IN p_tipo_showroom VARCHAR(50), -- Parâmetro para o tipo de showroom (ex: 'INVERNO 2025')
    IN p_ano INT                    -- Parâmetro para o ano (ex: 2024, 2025)
)
BEGIN
    SELECT
        COALESCE(ssm_agg.vendedor, sfm_agg.vendedor) AS vendedor,
        CONCAT('R$ ', REPLACE(REPLACE(REPLACE(FORMAT(COALESCE(ssm_agg.total_vendas_showroom, 0), 2), ',', '_TEMP_'), '.', ','), '_TEMP_', '.')) AS Vendas_Showroom,
        CONCAT('R$ ', REPLACE(REPLACE(REPLACE(FORMAT(COALESCE(sfm_agg.total_faturamento, 0), 2), ',', '_TEMP_'), '.', ','), '_TEMP_', '.')) AS Faturamento_Showroom,
        CASE
            WHEN COALESCE(ssm_agg.total_vendas_showroom, 0) > 0 THEN
                CONCAT(FORMAT((COALESCE(sfm_agg.total_faturamento, 0) / ssm_agg.total_vendas_showroom) * 100, 2), '%')
            ELSE
                '0.00%'
        END AS Percentual,
        CASE
            WHEN COALESCE(ssm_agg.total_vendas_showroom, 0) > 0 THEN
                CONCAT(FORMAT(100 - (COALESCE(sfm_agg.total_faturamento, 0) / ssm_agg.total_vendas_showroom) * 100, 2), '%')
            ELSE
                '100.00%'
        END AS Indice_de_Perda,
        CASE
            WHEN COALESCE(ssm_agg.total_vendas_showroom, 0) > 0 THEN
                CASE
                    WHEN (100 - (COALESCE(sfm_agg.total_faturamento, 0) / ssm_agg.total_vendas_showroom) * 100) > 27.6 THEN
                        'BAIXO APROVEITAMENTO'
                    ELSE
                        'BOM APROVEITAMENTO'
                END
            ELSE
                'BAIXO APROVEITAMENTO'
        END AS status_meta
    FROM
        (SELECT
            vendedor,
            SUM(valor_total_showroom) AS total_vendas_showroom
        FROM
            staging_showroom_multimarcas
        WHERE
            chave_ano = p_ano           -- Corrigido para 'chave_ano'
            AND tipo_evento = p_tipo_showroom -- Mantendo 'tipo_evento' (staging_showroom_multimarcas)
        GROUP BY
            vendedor
        ) AS ssm_agg
    LEFT JOIN
        (SELECT
            vendedor,
            SUM(valor_faturado) AS total_faturamento
        FROM
            staging_faturamento_multimarcas
        WHERE
            chave_ano = p_ano               -- Corrigido para 'chave_ano'
            AND tipo_showroom = p_tipo_showroom -- Mantendo 'tipo_showroom' (staging_faturamento_multimarcas)
        GROUP BY
            vendedor
        ) AS sfm_agg ON ssm_agg.vendedor = sfm_agg.vendedor
    ORDER BY
        COALESCE(sfm_agg.total_faturamento, 0) DESC;
END;