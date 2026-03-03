CREATE DEFINER=`root`@`localhost` PROCEDURE `Mix_Mensal_MM`(
    IN p_data_referencia DATE
)
BEGIN
    SET lc_time_names = 'pt_BR';

    SET @current_year = YEAR(p_data_referencia);
    SET @current_month_name = UCASE(DATE_FORMAT(p_data_referencia, '%M'));

    SET @previous_date = DATE_SUB(p_data_referencia, INTERVAL 1 MONTH);
    SET @previous_year = YEAR(@previous_date);
    SET @previous_month_name = UCASE(DATE_FORMAT(@previous_date, '%M'));

    SELECT
        1 AS sort_order,
        CONCAT(@previous_month_name, ' (', @previous_year, ')') AS `Período`,
        COALESCE(SUM(CASE WHEN smpv.macrogrupo_produto = 'SAPATOS' THEN smpv.quantidade_total_item ELSE 0 END), 0) AS `QTD Sapatos`,
        COALESCE(SUM(CASE WHEN smpv.macrogrupo_produto = 'BOLSAS' THEN smpv.quantidade_total_item ELSE 0 END), 0) AS `QTD Bolsas`,
        CONCAT(
            FORMAT(
                COALESCE(
                    (SUM(CASE WHEN smpv.macrogrupo_produto = 'BOLSAS' THEN smpv.quantidade_total_item ELSE 0 END) /
                     NULLIF(SUM(CASE WHEN smpv.macrogrupo_produto IN ('BOLSAS', 'SAPATOS') THEN smpv.quantidade_total_item ELSE 0 END), 0)) * 100,
                    0
                ), 2
            ), '%'
        ) AS `% Bolsas/Sapatos`,
        CONCAT(
            FORMAT(
                COALESCE(
                    (SUM(CASE WHEN smpv.macrogrupo_produto = 'SAPATOS' THEN smpv.quantidade_total_item ELSE 0 END) /
                     NULLIF(SUM(CASE WHEN smpv.macrogrupo_produto IN ('BOLSAS', 'SAPATOS') THEN smpv.quantidade_total_item ELSE 0 END), 0)) * 100,
                    0
                ), 2
            ), '%'
        ) AS `% Sapatos/Bolsas`
    FROM
        staging_mix_produtos_vendidos smpv
    WHERE
        smpv.chave_ano = @previous_year
        AND smpv.chave_mes = @previous_month_name
        AND smpv.macrogrupo_produto IN ('BOLSAS', 'SAPATOS')
        AND smpv.quantidade_total_item > 0

    UNION ALL

    SELECT
        2 AS sort_order,
        CONCAT(@current_month_name, ' (', @current_year, ')') AS `Período`,
        COALESCE(SUM(CASE WHEN smpv.macrogrupo_produto = 'SAPATOS' THEN smpv.quantidade_total_item ELSE 0 END), 0) AS `QTD Sapatos`,
        COALESCE(SUM(CASE WHEN smpv.macrogrupo_produto = 'BOLSAS' THEN smpv.quantidade_total_item ELSE 0 END), 0) AS `QTD Bolsas`,
        CONCAT(
            FORMAT(
                COALESCE(
                    (SUM(CASE WHEN smpv.macrogrupo_produto = 'BOLSAS' THEN smpv.quantidade_total_item ELSE 0 END) /
                     NULLIF(SUM(CASE WHEN smpv.macrogrupo_produto IN ('BOLSAS', 'SAPATOS') THEN smpv.quantidade_total_item ELSE 0 END), 0)) * 100,
                    0
                ), 2
            ), '%'
        ) AS `% Bolsas/Sapatos`,
        CONCAT(
            FORMAT(
                COALESCE(
                    (SUM(CASE WHEN smpv.macrogrupo_produto = 'SAPATOS' THEN smpv.quantidade_total_item ELSE 0 END) /
                     NULLIF(SUM(CASE WHEN smpv.macrogrupo_produto IN ('BOLSAS', 'SAPATOS') THEN smpv.quantidade_total_item ELSE 0 END), 0)) * 100,
                    0
                ), 2
            ), '%'
        ) AS `% Sapatos/Bolsas`
    FROM
        staging_mix_produtos_vendidos smpv
    WHERE
        smpv.chave_ano = @current_year
        AND smpv.chave_mes = @current_month_name
        AND smpv.macrogrupo_produto IN ('BOLSAS', 'SAPATOS')
        AND smpv.quantidade_total_item > 0
    ORDER BY
        sort_order;

END;