CREATE DEFINER=`root`@`localhost` PROCEDURE `Mix_Anual_MM`(
    IN p_ano INT -- Ano para a análise (ex: 2023, 2024)
)
BEGIN
    SET lc_time_names = 'pt_BR';

    WITH MonthlyData AS (
        SELECT
            smpv.chave_ano,
            smpv.chave_mes,
            COALESCE(SUM(CASE WHEN smpv.macrogrupo_produto = 'SAPATOS' THEN smpv.quantidade_total_item ELSE 0 END), 0) AS `QTD Sapatos`,
            -- CÁLCULO DO PREÇO MÉDIO (VALOR TOTAL / QUANTIDADE TOTAL) PARA SAPATOS
            COALESCE(SUM(CASE WHEN smpv.macrogrupo_produto = 'SAPATOS' THEN smpv.valor_total_item ELSE 0 END) / 
                     NULLIF(SUM(CASE WHEN smpv.macrogrupo_produto = 'SAPATOS' THEN smpv.quantidade_total_item ELSE 0 END), 0), 0) AS `Media Preco Sapatos`,
            COALESCE(SUM(CASE WHEN smpv.macrogrupo_produto = 'BOLSAS' THEN smpv.quantidade_total_item ELSE 0 END), 0) AS `QTD Bolsas`,
            COALESCE(SUM(CASE WHEN smpv.macrogrupo_produto IN ('BOLSAS', 'SAPATOS') THEN smpv.quantidade_total_item ELSE 0 END), 0) AS `Total B/S`,
            (SUM(CASE WHEN smpv.macrogrupo_produto = 'BOLSAS' THEN smpv.quantidade_total_item ELSE 0 END) /
             NULLIF(SUM(CASE WHEN smpv.macrogrupo_produto IN ('BOLSAS', 'SAPATOS') THEN smpv.quantidade_total_item ELSE 0 END), 0)) * 100 AS `Pct Bolsas`,
            (SUM(CASE WHEN smpv.macrogrupo_produto = 'SAPATOS' THEN smpv.quantidade_total_item ELSE 0 END) /
             NULLIF(SUM(CASE WHEN smpv.macrogrupo_produto IN ('BOLSAS', 'SAPATOS') THEN smpv.quantidade_total_item ELSE 0 END), 0)) * 100 AS `Pct Sapatos`
        FROM
            staging_mix_produtos_vendidos smpv
        WHERE
            smpv.chave_ano = p_ano
            AND smpv.macrogrupo_produto IN ('BOLSAS', 'SAPATOS')
            AND smpv.quantidade_total_item > 0
        GROUP BY
            smpv.chave_ano,
            smpv.chave_mes
    )
    SELECT
        `Período`,
        `QTD Sapatos`,
        `Preço Médio Sapatos`, -- Nova coluna incluída
        `QTD Bolsas`,
        `Total B/S`,
        `% Bolsas/Sapatos`,
        `% Sapatos/Bolsas`
    FROM (
        SELECT
            CONCAT(md.chave_mes, ' (', md.chave_ano, ')') AS `Período`,
            md.`QTD Sapatos`,
            CONCAT('R$ ', FORMAT(md.`Media Preco Sapatos`, 2, 'de_DE')) AS `Preço Médio Sapatos`,
            md.`QTD Bolsas`,
            md.`Total B/S`,
            CONCAT(FORMAT(md.`Pct Bolsas`, 2), '%') AS `% Bolsas/Sapatos`,
            CONCAT(FORMAT(md.`Pct Sapatos`, 2), '%') AS `% Sapatos/Bolsas`,
            md.chave_ano AS `_sort_ano`,
            FIELD(md.chave_mes,
                  'JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO',
                  'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO') AS `_sort_mes_num`,
            1 AS sort_order
        FROM
            MonthlyData md
        UNION ALL
        SELECT
            'MÉDIA ANUAL' AS `Período`,
            FORMAT(AVG(md.`QTD Sapatos`), 0, 'de_DE') AS `QTD Sapatos`,
            CONCAT('R$ ', FORMAT(AVG(md.`Media Preco Sapatos`), 2, 'de_DE')) AS `Preço Médio Sapatos`,
            FORMAT(AVG(md.`QTD Bolsas`), 0, 'de_DE') AS `QTD Bolsas`,
            FORMAT(AVG(md.`Total B/S`), 0, 'de_DE') AS `Total B/S`,
            CONCAT(FORMAT(AVG(md.`Pct Bolsas`), 2), '%') AS `% Bolsas/Sapatos`,
            CONCAT(FORMAT(AVG(md.`Pct Sapatos`), 2), '%') AS `% Sapatos/Bolsas`,
            p_ano AS `_sort_ano`,
            13 AS `_sort_mes_num`,
            2 AS sort_order
        FROM
            MonthlyData md
    ) AS final_results
    ORDER BY
        sort_order,
        _sort_ano,
        _sort_mes_num;

END;