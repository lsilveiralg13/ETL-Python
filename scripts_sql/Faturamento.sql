CREATE DEFINER=`root`@`localhost` PROCEDURE `Faturamento`(
    IN p_chave_mes VARCHAR(20),
    IN p_chave_ano INT,
    IN p_tipo_venda VARCHAR(30)
)
BEGIN
    -- 1. QUERY PRINCIPAL UNIFICADA
    WITH metas_validas AS (
        SELECT 
            TRIM(UPPER(vendedor)) AS vendedor_join,
            vendedor AS vendedor_original,
            CASE 
                WHEN UPPER(p_tipo_venda) IN ('TODOS', 'TODAS') THEN COALESCE(meta_valor, 0)
                WHEN UPPER(p_tipo_venda) = 'PRONTA ENTREGA' THEN COALESCE(meta_prontaentrega, 0)
                WHEN UPPER(p_tipo_venda) = 'SHOWROOM' THEN COALESCE(meta_showroom, 0)
                ELSE COALESCE(meta_valor, 0) 
            END AS valor_meta
        FROM metas_vendedoras
        WHERE chave_mes = p_chave_mes 
          AND chave_ano = p_chave_ano
          AND vendedor <> 'ALEX SANDRO'
          AND vendedor IS NOT NULL AND TRIM(vendedor) <> ''
    ),
    metas_com_valor AS (
        -- Filtra vendedoras com meta > 0 (Remove André no Showroom)
        SELECT 
            *,
            -- Calcula a soma de todas as metas válidas do período para usar no Total Geral
            SUM(valor_meta) OVER() AS meta_total_periodo 
        FROM metas_validas 
        WHERE valor_meta > 0
    ),
    faturamento_agrupado AS (
        SELECT 
            TRIM(UPPER(vendedor)) AS vendedor_join,
            SUM(valor_faturado) AS total_faturado,
            SUM(qtd_itens) AS total_itens,
            COUNT(DISTINCT nome_parceiro) AS total_conversao
        FROM staging_faturamento_multimarcas
        WHERE chave_mes = p_chave_mes 
          AND chave_ano = p_chave_ano
          AND vendedor <> 'ALEX SANDRO'
          AND (
               UPPER(p_tipo_venda) IN ('TODOS', 'TODAS') 
               OR (UPPER(p_tipo_venda) = 'SHOWROOM' AND tipo_venda = 'SHOWROOM')
               OR (UPPER(p_tipo_venda) = 'PRONTA ENTREGA' AND tipo_venda = 'PRONTA ENTREGA')
          )
        GROUP BY TRIM(UPPER(vendedor))
    )
    SELECT
        p_chave_mes AS chave_mes,
        p_chave_ano AS chave_ano,
        -- Identifica a linha do ROLLUP e define como "Total Geral"
        IF(GROUPING(m.vendedor_join) = 1, 'Total Geral', MAX(m.vendedor_original)) AS vendedor,

        -- Exibe a meta individual ou a soma total (deve bater os 1.613.528,00)
        CONCAT('R$ ', FORMAT(
            IF(GROUPING(m.vendedor_join) = 1, MAX(m.meta_total_periodo), MAX(m.valor_meta))
        , 2, 'de_DE')) AS meta_vendedor,

        CONCAT('R$ ', FORMAT(SUM(COALESCE(fa.total_faturado, 0)), 2, 'de_DE')) AS Faturado,
        
        CONCAT('R$ ', FORMAT(
            GREATEST(IF(GROUPING(m.vendedor_join) = 1, MAX(m.meta_total_periodo), MAX(m.valor_meta)) - SUM(COALESCE(fa.total_faturado, 0)), 0)
        , 2, 'de_DE')) AS `GAP (R$)`,

        SUM(COALESCE(fa.total_itens, 0)) AS Itens,
        SUM(COALESCE(fa.total_conversao, 0)) AS Conversao,
        
        CONCAT(IFNULL(FORMAT(
            (SUM(COALESCE(fa.total_faturado, 0)) / NULLIF(
                IF(GROUPING(m.vendedor_join) = 1, MAX(m.meta_total_periodo), MAX(m.valor_meta))
            , 0)) * 100
        , 2), '0,00'), '%') AS Atingimento,

        IF(GROUPING(m.vendedor_join) = 1,
            IF(SUM(COALESCE(fa.total_faturado, 0)) >= MAX(m.meta_total_periodo), 'BATEU META', 'FORA DA META'),
            IF(SUM(COALESCE(fa.total_faturado, 0)) >= MAX(m.valor_meta), 'BATEU META', 'FORA DA META')
        ) AS StatusMeta

    FROM metas_com_valor m
    LEFT JOIN faturamento_agrupado fa ON m.vendedor_join = fa.vendedor_join
    
    GROUP BY m.vendedor_join WITH ROLLUP
    ORDER BY GROUPING(m.vendedor_join) ASC, SUM(COALESCE(fa.total_faturado, 0)) DESC;

END;