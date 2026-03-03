CREATE DEFINER=`root`@`localhost` PROCEDURE `Faturamento`(
    IN p_chave_mes VARCHAR(20),
    IN p_chave_ano INT,
    IN p_tipo_venda VARCHAR(30) -- NOVO PARÂMETRO: 'SHOWROOM', 'PRONTA ENTREGA' ou 'TODOS'
)
BEGIN
    DECLARE v_meta_individual_periodo DECIMAL(10, 2);
    DECLARE v_num_vendedores INT;

    SELECT meta_vendedor INTO v_meta_individual_periodo
    FROM metas_vendedoras
    WHERE chave_mes = p_chave_mes AND chave_ano = p_chave_ano
    LIMIT 1;

    IF v_meta_individual_periodo IS NULL THEN
        SET v_meta_individual_periodo = 0;
    END IF;

    -- Contagem de vendedores respeitando as exceções
    SELECT COUNT(DISTINCT vendedor) INTO v_num_vendedores
    FROM staging_faturamento_multimarcas
    WHERE chave_mes = p_chave_mes AND chave_ano = p_chave_ano
      AND vendedor NOT IN ('ALEX SANDRO', 'ANDRELEOCADIO', 'SILVANIAGOMES'); 

    IF v_num_vendedores IS NULL OR v_num_vendedores = 0 THEN
        SET v_num_vendedores = 1;
    END IF;

    SELECT
        COALESCE(MIN(sub.chave_mes), 'Total Geral') AS chave_mes,
        COALESCE(CAST(MIN(sub.chave_ano) AS CHAR), 'Total Geral') AS chave_ano,
        COALESCE(sub.vendedor, 'Total Geral') AS vendedor,

        CONCAT('R$ ', FORMAT(
            CASE
                WHEN GROUPING(sub.vendedor) = 1 THEN v_meta_individual_periodo * v_num_vendedores 
                WHEN sub.vendedor IN ('ALEX SANDRO', 'ANDRELEOCADIO', 'SILVANIAGOMES') THEN 0
                ELSE v_meta_individual_periodo 
            END
        , 2, 'de_DE')) AS meta_vendedor,

        CONCAT('R$ ', FORMAT(SUM(sub.Faturado), 2, 'de_DE')) AS Faturado,
        CONCAT('R$ ', FORMAT(SUM(sub.RawGapForSum), 2, 'de_DE')) AS `GAP (R$)`,
        SUM(sub.Itens) AS Itens,
        SUM(sub.Conversao) AS Conversao,
        CONCAT('R$ ', FORMAT(SUM(sub.Faturado) / NULLIF(SUM(sub.Conversao), 0), 2, 'de_DE')) AS VLM,
        
        CONCAT(IFNULL(FORMAT(
            (SUM(sub.Faturado) / NULLIF(
                CASE
                    WHEN GROUPING(sub.vendedor) = 1 THEN v_meta_individual_periodo * v_num_vendedores 
                    WHEN sub.vendedor IN ('ALEX SANDRO', 'ANDRELEOCADIO', 'SILVANIAGOMES') THEN NULL 
                    ELSE v_meta_individual_periodo 
                END
            , 0)) * 100
        , 2), '0,00'), '%') AS Atingimento,

        CASE
            WHEN sub.vendedor IN ('ALEX SANDRO', 'ANDRELEOCADIO', 'SILVANIAGOMES') THEN 'SEM META'
            WHEN (SUM(sub.Faturado) / NULLIF(
                CASE
                    WHEN GROUPING(sub.vendedor) = 1 THEN v_meta_individual_periodo * v_num_vendedores
                    ELSE v_meta_individual_periodo
                END
            , 0)) * 100 > 100 THEN 'BATEU META'
            ELSE 'FORA DA META'
        END AS StatusMeta
    FROM
        (
            SELECT
                sf.chave_mes,
                sf.chave_ano,
                sf.vendedor,
                SUM(sf.valor_faturado) AS Faturado,
                SUM(sf.qtd_itens) AS Itens,
                COUNT(DISTINCT sf.nome_parceiro) AS Conversao,
                CASE 
                    WHEN sf.vendedor IN ('ALEX SANDRO', 'ANDRELEOCADIO', 'SILVANIAGOMES') THEN 0
                    ELSE (v_meta_individual_periodo - SUM(sf.valor_faturado))
                END AS RawIndividualGap,
                CASE
                    WHEN sf.vendedor IN ('ALEX SANDRO', 'ANDRELEOCADIO', 'SILVANIAGOMES') THEN 0
                    WHEN (v_meta_individual_periodo - SUM(sf.valor_faturado)) <= 0 THEN 0
                    ELSE (v_meta_individual_periodo - SUM(sf.valor_faturado))
                END AS RawGapForSum 
            FROM
                staging_faturamento_multimarcas sf
            WHERE
                sf.chave_mes = p_chave_mes 
                AND sf.chave_ano = p_chave_ano
                -- LOGICA DE FILTRO DE TIPO DE VENDA
                AND (
                    p_tipo_venda = 'TODOS' 
                    OR (p_tipo_venda = 'SHOWROOM' AND sf.tipo_venda = 'SHOWROOM')
                    OR (p_tipo_venda = 'PRONTA ENTREGA' AND sf.tipo_venda = 'PRONTA ENTREGA')
                )
            GROUP BY
                sf.chave_mes, sf.chave_ano, sf.vendedor
        ) AS sub
    GROUP BY
        sub.vendedor WITH ROLLUP 
    ORDER BY
        GROUPING(sub.vendedor) ASC, 
        SUM(sub.Faturado) DESC;
END;