CREATE DEFINER=`root`@`localhost` PROCEDURE `Faturamento`(
    IN p_chave_mes VARCHAR(20),
    IN p_chave_ano INT,
    IN p_tipo_venda VARCHAR(30)
)
BEGIN
    DECLARE v_meta_total_periodo DECIMAL(10, 2);
    DECLARE v_num_vendedores INT;
    DECLARE v_meta_individual DECIMAL(10, 2);
    DECLARE v_data_inicio_mes DATE;
    DECLARE v_mes_num VARCHAR(2);

    -- 1. TRATAMENTO DE DATA
    SET v_mes_num = CASE UPPER(p_chave_mes)
        WHEN 'JANEIRO' THEN '01' WHEN 'FEVEREIRO' THEN '02' WHEN 'MARÇO' THEN '03'
        WHEN 'ABRIL' THEN '04'   WHEN 'MAIO' THEN '05'      WHEN 'JUNHO' THEN '06'
        WHEN 'JULHO' THEN '07'   WHEN 'AGOSTO' THEN '08'    WHEN 'SETEMBRO' THEN '09'
        WHEN 'OUTUBRO' THEN '10' WHEN 'NOVEMBRO' THEN '11'  WHEN 'DEZEMBRO' THEN '12'
    END;
    SET v_data_inicio_mes = CAST(CONCAT(p_chave_ano, '-', v_mes_num, '-01') AS DATE);

    -- 2. BUSCA A META TOTAL (CORREÇÃO DA CONDIÇÃO 'TODAS')
    SELECT 
        CASE 
            WHEN UPPER(p_tipo_venda) IN ('TODOS', 'TODAS') THEN meta_valor -- Garante o uso de meta_valor conforme solicitado
            WHEN UPPER(p_tipo_venda) = 'PRONTA ENTREGA' THEN meta_prontaentrega
            WHEN UPPER(p_tipo_venda) = 'SHOWROOM' THEN meta_showroom
            ELSE meta_valor 
        END INTO v_meta_total_periodo
    FROM metas_vendedoras
    WHERE chave_mes = p_chave_mes AND chave_ano = p_chave_ano
    LIMIT 1;

    -- 3. CONTA VENDEDORES ATIVOS NO MÊS
    SELECT COUNT(*) INTO v_num_vendedores
    FROM cadastro_vendedores
    WHERE ativo = 1
      AND data_entrada <= LAST_DAY(v_data_inicio_mes)
      AND (data_saida IS NULL OR data_saida >= v_data_inicio_mes);

    SET v_meta_individual = IF(v_num_vendedores > 0, v_meta_total_periodo / v_num_vendedores, 0);

    -- 4. QUERY PRINCIPAL
    WITH faturamento_agrupado AS (
        SELECT 
            vendedor,
            SUM(valor_faturado) AS total_faturado,
            SUM(qtd_itens) AS total_itens,
            COUNT(DISTINCT nome_parceiro) AS total_conversao
        FROM staging_faturamento_multimarcas
        WHERE chave_mes = p_chave_mes 
          AND chave_ano = p_chave_ano
          AND (
               UPPER(p_tipo_venda) IN ('TODOS', 'TODAS') 
               OR (UPPER(p_tipo_venda) = 'SHOWROOM' AND tipo_venda = 'SHOWROOM')
               OR (UPPER(p_tipo_venda) = 'PRONTA ENTREGA' AND tipo_venda = 'PRONTA ENTREGA')
          )
        GROUP BY vendedor
    )
    SELECT
        p_chave_mes AS chave_mes,
        p_chave_ano AS chave_ano,
        COALESCE(cv.vendedor, 'Total Geral') AS vendedor,

        CONCAT('R$ ', FORMAT(
            CASE 
                WHEN GROUPING(cv.vendedor) = 1 THEN v_meta_total_periodo
                WHEN MAX(cv.ativo) = 1 THEN v_meta_individual
                ELSE 0 
            END
        , 2, 'de_DE')) AS meta_vendedor,

        CONCAT('R$ ', FORMAT(SUM(COALESCE(fa.total_faturado, 0)), 2, 'de_DE')) AS Faturado,
        
        CONCAT('R$ ', FORMAT(
            CASE 
                WHEN GROUPING(cv.vendedor) = 1 THEN 
                    GREATEST(v_meta_total_periodo - SUM(COALESCE(fa.total_faturado, 0)), 0)
                WHEN MAX(cv.ativo) = 1 THEN 
                    GREATEST(v_meta_individual - SUM(COALESCE(fa.total_faturado, 0)), 0)
                ELSE 0 
            END
        , 2, 'de_DE')) AS `GAP (R$)`,

        SUM(COALESCE(fa.total_itens, 0)) AS Itens,
        SUM(COALESCE(fa.total_conversao, 0)) AS Conversao,
        
        CONCAT(IFNULL(FORMAT(
            (SUM(COALESCE(fa.total_faturado, 0)) / NULLIF(
                CASE 
                    WHEN GROUPING(cv.vendedor) = 1 THEN v_meta_total_periodo
                    WHEN MAX(cv.ativo) = 1 THEN v_meta_individual
                    ELSE NULL 
                END
            , 0)) * 100
        , 2), '0,00'), '%') AS Atingimento,

        CASE
            WHEN GROUPING(cv.vendedor) = 1 THEN 
                IF((SUM(COALESCE(fa.total_faturado, 0)) / v_meta_total_periodo) >= 1, 'BATEU META', 'FORA DA META')
            WHEN MAX(cv.ativo) = 0 OR MAX(cv.ativo) IS NULL THEN 'SEM META'
            WHEN (SUM(COALESCE(fa.total_faturado, 0)) / NULLIF(v_meta_individual, 0)) >= 1 THEN 'BATEU META'
            ELSE 'FORA DA META'
        END AS StatusMeta

    FROM cadastro_vendedores cv
    LEFT JOIN faturamento_agrupado fa ON cv.vendedor = fa.vendedor
    
    WHERE (
        (cv.data_entrada <= LAST_DAY(v_data_inicio_mes) AND (cv.data_saida IS NULL OR cv.data_saida >= v_data_inicio_mes))
        OR (fa.total_faturado > 0)
    )
    GROUP BY cv.vendedor WITH ROLLUP
    ORDER BY GROUPING(cv.vendedor) ASC, SUM(COALESCE(fa.total_faturado, 0)) DESC;

END;