CREATE DEFINER=`root`@`localhost` PROCEDURE `Cenario_Faturamento`(
    IN p_chave_mes VARCHAR(20),
    IN p_chave_ano INT
)
BEGIN
    -- Declara variáveis para armazenar a meta individual do período e o número de vendedores
    DECLARE v_meta_individual_periodo DECIMAL(10, 2);
    DECLARE v_num_vendedores INT;

    -- Obtém a meta individual para o mês e ano especificados da tabela metas_vendedoras.
    SELECT meta_vendedor INTO v_meta_individual_periodo
    FROM metas_vendedoras
    WHERE chave_mes = p_chave_mes AND chave_ano = p_chave_ano
    LIMIT 1;

    -- Se não houver meta para o período, define como 0
    IF v_meta_individual_periodo IS NULL THEN
        SET v_meta_individual_periodo = 0;
    END IF;

    -- Obtém o número total de vendedores distintos para o período
    SELECT COUNT(DISTINCT vendedor) INTO v_num_vendedores
    FROM staging_faturamento_multimarcas
    WHERE chave_mes = p_chave_mes AND chave_ano = p_chave_ano;

    -- Se não houver vendedores, define como 1 para evitar divisão por zero
    IF v_num_vendedores IS NULL OR v_num_vendedores = 0 THEN
        SET v_num_vendedores = 1;
    END IF;

    SELECT
        -- Colunas de agrupamento: exibem 'Total Geral' na linha de total
        COALESCE(MIN(sub.chave_mes), 'Total Geral') AS chave_mes,
        COALESCE(CAST(MIN(sub.chave_ano) AS CHAR), 'Total Geral') AS chave_ano,
        COALESCE(sub.vendedor, 'Total Geral') AS vendedor,

        -- meta_vendedor: Para linhas de detalhe, meta individual. Para o Total Geral, soma das metas individuais.
        CONCAT('R$ ', FORMAT(
            CASE
                WHEN GROUPING(sub.vendedor) = 1 THEN v_meta_individual_periodo * v_num_vendedores -- Soma das metas individuais para o Total Geral
                ELSE v_meta_individual_periodo -- Meta individual por vendedor
            END
        , 2, 'de_DE')) AS meta_vendedor,

        CONCAT('R$ ', FORMAT(SUM(sub.Faturado), 2, 'de_DE')) AS Faturado,

        -- GAP (R$): Soma dos GAPs individuais (já tratados para serem >= 0)
        CONCAT('R$ ', FORMAT(SUM(sub.RawGapForSum), 2, 'de_DE')) AS `GAP (R$)`,

        SUM(sub.Itens) AS Itens,
        SUM(sub.Conversao) AS Conversao,

        -- VLM: (Soma Faturado) / (Contagem Parceiros Distintos) - Correto para detalhe e total
        CONCAT('R$ ', FORMAT(SUM(sub.Faturado) / NULLIF(SUM(sub.Conversao), 0), 2, 'de_DE')) AS VLM,
        
        -- Atingimento: (Soma Faturado) / (Meta apropriada) * 100
        CONCAT(FORMAT(
            (SUM(sub.Faturado) / NULLIF(
                CASE
                    WHEN GROUPING(sub.vendedor) = 1 THEN v_meta_individual_periodo * v_num_vendedores -- Soma das metas para o total geral
                    ELSE v_meta_individual_periodo -- Meta individual para detalhe
                END
            , 0)) * 100
        , 2), '%') AS Atingimento,

        -- StatusMeta: Lógica baseada no atingimento correto para detalhe e total
        CASE
            WHEN (SUM(sub.Faturado) / NULLIF(
                CASE
                    WHEN GROUPING(sub.vendedor) = 1 THEN v_meta_individual_periodo * v_num_vendedores
                    ELSE v_meta_individual_periodo
                END
            , 0)) * 100 > 100 THEN 'BATEU META'
            ELSE 'FORA DA META'
        END AS StatusMeta
    FROM
        ( -- Subconsulta para calcular o GAP individual antes do ROLLUP
            SELECT
                sf.chave_mes,
                sf.chave_ano,
                sf.vendedor,
                SUM(sf.valor_faturado) AS Faturado,
                SUM(sf.qtd_itens) AS Itens,
                COUNT(DISTINCT sf.nome_parceiro) AS Conversao,
                -- Calcula o GAP real para cada vendedor
                (v_meta_individual_periodo - SUM(sf.valor_faturado)) AS RawIndividualGap,
                -- Aplica a lógica de <=0 THEN 0 para a soma do total geral
                CASE
                    WHEN (v_meta_individual_periodo - SUM(sf.valor_faturado)) <= 0 THEN 0
                    ELSE (v_meta_individual_periodo - SUM(sf.valor_faturado))
                END AS RawGapForSum -- Este é o valor que será somado para o total GAP
            FROM
                staging_faturamento_multimarcas sf
            WHERE
                sf.chave_mes = p_chave_mes AND sf.chave_ano = p_chave_ano
            GROUP BY
                sf.chave_mes, sf.chave_ano, sf.vendedor
        ) AS sub
    GROUP BY
        sub.vendedor WITH ROLLUP -- ROLLUP apenas no vendedor para gerar um único total geral para o mês/ano
    ORDER BY
        GROUPING(sub.vendedor) ASC, -- Garante que a linha de total (onde vendedor é NULL) fique por último
        SUM(sub.Faturado) DESC; -- Ordena os vendedores individuais por faturamento
END