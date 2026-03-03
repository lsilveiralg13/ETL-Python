CREATE DEFINER=`root`@`localhost` PROCEDURE `Pronta Entrega x Showrooom`(
    IN p_chave_mes VARCHAR(255),
    IN p_chave_ano INT
)
BEGIN
    -- Declaração de variáveis locais para evitar ambiguidades com parâmetros
    DECLARE v_mes_param VARCHAR(255);
    DECLARE v_ano_param INT;

    SET v_mes_param = p_chave_mes;
    SET v_ano_param = p_chave_ano;

    -- Consulta 1: Faturamento e Total Geral
    SELECT
        'Faturamento' AS Tipo,
        sfm.chave_ano,
        sfm.chave_mes AS `Mês`,
        CONCAT('R$ ', COALESCE(FORMAT(SUM(CASE WHEN sfm.tipo_venda = 'PRONTA ENTREGA' THEN sfm.valor_faturado ELSE 0 END), 2, 'de_DE'), '0,00')) AS `Pronta Entrega`,
        CONCAT('R$ ', COALESCE(FORMAT(SUM(CASE WHEN sfm.tipo_venda = 'SHOWROOM' THEN sfm.valor_faturado ELSE 0 END), 2, 'de_DE'), '0,00')) AS `Showroom`,
        CONCAT('R$ ', COALESCE(FORMAT(SUM(CASE WHEN sfm.tipo_venda IN ('PRONTA ENTREGA', 'SHOWROOM') THEN sfm.valor_faturado ELSE 0 END), 2, 'de_DE'), '0,00')) AS `Total Geral`
    FROM
        staging_faturamento_multimarcas sfm
    WHERE
        sfm.chave_mes = v_mes_param
        AND sfm.chave_ano = v_ano_param
        AND sfm.tipo_venda IN ('PRONTA ENTREGA', 'SHOWROOM')
    GROUP BY
        sfm.chave_ano,
        sfm.chave_mes
    
    UNION ALL

    -- Consulta 2: Meta
    SELECT
        'Meta' AS Tipo,
        m.chave_ano,
        m.chave_mes AS `Mês`,
        CONCAT('R$ ', COALESCE(FORMAT(m.meta_prontaentrega, 2, 'de_DE'), '0,00')) AS `Pronta Entrega`,
        CONCAT('R$ ', COALESCE(FORMAT(m.meta_showroom, 2, 'de_DE'), '0,00')) AS `Showroom`,
        CONCAT('R$ ', COALESCE(FORMAT(m.meta_prontaentrega + m.meta_showroom, 2, 'de_DE'), '0,00')) AS `Total Geral`
    FROM
        metas_vendedoras m
    WHERE
        m.chave_mes = v_mes_param
        AND m.chave_ano = v_ano_param
        AND m.meta_prontaentrega IS NOT NULL
        AND m.meta_showroom IS NOT NULL

    UNION ALL

    -- Consulta 3: O quanto falta para atingir a meta (diferença entre Meta e Faturamento)
    SELECT
        'Faltam (R$)' AS Tipo,
        sfm.chave_ano,
        sfm.chave_mes AS `Mês`,
        CONCAT('R$ ', COALESCE(FORMAT(
            CASE 
                WHEN (m.meta_prontaentrega - SUM(CASE WHEN sfm.tipo_venda = 'PRONTA ENTREGA' THEN sfm.valor_faturado ELSE 0 END)) < 0 THEN 0
                ELSE (m.meta_prontaentrega - SUM(CASE WHEN sfm.tipo_venda = 'PRONTA ENTREGA' THEN sfm.valor_faturado ELSE 0 END))
            END
        , 2, 'de_DE'), '0,00')) AS `Pronta Entrega`,
        CONCAT('R$ ', COALESCE(FORMAT(
            CASE 
                WHEN (m.meta_showroom - SUM(CASE WHEN sfm.tipo_venda = 'SHOWROOM' THEN sfm.valor_faturado ELSE 0 END)) < 0 THEN 0
                ELSE (m.meta_showroom - SUM(CASE WHEN sfm.tipo_venda = 'SHOWROOM' THEN sfm.valor_faturado ELSE 0 END))
            END
        , 2, 'de_DE'), '0,00')) AS `Showroom`,
        CONCAT('R$ ', COALESCE(FORMAT(
            CASE 
                WHEN ((m.meta_prontaentrega + m.meta_showroom) - SUM(CASE WHEN sfm.tipo_venda IN ('PRONTA ENTREGA', 'SHOWROOM') THEN sfm.valor_faturado ELSE 0 END)) < 0 THEN 0
                ELSE ((m.meta_prontaentrega + m.meta_showroom) - SUM(CASE WHEN sfm.tipo_venda IN ('PRONTA ENTREGA', 'SHOWROOM') THEN sfm.valor_faturado ELSE 0 END))
            END
        , 2, 'de_DE'), '0,00')) AS `Total Geral`
    FROM
        staging_faturamento_multimarcas sfm
    JOIN
        metas_vendedoras m ON sfm.chave_mes = m.chave_mes AND sfm.chave_ano = m.chave_ano
    WHERE
        sfm.chave_mes = v_mes_param
        AND sfm.chave_ano = v_ano_param
        AND sfm.tipo_venda IN ('PRONTA ENTREGA', 'SHOWROOM')
        AND m.meta_prontaentrega IS NOT NULL
        AND m.meta_showroom IS NOT NULL
    GROUP BY
        sfm.chave_ano,
        sfm.chave_mes,
        m.meta_prontaentrega,
        m.meta_showroom
        
    UNION ALL

    -- Consulta 4: Cálculo do Percentual
    SELECT
        'Atingimento' AS Tipo,
        sfm.chave_ano,
        sfm.chave_mes AS `Mês`,
        CONCAT(
            FORMAT(
                (SUM(CASE WHEN sfm.tipo_venda = 'PRONTA ENTREGA' THEN sfm.valor_faturado ELSE 0 END) / m.meta_prontaentrega) * 100, 
                2
            ),
            '%'
        ) AS `Pronta Entrega`,
        CONCAT(
            FORMAT(
                (SUM(CASE WHEN sfm.tipo_venda = 'SHOWROOM' THEN sfm.valor_faturado ELSE 0 END) / m.meta_showroom) * 100, 
                2
            ),
            '%'
        ) AS `Showroom`,
        CONCAT(
            FORMAT(
                (SUM(CASE WHEN sfm.tipo_venda IN ('PRONTA ENTREGA', 'SHOWROOM') THEN sfm.valor_faturado ELSE 0 END) / (m.meta_prontaentrega + m.meta_showroom)) * 100, 
                2
            ),
            '%'
        ) AS `Total Geral`
    FROM
        staging_faturamento_multimarcas sfm
    JOIN
        metas_vendedoras m ON sfm.chave_mes = m.chave_mes AND sfm.chave_ano = m.chave_ano
    WHERE
        sfm.chave_mes = v_mes_param
        AND sfm.chave_ano = v_ano_param
        AND sfm.tipo_venda IN ('PRONTA ENTREGA', 'SHOWROOM')
        AND m.meta_prontaentrega IS NOT NULL
        AND m.meta_showroom IS NOT NULL
    GROUP BY
        sfm.chave_ano,
        sfm.chave_mes,
        m.meta_prontaentrega,
        m.meta_showroom
    ORDER BY
        chave_ano ASC,
        CASE `Mês`
            WHEN 'JANEIRO' THEN 1
            WHEN 'FEVEREIRO' THEN 2
            WHEN 'MARÇO' THEN 3
            WHEN 'ABRIL' THEN 4
            WHEN 'MAIO' THEN 5
            WHEN 'JUNHO' THEN 6
            WHEN 'JULHO' THEN 7
            WHEN 'AGOSTO' THEN 8
            WHEN 'SETEMBRO' THEN 9
            WHEN 'OUTUBRO' THEN 10
            WHEN 'NOVEMBRO' THEN 11
            WHEN 'DEZEMBRO' THEN 12
            ELSE 0
        END ASC;

END;