CREATE DEFINER=`root`@`localhost` PROCEDURE `Analise_Custo_Operacional_Showroom`(
    IN p_start_date DATE,
    IN p_end_date DATE,
    IN p_codigo_parceiro BIGINT -- Novo parâmetro para filtrar por Código do Parceiro
)
BEGIN
    SELECT
        scs.`CODIGO PAR.` AS Codigo_Parceiro, -- Agrupamento por código do parceiro
        scs.`NOME DO PARCEIRO` AS Nome_Parceiro, -- Agrupamento por nome do parceiro
        scs.MODALIDADE AS Modalidade,
        
        -- Soma do custo total
        CONCAT('R$ ', FORMAT(COALESCE(SUM(scs.`CUSTO - AÉREO` + scs.`CUSTO - HOTEL` + scs.`CUSTO - RODOVIÁRIO`), 0), 2, 'de_DE')) AS Custo_Total,
        
        -- Soma do valor vendido no evento
        CONCAT('R$ ', FORMAT(COALESCE(SUM(scs.`VENDIDO VERÃO 2026`), 0), 2, 'de_DE')) AS Valor_Vendido_Evento,
        
        -- Percentual do custo sobre a venda
        CONCAT(
            FORMAT(
                COALESCE(
                    (SUM(scs.`CUSTO - AÉREO` + scs.`CUSTO - HOTEL` + scs.`CUSTO - RODOVIÁRIO`) /
                     NULLIF(SUM(scs.`VENDIDO VERÃO 2026`), 0)) * 100,
                    0
                ), 2
            ), '%'
        ) AS Pct_Custo_Sobre_Venda
    FROM
        staging_cadastro_showroom scs
    WHERE
            scs.DATA BETWEEN p_start_date AND p_end_date
        AND
            (p_codigo_parceiro IS NULL OR scs.`CODIGO PAR.` = p_codigo_parceiro) -- Filtro opcional por Código do Parceiro
    GROUP BY
        Codigo_Parceiro,
        Nome_Parceiro,
        Modalidade
    ORDER BY
        Codigo_Parceiro ASC,
        Nome_Parceiro ASC,
        Modalidade ASC;

END;