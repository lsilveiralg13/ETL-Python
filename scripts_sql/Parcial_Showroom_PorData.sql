CREATE DEFINER=`root`@`localhost` PROCEDURE `Parcial_Showroom_PorData`(

    IN p_data_negociacao DATE,
    IN p_tipo_evento VARCHAR(255)

)
BEGIN
    -- 1. Tabela temporária de vendas consolidada
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_daily_sales (
        tipo_evento VARCHAR(255),
        vendedor VARCHAR(255),
        Valor_Total_Showroom DECIMAL(10, 2),
        Clientes_Convertidos INT,
        PRIMARY KEY (tipo_evento, vendedor)
    );

    TRUNCATE TABLE temp_daily_sales;
    INSERT INTO temp_daily_sales (tipo_evento, vendedor, Valor_Total_Showroom, Clientes_Convertidos)
    SELECT
        sf.tipo_evento,
        sf.vendedor,
        SUM(sf.valor_total_showroom),
        COUNT(DISTINCT sf.codigo_parceiro)
    FROM staging_showroom_multimarcas sf
    WHERE CAST(sf.data_negociacao AS DATE) = p_data_negociacao
      AND sf.tipo_evento = p_tipo_evento
    GROUP BY sf.tipo_evento, sf.vendedor;
    
    -- 2. SELECT FINAL com ROLLUP e tratamento de agregação para ORDER BY
    SELECT 
        IFNULL(tipo_evento, 'TOTAL') AS tipo_evento, 
        Data_Negociacao, 
        IFNULL(Vendedor, 'TOTAL GERAL') AS Vendedor, 
        CONCAT('R$ ', FORMAT(SUM(Vendido_Raw), 2, 'de_DE')) AS Vendido, 
        CONCAT('R$ ', FORMAT(SUM(Meta_Raw), 2, 'de_DE')) AS Meta_Diaria, 
        CONCAT(FORMAT(COALESCE((SUM(Vendido_Raw) / NULLIF(SUM(Meta_Raw), 0)) * 100, 0), 2, 'de_DE'), '%') AS Aproveitamento, 
        SUM(Clientes) AS Clientes, 
        CONCAT('R$ ', FORMAT(COALESCE((SUM(Vendido_Raw) / NULLIF(SUM(Clientes), 0)), 0), 2, 'de_DE')) AS TKM
    FROM (
        SELECT DISTINCT
            m.evento AS tipo_evento,
            p_data_negociacao AS Data_Negociacao,
            m.vendedor AS Vendedor,
            COALESCE(tds.Valor_Total_Showroom, 0) AS Vendido_Raw,
            COALESCE(m.meta_diaria, 0) AS Meta_Raw,
            COALESCE(tds.Clientes_Convertidos, 0) AS Clientes,
            m.meta_diaria AS meta_ordenacao -- Coluna para ordenação
        FROM
            metas_showroom_revisada m
        LEFT JOIN
            temp_daily_sales tds ON m.vendedor = tds.vendedor AND m.evento = tds.tipo_evento
        WHERE 
            m.data_evento = p_data_negociacao 
            AND m.evento = p_tipo_evento
    ) AS resultado_final
    GROUP BY tipo_evento, Data_Negociacao, Vendedor WITH ROLLUP
    HAVING (Vendedor IS NOT NULL OR tipo_evento = 'TOTAL')
    ORDER BY 
        (Vendedor IS NULL) ASC, -- Garante que o TOTAL GERAL (onde Vendedor é NULL pelo Rollup) fique por último
        MAX(meta_ordenacao) DESC, 
        Vendedor ASC;
        
    DROP TEMPORARY TABLE IF EXISTS temp_daily_sales;

END;