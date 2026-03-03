CREATE DEFINER=`root`@`localhost` PROCEDURE `Showroom_Comparativo_V2`(
    IN evento_A VARCHAR(50),
    IN evento_B VARCHAR(50)
)
BEGIN
    SET @sql_query = CONCAT('
        WITH VendasBase AS (
            SELECT 
                CASE 
                    WHEN data_inclusao BETWEEN ''2024-01-22 00:00:00'' AND ''2024-01-26 23:59:59'' THEN ''INVERNO 2024''
                    WHEN data_inclusao BETWEEN ''2024-06-17 00:00:00'' AND ''2024-06-21 23:59:59'' THEN ''VERÃO 2025''
                    -- Range ampliado para capturar vendas de negociação após o dia 07/02
                    WHEN data_inclusao BETWEEN ''2025-02-04 00:00:00'' AND ''2025-02-28 23:59:59'' THEN ''INVERNO 2025''
                    WHEN data_inclusao BETWEEN ''2025-06-30 00:00:00'' AND ''2025-07-04 23:59:59'' THEN ''VERÃO 2026''
                    WHEN data_inclusao BETWEEN ''2026-01-26 00:00:00'' AND ''2026-02-15 23:59:59'' THEN ''INVERNO 2026''
                END AS Nome_Evento,
                DATE(data_inclusao) AS Data_Venda,
                SUM(IFNULL(valor_total_showroom, 0)) AS Total_Dia
            FROM staging_showroom_multimarcas 
            GROUP BY Nome_Evento, Data_Venda
        ),
        VendasOrdenadas AS (
            SELECT 
                Nome_Evento,
                Data_Venda,
                Total_Dia,
                ROW_NUMBER() OVER(PARTITION BY Nome_Evento ORDER BY Data_Venda) AS Ordem_Dia
            FROM VendasBase
            WHERE Nome_Evento IS NOT NULL
        ),
        Comparativo AS (
            SELECT 
                COALESCE(A.Ordem_Dia, B.Ordem_Dia) AS Ordem_Dia,
                A.Data_Venda AS Data_A,
                A.Total_Dia AS Valor_A,
                B.Data_Venda AS Data_B,
                B.Total_Dia AS Valor_B
            FROM VendasOrdenadas A
            LEFT JOIN VendasOrdenadas B ON A.Ordem_Dia = B.Ordem_Dia AND B.Nome_Evento = ''', evento_B, '''
            WHERE A.Nome_Evento = ''', evento_A, '''
            
            UNION
            
            SELECT 
                COALESCE(A.Ordem_Dia, B.Ordem_Dia) AS Ordem_Dia,
                A.Data_Venda AS Data_A,
                A.Total_Dia AS Valor_A,
                B.Data_Venda AS Data_B,
                B.Total_Dia AS Valor_B
            FROM VendasOrdenadas B
            LEFT JOIN VendasOrdenadas A ON A.Ordem_Dia = B.Ordem_Dia AND A.Nome_Evento = ''', evento_A, '''
            WHERE B.Nome_Evento = ''', evento_B, '''
        )
        SELECT 
            IF(Ordem_Dia IS NULL, ''Total Geral'', Ordem_Dia) AS ''Dia'',
            -- Uso de MAX() para satisfazer o only_full_group_by
            IF(Ordem_Dia IS NULL, ''-'', IFNULL(DATE_FORMAT(MAX(Data_A), ''%d/%m/%y''), ''S/ Data'')) AS ''Data_A'',
            CONCAT(''R$ '', FORMAT(SUM(IFNULL(Valor_A, 0)), 2, ''de_DE'')) AS ''', evento_A, ''',
            IF(Ordem_Dia IS NULL, ''-'', IFNULL(DATE_FORMAT(MAX(Data_B), ''%d/%m/%y''), ''S/ Data'')) AS ''Data_B'',
            CONCAT(''R$ '', FORMAT(SUM(IFNULL(Valor_B, 0)), 2, ''de_DE'')) AS ''', evento_B, ''',
            CONCAT(
                ROUND(
                    IFNULL(
                        ((SUM(IFNULL(Valor_B, 0)) - SUM(IFNULL(Valor_A, 0))) / NULLIF(SUM(IFNULL(Valor_A, 0)), 0)) * 100, 
                        0
                    ), 2
                ), ''%''
            ) AS ''Cresc_Dia''
        FROM Comparativo
        GROUP BY Ordem_Dia WITH ROLLUP;'
    );

    PREPARE stmt FROM @sql_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END;