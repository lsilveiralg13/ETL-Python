CREATE DEFINER=`root`@`localhost` PROCEDURE `Showroom_Comparativo`(
    IN evento_A VARCHAR(50),
    IN evento_B VARCHAR(50)
)
BEGIN
    -- Montagem da Query Dinâmica ajustada para o modo only_full_group_by
    SET @sql_query = CONCAT('
        WITH VendasBase AS (
            SELECT 
                CASE 
                    WHEN data_inclusao BETWEEN ''2024-01-22 00:00:00'' AND ''2024-01-26 23:59:59'' THEN ''INVERNO 2024''
                    WHEN data_inclusao BETWEEN ''2024-06-17 00:00:00'' AND ''2024-06-21 23:59:59'' THEN ''VERÃO 2025''
                    WHEN data_inclusao BETWEEN ''2025-02-04 00:00:00'' AND ''2025-02-07 23:59:59'' THEN ''INVERNO 2025''
                    WHEN data_inclusao BETWEEN ''2025-06-30 00:00:00'' AND ''2025-07-04 23:59:59'' THEN ''VERÃO 2026''
                    WHEN data_inclusao BETWEEN ''2026-01-26 00:00:00'' AND ''2026-01-30 23:59:59'' THEN ''INVERNO 2026''
                END AS Nome_Evento,
                DATE(data_inclusao) AS Data_Venda,
                SUM(valor_total_showroom) AS Total_Dia
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
        ),
        Comparativo AS (
            SELECT 
                A.Ordem_Dia,
                A.Data_Venda AS Data_A,
                A.Total_Dia AS Valor_A,
                B.Data_Venda AS Data_B,
                B.Total_Dia AS Valor_B
            FROM VendasOrdenadas A
            INNER JOIN VendasOrdenadas B ON A.Ordem_Dia = B.Ordem_Dia
            WHERE A.Nome_Evento = ''', evento_A, ''' 
              AND B.Nome_Evento = ''', evento_B, '''
        )
        -- Seleção Final: Uso de MAX() nas datas para evitar o erro 1055
        SELECT 
            IF(Ordem_Dia IS NULL, '''', Ordem_Dia) AS ''Dia'',
            IF(Ordem_Dia IS NULL, ''TOTAL'', DATE_FORMAT(MAX(Data_A), ''%d/%m/%y'')) AS ''Data_A'',
            CONCAT(''R$ '', FORMAT(SUM(Valor_A), 2, ''de_DE'')) AS ''', evento_A, ''',
            IF(Ordem_Dia IS NULL, ''TOTAL'', DATE_FORMAT(MAX(Data_B), ''%d/%m/%y'')) AS ''Data_B'',
            CONCAT(''R$ '', FORMAT(SUM(Valor_B), 2, ''de_DE'')) AS ''', evento_B, ''',
            CONCAT(ROUND(((SUM(Valor_B) - SUM(Valor_A)) / SUM(Valor_A)) * 100, 2), ''%'') AS ''Cresc_Dia''
        FROM Comparativo
        GROUP BY Ordem_Dia WITH ROLLUP;'
    );

    PREPARE stmt FROM @sql_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END;