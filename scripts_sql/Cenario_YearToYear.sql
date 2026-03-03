CREATE DEFINER=`root`@`localhost` PROCEDURE `Cenario_YearToYear`()
BEGIN
    SET lc_time_names = 'pt_BR';

    -- 1. CTE para simular a tabela de Dias Úteis (D.U.) incluindo 2026
    WITH DiasUteis AS (
        SELECT 1 AS MesNum, 2023 AS AnoNum, 22 AS Dias_Uteis UNION ALL 
        SELECT 2, 2023, 19 UNION ALL SELECT 3, 2023, 23 UNION ALL SELECT 4, 2023, 19 UNION ALL SELECT 5, 2023, 22 UNION ALL SELECT 6, 2023, 21 UNION ALL 
        SELECT 7, 2023, 21 UNION ALL SELECT 8, 2023, 23 UNION ALL SELECT 9, 2023, 21 UNION ALL SELECT 10, 2023, 22 UNION ALL SELECT 11, 2023, 21 UNION ALL 
        SELECT 12, 2023, 20 UNION ALL
        
        SELECT 1, 2024, 22 UNION ALL SELECT 2, 2024, 20 UNION ALL SELECT 3, 2024, 21 UNION ALL SELECT 4, 2024, 22 UNION ALL SELECT 5, 2024, 22 UNION ALL SELECT 6, 2024, 20 UNION ALL 
        SELECT 7, 2024, 23 UNION ALL SELECT 8, 2024, 22 UNION ALL SELECT 9, 2024, 21 UNION ALL SELECT 10, 2024, 23 UNION ALL SELECT 11, 2024, 20 UNION ALL 
        SELECT 12, 2024, 22 UNION ALL

        SELECT 1, 2025, 22 UNION ALL SELECT 2, 2025, 20 UNION ALL SELECT 3, 2025, 20 UNION ALL SELECT 4, 2025, 21 UNION ALL SELECT 5, 2025, 21 UNION ALL SELECT 6, 2025, 21 UNION ALL 
        SELECT 7, 2025, 23 UNION ALL SELECT 8, 2025, 21 UNION ALL SELECT 9, 2025, 22 UNION ALL SELECT 10, 2025, 23 UNION ALL SELECT 11, 2025, 20 UNION ALL 
        SELECT 12, 2025, 22 UNION ALL
        
        -- Adicionado Dias Úteis 2026
        SELECT 1, 2026, 22 UNION ALL SELECT 2, 2026, 20 UNION ALL SELECT 3, 2026, 21 UNION ALL SELECT 4, 2026, 21 UNION ALL SELECT 5, 2026, 21 UNION ALL SELECT 6, 2026, 21 UNION ALL 
        SELECT 7, 2026, 23 UNION ALL SELECT 8, 2026, 21 UNION ALL SELECT 9, 2026, 21 UNION ALL SELECT 10, 2026, 22 UNION ALL SELECT 11, 2026, 20 UNION ALL 
        SELECT 12, 2026, 22
    )
    , DadosDetalhe AS (
        -- 2. Pivot de faturamento incluindo 2026
        SELECT
            T1.chave_mes AS Mes,
            CASE T1.chave_mes 
                WHEN 'Janeiro' THEN 1 WHEN 'Fevereiro' THEN 2 WHEN 'Março' THEN 3 WHEN 'Abril' THEN 4
                WHEN 'Maio' THEN 5 WHEN 'Junho' THEN 6 WHEN 'Julho' THEN 7 WHEN 'Agosto' THEN 8
                WHEN 'Setembro' THEN 9 WHEN 'Outubro' THEN 10 WHEN 'Novembro' THEN 11 WHEN 'Dezembro' THEN 12
            END AS Mes_Num,
            
            SUM(IF(T1.chave_ano = 2023, T1.valor_faturado, 0)) AS Faturamento_2023_NUM,
            SUM(IF(T1.chave_ano = 2024, T1.valor_faturado, 0)) AS Faturamento_2024_NUM,
            SUM(IF(T1.chave_ano = 2025, T1.valor_faturado, 0)) AS Faturamento_2025_NUM,
            SUM(IF(T1.chave_ano = 2026, T1.valor_faturado, 0)) AS Faturamento_2026_NUM
            
        FROM
            faturamento_multimarcas_dw.staging_faturamento_multimarcas T1
        GROUP BY
            T1.chave_mes
    )
    , DadosFinal AS (
        -- 3. Formatação e cálculos de crescimento
        SELECT
            DD.Mes,
            DD.Mes_Num,
            CONCAT('R$ ', FORMAT(DD.Faturamento_2023_NUM, 2, 'de_DE')) AS `2023`,
            CONCAT('R$ ', FORMAT(DD.Faturamento_2024_NUM, 2, 'de_DE')) AS `2024`,
            CONCAT('R$ ', FORMAT(DD.Faturamento_2025_NUM, 2, 'de_DE')) AS `2025`,
            CONCAT('R$ ', FORMAT(DD.Faturamento_2026_NUM, 2, 'de_DE')) AS `2026`,
            
            CASE
                WHEN DD.Faturamento_2023_NUM = 0 THEN 'N/A' 
                ELSE CONCAT(FORMAT((DD.Faturamento_2024_NUM / DD.Faturamento_2023_NUM - 1) * 100, 2,'de_DE'),'%')
            END AS `Cresc% 24/23`,
            CASE
                WHEN DD.Faturamento_2024_NUM = 0 THEN 'N/A' 
                ELSE CONCAT(FORMAT((DD.Faturamento_2025_NUM / DD.Faturamento_2024_NUM - 1) * 100, 2,'de_DE'),'%')
            END AS `Cresc% 25/24`,
            CASE
                WHEN DD.Faturamento_2025_NUM = 0 THEN 'N/A' 
                ELSE CONCAT(FORMAT((DD.Faturamento_2026_NUM / DD.Faturamento_2025_NUM - 1) * 100, 2,'de_DE'),'%')
            END AS `Cresc% 26/25`,
            
            CONCAT('R$ ', FORMAT(
                IFNULL(DD.Faturamento_2026_NUM / DU_2026.Dias_Uteis, 0),
                2,
                'de_DE'
            )) AS `ROB D.U 26`
            
        FROM
            DadosDetalhe DD
        LEFT JOIN
            DiasUteis DU_2026 ON DD.Mes_Num = DU_2026.MesNum AND DU_2026.AnoNum = 2026
    )
    , DadosTotal AS (
        -- 4. Linha de TOTAL GERAL
        SELECT
            'TOTAL GERAL/MÉDIA' AS Mes,
            13 AS Mes_Num,
            
            CONCAT('R$ ', FORMAT(SUM(Faturamento_2023_NUM), 2, 'de_DE')) AS `2023`,
            CONCAT('R$ ', FORMAT(SUM(Faturamento_2024_NUM), 2, 'de_DE')) AS `2024`,
            CONCAT('R$ ', FORMAT(SUM(Faturamento_2025_NUM), 2, 'de_DE')) AS `2025`,
            CONCAT('R$ ', FORMAT(SUM(Faturamento_2026_NUM), 2, 'de_DE')) AS `2026`,
            
            CASE
                WHEN SUM(Faturamento_2023_NUM) = 0 THEN 'N/A' 
                ELSE CONCAT(FORMAT((SUM(Faturamento_2024_NUM) / SUM(Faturamento_2023_NUM) - 1) * 100, 2,'de_DE'),'%')
            END AS `Cresc% 24/23`,
            CASE
                WHEN SUM(Faturamento_2024_NUM) = 0 THEN 'N/A' 
                ELSE CONCAT(FORMAT((SUM(Faturamento_2025_NUM) / SUM(Faturamento_2024_NUM) - 1) * 100, 2,'de_DE'),'%')
            END AS `Cresc% 25/24`,
            CASE
                WHEN SUM(Faturamento_2025_NUM) = 0 THEN 'N/A' 
                ELSE CONCAT(FORMAT((SUM(Faturamento_2026_NUM) / SUM(Faturamento_2025_NUM) - 1) * 100, 2,'de_DE'),'%')
            END AS `Cresc% 26/25`,
            
            CONCAT('R$ ', FORMAT(
                SUM(Faturamento_2026_NUM) / (SELECT SUM(Dias_Uteis) FROM DiasUteis WHERE AnoNum = 2026),
                2,
                'de_DE'
            )) AS `ROB D.U 26`
        FROM
            DadosDetalhe
    )
    -- 5. Resultado Final
    SELECT Mes, Mes_Num, `2023`, `2024`, `2025`, `2026`, `Cresc% 24/23`, `Cresc% 25/24`, `Cresc% 26/25`, `ROB D.U 26` FROM DadosFinal
    UNION ALL
    SELECT Mes, Mes_Num, `2023`, `2024`, `2025`, `2026`, `Cresc% 24/23`, `Cresc% 25/24`, `Cresc% 26/25`, `ROB D.U 26` FROM DadosTotal
    ORDER BY Mes_Num ASC;

END;