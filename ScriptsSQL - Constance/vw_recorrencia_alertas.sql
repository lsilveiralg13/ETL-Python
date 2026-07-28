CREATE OR REPLACE VIEW vw_Recorrencia_Alertas AS
SELECT
    vendedor,
    Recorrentes,
    Total_Base,
    Recorrencia,
    CASE 
        -- Abaixo de 40% é o nível de maior risco (Crítico)
        WHEN CAST(REPLACE(Recorrencia, '%', '') AS DECIMAL(10,2)) < 40.00 THEN 'CRÍTICO'
        -- Entre 40% e 47% é a zona de observação (Atenção)
        WHEN CAST(REPLACE(Recorrencia, '%', '') AS DECIMAL(10,2)) <= 47.00 THEN 'ATENÇÃO'
        -- Acima de 47% a performance está saudável (Normal)
        ELSE 'NORMAL'
    END AS Status_Fidelizacao
FROM vw_Recorrencia_Base;