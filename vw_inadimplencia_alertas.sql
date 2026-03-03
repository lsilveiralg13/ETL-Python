CREATE OR REPLACE VIEW vw_inadimplencia_alertas AS
    SELECT
        vendedor,
        Inadimplentes,
        Ativos,
        Valor,
        Inadimplencia,
        CASE 
            -- Convertendo a string 'XX.XX%' em decimal para comparação lógica
            WHEN CAST(REPLACE(Inadimplencia, '%', '') AS DECIMAL(10,2)) > 10 THEN 'CRÍTICO'
            WHEN CAST(REPLACE(Inadimplencia, '%', '') AS DECIMAL(10,2)) > 8 THEN 'ATENÇÃO'
            ELSE 'NORMAL'
        END AS Status_Risco
    FROM vw_Inadimplencia_Base;