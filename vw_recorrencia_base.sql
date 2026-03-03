CREATE OR REPLACE VIEW vw_Recorrencia_Base AS
SELECT 
    vendedor, 
    Recorrentes,
	Total_Base,
    Recorrencia 
FROM (
    WITH cte_base_recorrencia AS (
        SELECT 
            vendedor,
            COUNT(CASE WHEN grupo_recorrencia = '0' THEN 1 END) AS c_0,
            COUNT(CASE WHEN grupo_recorrencia = '1' THEN 1 END) AS c_1,
            COUNT(CASE WHEN grupo_recorrencia = '2' THEN 1 END) AS c_2,
            COUNT(CASE WHEN grupo_recorrencia = '>=3' THEN 1 END) AS c_3_mais
        FROM staging_recorrencia_multimarcas
        WHERE grupo_status = 'BASE ATIVA'
          AND regua_cadastro = '>=90D'
          AND vendedor NOT IN ('ALEX SANDRO', 'MARCELAVAZ', '<SEM VENDEDOR>')
        GROUP BY vendedor
    )
    -- Parte 1: Performance por Vendedor
    SELECT 
        vendedor,
        (c_0 + c_1 + c_2 + c_3_mais) AS Total_Base,
        (c_1 + c_2 + c_3_mais) AS Recorrentes,
        CONCAT(ROUND(((c_1 + c_2 + c_3_mais) / NULLIF(c_0 + c_1 + c_2 + c_3_mais, 0)) * 100, 2), '%') AS Recorrencia,
        -- Criamos o valor numérico real para a ordenação
        ((c_1 + c_2 + c_3_mais) / NULLIF(c_0 + c_1 + c_2 + c_3_mais, 0)) AS perc_ordenacao,
        0 AS linha_total
    FROM cte_base_recorrencia

    UNION ALL

    -- Parte 2: Total Geral da Equipe
    SELECT 
        '--- TOTAL DA EQUIPE ---',
        SUM(c_0 + c_1 + c_2 + c_3_mais),
        SUM(c_1 + c_2 + c_3_mais),
        CONCAT(ROUND((SUM(c_1 + c_2 + c_3_mais) / NULLIF(SUM(c_0 + c_1 + c_2 + c_3_mais), 0)) * 100, 2), '%'),
        (SUM(c_1 + c_2 + c_3_mais) / NULLIF(SUM(c_0 + c_1 + c_2 + c_3_mais), 0)) AS perc_ordenacao,
        1 AS linha_total
    FROM cte_base_recorrencia
) AS resultado
-- Ordena primeiro para manter o Total no fim, depois pela porcentagem real DESC
ORDER BY linha_total ASC, perc_ordenacao DESC;