CREATE OR REPLACE VIEW vw_KPI_Alerta_Churn AS
SELECT 
    vendedor,
    COUNT(*) AS `Recorrência 0`,
    CONCAT(ROUND((COUNT(*) / (SELECT Total_Base FROM vw_Recorrencia_Base WHERE vendedor = staging_recorrencia_multimarcas.vendedor)) * 100, 2), '%') AS Impacto
FROM staging_recorrencia_multimarcas
WHERE grupo_status = 'BASE ATIVA' 
  AND regua_cadastro = '>=90D' 
  AND grupo_recorrencia = '0'
  AND vendedor NOT IN ('ALEX SANDRO')
GROUP BY vendedor 
ORDER BY (COUNT(*) / (SELECT Total_Base FROM vw_Recorrencia_Base WHERE vendedor = staging_recorrencia_multimarcas.vendedor)) DESC;