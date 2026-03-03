CREATE DEFINER=`root`@`localhost` PROCEDURE `Recorrencia_Resumo`()
BEGIN

WITH cte_calculos AS (
    SELECT 
        -- Lojas Recorrentes: soma de quem comprou 1, 2 ou 3+ vezes
        SUM(CASE WHEN grupo_recorrencia IN ('1', '2', '>=3') THEN qtd_total ELSE 0 END) AS lojas_recorrentes,
        -- Total de Lojas: soma de toda a base (incluindo quem não comprou '0')
        SUM(qtd_total) AS total_base_90d
    FROM staging_recorrencia_multimarcas
    WHERE grupo_status = 'BASE ATIVA'
      AND regua_cadastro = '>=90D'
      -- Removido status_vendedor pois a coluna não existe nesta tabela
      AND vendedor NOT IN ('ALEX SANDRO', 'MARCELAVAZ', '<SEM VENDEDOR>')
)
SELECT 
    'Lojas Recorrentes' AS Descricao, 
    CAST(lojas_recorrentes AS CHAR) AS Valor
FROM cte_calculos

UNION ALL

SELECT 
    'Total de Lojas >=90D Cadastro', 
    CAST(total_base_90d AS CHAR)
FROM cte_calculos

UNION ALL

SELECT 
    '% de Recorrência', 
    CONCAT(ROUND((lojas_recorrentes / NULLIF(total_base_90d, 0)) * 100, 2), '%')
FROM cte_calculos;

END;