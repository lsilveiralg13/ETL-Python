CREATE DEFINER=`root`@`localhost` PROCEDURE `Inadimplencia`()
BEGIN

WITH cte_ativos AS (
    SELECT 
        vendedor,
        COUNT(DISTINCT codigo_parceiro) AS ativos_num
    FROM staging_financeiro_multimarcas
    WHERE base_ativos = 'BASE ATIVA'
      AND regua_cadastro = '>=90D'
      AND status_vendedor = 'VENDEDOR ATIVO'
      AND vendedor NOT IN ('ALEX SANDRO', 'MARCELAVAZ', '<SEM VENDEDOR>')
    GROUP BY vendedor
),
cte_inadimplencia AS (
    SELECT 
        vendedor,
        SUM(valor_inadimplente) AS valor_num,
        COUNT(DISTINCT codigo_parceiro) AS qtd_num
    FROM staging_financeiro_multimarcas
    WHERE base_ativos = 'BASE ATIVA'
      AND status_vendedor = 'VENDEDOR ATIVO'
      AND vendedor NOT IN ('ALEX SANDRO', 'MARCELAVAZ', '<SEM VENDEDOR>')
      AND atraso_dias BETWEEN 1 AND 31
      AND inadimplentes = 'S'
    GROUP BY vendedor
)
SELECT 
    vendedor, Valor, Ativos, QTD, Inadimplencia 
FROM (
    SELECT 
        i.vendedor,
        CONCAT('R$ ', FORMAT(i.valor_num, 2, 'de_DE')) AS Valor,
        IFNULL(a.ativos_num, 0) AS Ativos,
        i.qtd_num AS QTD,
        CONCAT(ROUND((i.qtd_num / NULLIF(a.ativos_num, 0)) * 100, 2), '%') AS Inadimplencia,
        -- Coluna 6: Para ordenar pelo percentual real
        (i.qtd_num / NULLIF(a.ativos_num, 0)) AS perc_ordenacao,
        -- Coluna 7: Para garantir o Total no fim
        0 AS ordem 
    FROM cte_inadimplencia i
    INNER JOIN cte_ativos a ON i.vendedor = a.vendedor

    UNION ALL

    -- Linha do Total
    SELECT 
        '--- TOTAL DA EQUIPE ---',
        CONCAT('R$ ', FORMAT(SUM(total_em_aberto), 2, 'de_DE')),
        (SELECT COUNT(DISTINCT codigo_parceiro) 
         FROM staging_financeiro_multimarcas 
         WHERE base_ativos = 'BASE ATIVA' 
           AND regua_cadastro = '>=90D' 
           AND status_vendedor = 'VENDEDOR ATIVO'
           AND vendedor NOT IN ('ALEX SANDRO', 'MARCELAVAZ', '<SEM VENDEDOR>')),
        COUNT(DISTINCT codigo_parceiro), 
        CONCAT(ROUND((COUNT(DISTINCT codigo_parceiro) / NULLIF((SELECT COUNT(DISTINCT codigo_parceiro) FROM staging_financeiro_multimarcas WHERE base_ativos = 'BASE ATIVA' AND regua_cadastro = '>=90D' AND status_vendedor = 'VENDEDOR ATIVO' AND vendedor NOT IN ('ALEX SANDRO', 'MARCELAVAZ', '<SEM VENDEDOR>')), 0)) * 100, 2), '%'),
        -- Coluna 6: Percentual do total (precisa existir para o UNION bater)
        (COUNT(DISTINCT codigo_parceiro) / NULLIF((SELECT COUNT(DISTINCT codigo_parceiro) FROM staging_financeiro_multimarcas WHERE base_ativos = 'BASE ATIVA' AND regua_cadastro = '>=90D' AND status_vendedor = 'VENDEDOR ATIVO' AND vendedor NOT IN ('ALEX SANDRO', 'MARCELAVAZ', '<SEM VENDEDOR>')), 0)),
        -- Coluna 7: Ordem do Total
        1 AS ordem
    FROM staging_financeiro_multimarcas
    WHERE base_ativos = 'BASE ATIVA'
      AND status_vendedor = 'VENDEDOR ATIVO'
      AND vendedor NOT IN ('ALEX SANDRO', 'MARCELAVAZ', '<SEM VENDEDOR>')
      AND atraso_dias BETWEEN 1 AND 31
      AND inadimplentes = 'S'
) AS resultado_final
ORDER BY ordem ASC, perc_ordenacao DESC;

END;