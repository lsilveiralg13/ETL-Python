-- ETAPA 1: Estruturar a base de dados
WITH CTE_BASE AS (
    SELECT
        CAST(s.VALUE AS DECIMAL(15,2)) AS valor_num,
        e.NAME AS nome_empresa,
        CASE
            WHEN s.DESCRIPTION LIKE '%ATIVO%' THEN 'ATIVO'
            WHEN s.DESCRIPTION LIKE '%PASSIVO%' THEN 'PASSIVO'
            ELSE 'OUTRO'
        END AS tipo
    FROM source s
    INNER JOIN empresa e ON e.CODE = s.CODE_EMPRESA
    WHERE s.ID_RELATORIO = '1'
      AND s.DATA >= '2023-01-01'
      AND s.DATA <  '2023-02-01'
),

-- ETAPA 2: Agregar valores por empresa + Função WITH ROLLUP para expor valores totais
CTE_EMPRESAS AS (
    SELECT
        IFNULL(nome_empresa, 'TOTAL DO GRUPO') AS Empresa, -- Nomeia a linha do ROLLUP
        SUM(CASE WHEN tipo = 'ATIVO' THEN valor_num ELSE 0 END) AS total_ativo,
        SUM(CASE WHEN tipo = 'PASSIVO' THEN valor_num ELSE 0 END) AS total_passivo
    FROM CTE_BASE
    GROUP BY nome_empresa WITH ROLLUP
)

-- RESULTADO FINAL: Consolidação dos dados, considerando movimentação de ATIVOS, PASSIVOS + CHECK de INTEGRIDADE
SELECT 
    Empresa,
    CONCAT('R$ ', FORMAT(total_ativo, 2, 'pt_BR')) AS `TOTAL ATIVO`,
    CONCAT('R$ ', FORMAT(total_passivo, 2, 'pt_BR')) AS `TOTAL PASSIVO`,
    ROUND(total_ativo + total_passivo, 2) AS `CHECK INTEGRIDADE`,
    CASE 
        WHEN ABS(total_ativo + total_passivo) < 0.01 THEN 'ÍNTEGRO'
        ELSE 'INCONSISTENTE'
    END AS `Status`
FROM CTE_EMPRESAS;