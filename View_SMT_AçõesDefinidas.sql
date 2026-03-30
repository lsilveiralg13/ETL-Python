CREATE OR REPLACE VIEW vw_SMT_AçõesDefinidas AS
WITH Base_Geral AS (
    -- 1. Volume total por SKU/Modelo/Ano/Mês
    SELECT 
        sku AS SKU_ID,
        modelo AS MODELO_DESC,
        chave_ano AS ANO,
        chave_mes AS MES,
        COUNT(*) AS VOLUME_SKU,
        -- Total absoluto do período para o cálculo do % Global
        SUM(COUNT(*)) OVER(PARTITION BY chave_ano, chave_mes) AS TOTAL_GERAL_PERIODO
    FROM staging_reparos
    GROUP BY sku, modelo, chave_ano, chave_mes
),
Ranking_Diagnostico AS (
    -- 2. Identifica o diagnóstico principal de cada SKU no período
    SELECT 
        sku, modelo, chave_ano, chave_mes,
        UPPER(TRIM(diagnostico_tecnico)) AS DIAGNOSTICO,
        COUNT(*) AS QTD_DIAG,
        ROW_NUMBER() OVER(PARTITION BY sku, modelo, chave_ano, chave_mes ORDER BY COUNT(*) DESC) AS rnk
    FROM staging_reparos
    GROUP BY sku, modelo, chave_ano, chave_mes, diagnostico_tecnico
),
Ranking_Acao AS (
    -- 3. Identifica a ação mais realizada de cada SKU no período
    SELECT 
        sku, modelo, chave_ano, chave_mes,
        CASE 
            WHEN UPPER(TRIM(acao_realizada)) = 'SEM AÇÃO' THEN 'SEM AÇÃO - NO FAULT FOUND'
            ELSE UPPER(TRIM(acao_realizada)) 
        END AS ACAO_PRINCIPAL,
        COUNT(*) AS QTD_ACAO,
        ROW_NUMBER() OVER(PARTITION BY sku, modelo, chave_ano, chave_mes ORDER BY COUNT(*) DESC) AS rnk
    FROM staging_reparos
    GROUP BY sku, modelo, chave_ano, chave_mes, acao_realizada
)
SELECT 
    B.ANO,
    B.MES,
    B.SKU_ID AS SKU,
    B.MODELO_DESC AS MODELO,
    B.VOLUME_SKU AS 'QTD SKU',
    ROUND((B.VOLUME_SKU / NULLIF(B.TOTAL_GERAL_PERIODO, 0)) * 100, 2) AS '% SOB TOTAL',
    D.DIAGNOSTICO AS 'DIAGNÓSTICO (OFENSOR)',
    D.QTD_DIAG AS 'QTD DIAGNÓSTICO',
    ROUND((D.QTD_DIAG / NULLIF(B.VOLUME_SKU, 0)) * 100, 2) AS '% SOB DIAG.',
    A.ACAO_PRINCIPAL AS 'AÇÃO MAIS REALIZADA',
    A.QTD_ACAO AS 'QTD AÇÃO',
    ROUND((A.QTD_ACAO / NULLIF(B.VOLUME_SKU, 0)) * 100, 2) AS '% SOB AÇÃO'

FROM Base_Geral B
JOIN Ranking_Diagnostico D ON B.SKU_ID = D.sku 
    AND B.MODELO_DESC = D.modelo 
    AND B.ANO = D.chave_ano 
    AND B.MES = D.chave_mes 
    AND D.rnk = 1
JOIN Ranking_Acao A ON B.SKU_ID = A.sku 
    AND B.MODELO_DESC = A.modelo 
    AND B.ANO = A.chave_ano 
    AND B.MES = A.chave_mes 
    AND A.rnk = 1

ORDER BY B.VOLUME_SKU DESC;