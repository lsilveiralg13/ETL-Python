WITH 

-- ============================================================
-- 1. IDENTIFICAÇÃO APENAS DA ATIVIDADE DE FECHAMENTO DE CADA OP
-- ============================================================
ULTIMA_ATIVIDADE_OP AS (
    SELECT 
        OrdemProducao,
        MAX(CodItemAtividade) AS CodItemAtividade_Final
    FROM producao.fato_atividade_op WITH (NOLOCK)
    GROUP BY OrdemProducao
),

-- ============================================================
-- 2. QUANTIDADE PLANEJADA ACUMULADA POR SKU (Engenharia)
-- ============================================================
PLANEJAMENTO_SKU AS (
    SELECT 
        CodProdutoAcabado,
        SUM(CAST(QuantidadeAProduzir AS INT)) AS Qtd_Planejada_Total
    FROM producao.fato_ordem_producao_item WITH (NOLOCK)
    GROUP BY CodProdutoAcabado
),

-- ============================================================
-- 3. QUANTIDADE PRODUZIDA REAL ACUMULADA POR SKU
-- ============================================================
PRODUCAO_SKU AS (
    SELECT
        app.CodProdutoAcabado                           AS CodProduto,
        SUM(CAST(app.QuantidadeApontada AS INT))        AS Qtd_Produzida_Total
    FROM producao.fato_apontamento ap WITH (NOLOCK)
    INNER JOIN ULTIMA_ATIVIDADE_OP ult 
        ON ap.CodItemAtividade = ult.CodItemAtividade_Final
    INNER JOIN producao.fato_atividade_op act WITH (NOLOCK) 
        ON ap.CodItemAtividade = act.CodItemAtividade
    INNER JOIN producao.fato_apontamento_produto app WITH (NOLOCK) 
        ON ap.CodApontamentoUnico = app.CodApontamentoUnico
    WHERE ap.DataHoraApontamento IS NOT NULL
    GROUP BY app.CodProdutoAcabado
)

-- ============================================================
-- 4. EXIBIÇÃO CONSOLIDADA FINAL COM CVP% (CORRIGIDA)
-- ============================================================
SELECT
    P.CodProduto,
    P.DescricaoProduto                              AS Descricao,
    P.Marca                                         AS marca,
    COALESCE(PROD.Qtd_Produzida_Total, 0)           AS [Qtd Produzida],
    COALESCE(PLN.Qtd_Planejada_Total, 0)            AS [Qtd Planejada], -- Alterado de PLAN para PLN
    
    -- CVP% = (Qtd Produzida / Qtd Planejada)
    CAST(
        CASE 
            WHEN COALESCE(PLN.Qtd_Planejada_Total, 0) = 0 THEN 0
            ELSE (COALESCE(PROD.Qtd_Produzida_Total, 0) * 100.0) / PLN.Qtd_Planejada_Total
        END AS DECIMAL(5,2)
    ) AS [CVP%]

FROM cadastros.dim_produtos P WITH (NOLOCK)
LEFT JOIN PRODUCAO_SKU PROD ON P.CodProduto = PROD.CodProduto
LEFT JOIN PLANEJAMENTO_SKU PLN ON P.CodProduto = PLN.CodProdutoAcabado -- Alterado de PLAN para PLN

-- 🎯 COLOQUE O SKU DO PRODUTO ACABADO AQUI:
WHERE P.CodProduto = '96513';