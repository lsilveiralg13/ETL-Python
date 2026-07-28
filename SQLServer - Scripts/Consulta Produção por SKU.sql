-- =====================================================================================
-- DIGITE O SKU QUE VOCÊ QUER INVESTIGAR AQUI:
-- =====================================================================================
DECLARE @SKU_PESQUISA VARCHAR(50) = '84047'; 


WITH BASE_PRODUCAO_UNIFICADA AS (
    -- Consolida os bipes e apontamentos puros do chão de fábrica antes do filtro de SKU
    SELECT 
        act.OrdemProducao, 
        app.CodProdutoAcabado AS CodProdOriginal, 
        SUM(CAST(app.QuantidadeApontada AS INT)) AS Quantidade_Produzida
    FROM producao.fato_apontamento ap WITH (NOLOCK)
    INNER JOIN producao.fato_atividade_op act WITH (NOLOCK) ON ap.CodItemAtividade = act.CodItemAtividade
    INNER JOIN producao.fato_apontamento_produto app WITH (NOLOCK) ON ap.CodApontamentoUnico = app.CodApontamentoUnico
    WHERE ap.DataHoraApontamento >= '2025-01-01'
      AND app.CodProdutoAcabado = @SKU_PESQUISA
    GROUP BY act.OrdemProducao, app.CodProdutoAcabado

    UNION ALL

    SELECT 
        ap.OrdemProducao, 
        pa.CodProdutoAcabado AS CodProdOriginal, 
        COUNT(CASE WHEN ap.DataHoraEmbalagem IS NOT NULL THEN ap.SerieProdutoAcabado END) AS Quantidade_Produzida
    FROM producao.fato_ordem_producao_seriepa_ciclo ap WITH (NOLOCK)
    INNER JOIN producao.fato_ordem_producao_item pa WITH (NOLOCK) ON ap.OrdemProducao = pa.OrdemProducao
    WHERE ap.DataHoraEmbalagem >= '2025-01-01'
      AND pa.CodProdutoAcabado = @SKU_PESQUISA
    GROUP BY ap.OrdemProducao, pa.CodProdutoAcabado
),

PLANEJAMENTO_AUX AS (
    -- Busca a meta planejada da engenharia para o lote por SKU
    SELECT 
        OrdemProducao,
        CodProdutoAcabado,
        MAX(CAST(QuantidadeAProduzir AS INT)) AS Qtd_Planejada_Item
    FROM producao.fato_ordem_producao_item WITH (NOLOCK)
    WHERE CodProdutoAcabado = @SKU_PESQUISA
    GROUP BY OrdemProducao, CodProdutoAcabado
),

RESULTADO_CONSOLIDADO AS (
    -- Agrupa as OPs eliminando a duplicidade causada por possíveis SKUs nulos na origem
    SELECT 
        B.OrdemProducao AS OP,
        MAX(B.CodProdOriginal) AS CodProduto_No_ChaoDeFabrica, -- Evita abrir uma linha pro NULL
        MAX(P.DescricaoProduto) AS Descricao_No_Cadastro_Geral,
        MAX(P.Marca) AS Marca,
        MAX(GP.LinhaDeNegocio) AS LinhaDeNegocio,
        MAX(GP.NomeGrupoPai) AS NomeGrupoPai,
        SUM(B.Quantidade_Produzida) AS Qtd_Produzida,
        MAX(COALESCE(PL.Qtd_Planejada_Item, 0)) AS Qtd_Planejada
    FROM BASE_PRODUCAO_UNIFICADA B
    LEFT JOIN cadastros.dim_produtos P WITH (NOLOCK) ON B.CodProdOriginal = P.CodProduto
    LEFT JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) ON GP.CodGrupoProduto = P.CodGrupoProduto
    LEFT JOIN PLANEJAMENTO_AUX PL ON B.OrdemProducao = PL.OrdemProducao AND B.CodProdOriginal = PL.CodProdutoAcabado
    GROUP BY B.OrdemProducao
)

-- =====================================================================================
-- CORPO PRINCIPAL COM AS LINHAS DE CADA OP
-- =====================================================================================
SELECT 
    0 AS Tipo_Linha, -- Usado para ordenar o total para o final
    CAST(OP AS VARCHAR(20)) AS OP,
    CodProduto_No_ChaoDeFabrica,
    Descricao_No_Cadastro_Geral,
    Marca,
    LinhaDeNegocio,
    NomeGrupoPai,
    Qtd_Produzida,
    Qtd_Planejada,
    (Qtd_Planejada - Qtd_Produzida) AS Saldo
FROM RESULTADO_CONSOLIDADO

UNION ALL

-- =====================================================================================
-- LINHA ÚNICA DE SOMA FINAL
-- =====================================================================================
SELECT 
    1 AS Tipo_Linha,
    '--- TOTAL GERAL ---' AS OP,
    '' AS CodProduto_No_ChaoDeFabrica,
    '' AS Descricao_No_Cadastro_Geral,
    '' AS Marca,
    '' AS LinhaDeNegocio,
    '' AS NomeGrupoPai,
    SUM(Qtd_Produzida) AS Qtd_Produzida,
    SUM(Qtd_Planejada) AS Qtd_Planejada,
    (SUM(Qtd_Planejada) - SUM(Qtd_Produzida)) AS Saldo
FROM RESULTADO_CONSOLIDADO

ORDER BY Tipo_Linha ASC, OP DESC;