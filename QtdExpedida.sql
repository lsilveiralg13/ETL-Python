DECLARE @CodProd VARCHAR(50) = '66523';

SELECT
    I.CodProduto AS CodProd,
    P.DescricaoProduto,
    GP.LinhaDeNegocio,
    SUM(I.QtdNegociada) AS QuantidadeExpedida

FROM belmicro.fato_itens I WITH (NOLOCK)

INNER JOIN belmicro.fato_itens_notas_expedidas EXP WITH (NOLOCK)
    ON EXP.NumUnicoNota = I.NumUnicoNota

INNER JOIN cadastros.dim_produtos P WITH (NOLOCK)
    ON P.CodProduto = I.CodProduto

INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK)
    ON GP.CodGrupoProduto = P.CodGrupoProduto

WHERE 
    EXP.DataExpedicao IS NOT NULL
    AND I.CodProduto = @CodProd
    AND EXP.DataExpedicao >= '2025-01-01'
    AND EXP.DataExpedicao < '2026-04-24'

GROUP BY 
    I.CodProduto,
    P.DescricaoProduto,
    GP.LinhaDeNegocio;