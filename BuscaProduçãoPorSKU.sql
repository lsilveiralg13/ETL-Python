SELECT
    I.CodProduto,
    P.DescricaoProduto,
    SUM(I.QtdNegociada) AS Qtd_Expedida
FROM belmicro.fato_operacoes AS O
INNER JOIN belmicro.fato_itens AS I
    ON O.NumUnicoNota = I.NumUnicoNota
INNER JOIN cadastros.dim_produtos AS P
    ON I.CodProduto = P.CodProduto
WHERE O.DataNegociacao >= '2025-01-01'
  AND O.DataNegociacao <= GETDATE()
  AND O.CodTipoOperacao IN (1102, 1119, 1607)
  AND I.CodProduto IN (80654)
GROUP BY
    I.CodProduto,
    P.DescricaoProduto
ORDER BY
    Qtd_Expedida DESC;