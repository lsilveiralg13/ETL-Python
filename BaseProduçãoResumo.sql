SELECT 
    I.CodProduto,
    P.DescricaoProduto,
    G.NomeGrupoPai,
    O.NumNota AS Numero_OP, -- Testando NumNota como OP conforme evidência anterior
    O.CodTipoOperacao,
    O.DataEntradaSaida AS Data_Producao, -- Mantido o timestamp original
    -- Campos para estudo de SLA com timestamp completo
    O.DataNegociacao, 
    O.DataMovimento,
    SUM(I.QtdNegociada) AS Qtd_Total_na_OP
FROM belmicro.fato_operacoes AS O
INNER JOIN belmicro.fato_itens AS I 
    ON O.NumUnicoNota = I.NumUnicoNota
INNER JOIN cadastros.dim_produtos AS P 
    ON I.CodProduto = P.CodProduto
INNER JOIN cadastros.dim_grupo_produtos AS G 
    ON P.CodGrupoProduto = G.CodGrupoProduto
WHERE O.DataEntradaSaida >= '2025-01-01' 
  AND O.DataEntradaSaida < '2027-01-01'
  AND O.CodTipoOperacao IN (1607, 1604)
  AND G.NomeGrupoPai IN ('DESKTOP', 'ALL IN ONE', 'MONITORES', 'TV')
  AND G.LinhadeNegocio = 'WordPC/Skill'
GROUP BY 
    I.CodProduto,
    P.DescricaoProduto,
    G.NomeGrupoPai,
    O.NumNota,
    O.CodTipoOperacao,
    O.DataNegociacao,
    O.DataMovimento,
    O.DataEntradaSaida -- Removido o CAST para manter o timestamp no agrupamento
ORDER BY Data_Producao DESC, Qtd_Total_na_OP DESC;