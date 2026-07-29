/* BLOCO 1: Extração específica dos seus SKUs de Contagem */
SELECT 
    I.CodProduto,
    P.DescricaoProduto,
    G.NomeGrupoPai,
    O.NumNota AS Numero_OP,
    O.CodTipoOperacao,
    O.DataEntradaSaida AS Data_Producao, 
    O.DataNegociacao, 
    O.DataMovimento,
    O.DataAlteracao, -- Coluna com o Timestamp Real
    SUM(I.QtdNegociada) AS Qtd_Total_na_OP
FROM belmicro.fato_operacoes AS O
INNER JOIN belmicro.fato_itens AS I ON O.NumUnicoNota = I.NumUnicoNota
INNER JOIN cadastros.dim_produtos AS P ON I.CodProduto = P.CodProduto
INNER JOIN cadastros.dim_grupo_produtos AS G ON P.CodGrupoProduto = G.CodGrupoProduto
WHERE O.DataNegociacao >= '2024-01-01' 
  AND O.DataNegociacao <= GETDATE() 
  AND O.CodTipoOperacao IN (1102,1119,1607)
  AND I.CodProduto IN (
        81829, 81830, 81831, 83115, 83116, 83117, 83118, 
        84285, 91348, 91346, 91347, 94983, 94988, 94985, 
        94982, 94986, 94987, 94981,
        66522, 56549, 66523, 55836, 64018,
        66524, 56554, 64019, 71834, 66525,
        66526, 71828, 58481, 70119, 63945,
        70118, 71215, 68043, 68041, 68040,
        71830, 69674, 69669, 69277, 77217,
        77736
  )
GROUP BY I.CodProduto, P.DescricaoProduto, G.NomeGrupoPai, O.NumNota, O.CodTipoOperacao, O.DataNegociacao, O.DataMovimento, O.DataEntradaSaida, O.DataAlteracao

UNION ALL

/* BLOCO 2: Extração geral das categorias da linha WordPC/Skill */
SELECT 
    I.CodProduto,
    P.DescricaoProduto,
    G.NomeGrupoPai,
    O.NumNota AS Numero_OP,
    O.CodTipoOperacao,
    O.DataEntradaSaida AS Data_Producao, 
    O.DataNegociacao, 
    O.DataMovimento,
    O.DataAlteracao, -- Coluna com o Timestamp Real
    SUM(I.QtdNegociada) AS Qtd_Total
FROM belmicro.fato_operacoes AS O
INNER JOIN belmicro.fato_itens AS I ON O.NumUnicoNota = I.NumUnicoNota
INNER JOIN cadastros.dim_produtos AS P ON I.CodProduto = P.CodProduto
INNER JOIN cadastros.dim_grupo_produtos AS G ON P.CodGrupoProduto = G.CodGrupoProduto
WHERE O.DataNegociacao >= '2025-01-01' 
  AND O.DataNegociacao <= GETDATE()
  AND O.CodTipoOperacao IN (1102,1119,1607)
  AND G.NomeGrupoPai IN ('DESKTOP', 'ALL IN ONE', 'MONITORES', 'TV') 
  AND G.LinhadeNegocio = 'WordPC/Skill'
GROUP BY I.CodProduto, P.DescricaoProduto, G.NomeGrupoPai, O.NumNota, O.CodTipoOperacao, O.DataNegociacao, O.DataMovimento, O.DataEntradaSaida, O.DataAlteracao

ORDER BY DataAlteracao DESC;