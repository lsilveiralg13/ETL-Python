SELECT
    YEAR(EXP.DataExpedicao) AS Ano,
    I.CodProduto AS CodProd,
    P.DescricaoProduto,
    SUM(CAST(I.QtdNegociada AS INT)) AS Quantidade
FROM belmicro.fato_itens I WITH (NOLOCK)
INNER JOIN belmicro.fato_itens_notas_expedidas EXP WITH (NOLOCK) ON I.NumUnicoNota = EXP.NumUnicoNota
INNER JOIN cadastros.dim_produtos P WITH (NOLOCK) ON P.CodProduto = I.CodProduto
INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) ON GP.CodGrupoProduto = P.CodGrupoProduto
INNER JOIN belmicro.fato_operacoes O WITH (NOLOCK) ON O.NumUnicoNota = EXP.NumUnicoNota
WHERE EXP.DataExpedicao IS NOT NULL
    AND YEAR(EXP.DataExpedicao) IN (2024, 2025, 2026)
    AND P.UsadoComo IN ('Venda (fabricação própria)', 'Revenda')
    AND GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel')
    AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'QUANTUM', 'FOXPC', 'TAYREX', 'FORTT', 'AMD')
    AND GP.NomeGrupoPai <> 'COMPONENTES'
    AND O.CodTipoOperacao BETWEEN 3100 AND 3396 
    AND O.CodTipoOperacao NOT IN (3146, 3155, 3248, 3338, 3397, 3249, 3356, 3311, 3315, 3292, 3341, 3376, 3375, 3205, 3354, 3328, 3266)
    AND O.CodTipoOperacao NOT IN (3000, 3005, 3017, 3029)       
    AND O.CodTipoOperacao NOT IN (3207, 3210, 3230, 3245)
GROUP BY YEAR(EXP.DataExpedicao), I.CodProduto, P.DescricaoProduto

UNION ALL

SELECT
    YEAR(EXP.DataExpedicao) AS Ano,
    I.CodProduto AS CodProd, 
    P.DescricaoProduto, 
    SUM(CAST(I.QtdNegociada AS INT)) AS Quantidade
FROM belmicro.fato_itens I WITH (NOLOCK)
INNER JOIN belmicro.fato_itens_notas_expedidas EXP WITH (NOLOCK) ON EXP.NumUnicoNota = I.NumUnicoNota
INNER JOIN cadastros.dim_produtos P WITH (NOLOCK) ON P.CodProduto = I.CodProduto
INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) ON GP.CodGrupoProduto = P.CodGrupoProduto
INNER JOIN belmicro.fato_operacoes O WITH (NOLOCK) ON O.NumUnicoNota = EXP.NumUnicoNota
WHERE EXP.DataExpedicao IS NOT NULL
    AND YEAR(EXP.DataExpedicao) IN (2024, 2025, 2026)
    AND P.UsadoComo IN ('Revenda', 'Venda (fabricação própria)')
    AND P.Marca IN ('HQ', 'KONKA', '3GREEN')
    AND GP.NomeGrupoPai IN ('AR CONDICIONADO', 'FRIGOBAR', 'FORNO', 'NOTEBOOK', 'FRITADEIRA', 'REFRIGERADOR', 'GRILL E SANDUICHEIRAS', 'FREEZER', 'ADEGA', 'COOKTOPS', 'LAVADOURA LOUCAS', 'CERVEJEIRA', 'MAQUINA DE GELO', 'PANELA ELETRICA', 'MONITORES', 'TV', 'MONITOR')
    AND GP.NomeGrupoPai <> 'COMPONENTES'
    AND O.CodTipoOperacao BETWEEN 3100 AND 3396 
    AND O.CodTipoOperacao NOT IN (3146, 3155, 3248, 3338, 3397, 3249, 3356, 3311, 3315, 3292, 3341, 3376, 3375, 3205, 3354, 3328, 3266)
    AND O.CodTipoOperacao NOT IN (3000, 3005, 3017, 3029)
    AND O.CodTipoOperacao NOT IN (3207, 3210, 3230, 3245)
GROUP BY YEAR(EXP.DataExpedicao), I.CodProduto, P.DescricaoProduto;