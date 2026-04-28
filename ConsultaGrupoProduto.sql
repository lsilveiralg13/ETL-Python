SELECT 
    GP.NomeGrupoPai,
    P.UsadoComo,
    COUNT(P.CodProduto) AS QtdProdutos,
    GP.LinhaDeNegocio,
    STRING_AGG(CAST(P.DescricaoProduto AS VARCHAR(MAX)), ' | ') WITHIN GROUP (ORDER BY P.DescricaoProduto) AS ExemplosProdutos
FROM cadastros.dim_grupo_produtos GP WITH (NOLOCK)
LEFT JOIN cadastros.dim_produtos P WITH (NOLOCK) 
    ON GP.CodGrupoProduto = P.CodGrupoProduto

WHERE GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel') 

GROUP BY 
    GP.NomeGrupoPai,
    P.UsadoComo, 
    GP.LinhaDeNegocio
ORDER BY 
    QtdProdutos DESC;