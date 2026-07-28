SELECT 
    ISNULL(GP.LinhaDeNegocio, 'S/ LINHA DE NEGOCIO') AS LinhaDeNegocio,
    ISNULL(P.Marca, 'S/ MARCA') AS Marca,
    ISNULL(GP.NomeGrupoPai, '<SEM GRUPO PAI>') AS NomeGrupoPai,
    ISNULL(GP.NomeGrupoFamilia, '<SEM FAMILIA>') AS NomeGrupoFamilia,
    COUNT(P.CodProduto) AS Qtd_Produtos_Cadastrados
FROM cadastros.dim_produtos P WITH (NOLOCK)
INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) 
    ON GP.CodGrupoProduto = P.CodGrupoProduto
GROUP BY 
    GP.LinhaDeNegocio, 
    P.Marca, 
    GP.NomeGrupoPai, 
    GP.NomeGrupoFamilia
ORDER BY 
    Qtd_Produtos_Cadastrados DESC, 
    LinhaDeNegocio, 
    Marca;