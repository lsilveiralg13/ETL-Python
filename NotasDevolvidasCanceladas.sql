SELECT
    -- 1. Identificação do Produto (SKU)
    FI.CodProduto AS SKU,
    PR.DescricaoProduto,
    GP.NomeGrupoProduto,
    
    -- Dados da Operação
    FO.NumNota,
    FO.NumUnicoNota,
    FO.DataNegociacao,
    FO.DescricaoTipoOperacao,
    FO.StatusNota,
    FO.StatusNFe,
    
    -- 3. Identificação de Fonte/Ticket
    FO.CodIntegracaoMkt AS Fonte_Integracao, 
    FO.NumRemessa AS Documento_Origem,
    
    -- Dados do Parceiro (Cliente)
    P.NomeParceiro,
    P.CpfCnpj,
    C.NomeCidade,
    U.SiglaUf,
    
    -- Dados do Vendedor
    V.NomeVendedor,
    V.LinhaNegocio,
    
    -- Valores e Quantidades
    FI.QtdNegociada,
    FI.ValorTotal,
    FO.ValorFrete
    
FROM belmicro.fato_operacoes FO
INNER JOIN belmicro.fato_itens FI 
    ON FO.NumUnicoNota = FI.NumUnicoNota
LEFT JOIN cadastros.dim_parceiros P 
    ON FO.CodParceiro = P.CodParceiro
LEFT JOIN cadastros.dim_cidades C 
    ON P.CodCidade = C.CodCidade
LEFT JOIN cadastros.dim_uf U 
    ON C.CodUF = U.CodUF
LEFT JOIN cadastros.dim_vendedor V 
    ON FO.CodVendedor = V.CodVendedor
LEFT JOIN cadastros.dim_produtos PR 
    ON FI.CodProduto = PR.CodProduto
LEFT JOIN cadastros.dim_grupo_produtos GP 
    ON PR.CodGrupoProduto = GP.CodGrupoProduto

WHERE 
    -- Filtro de Período (Janeiro de 2025 até Abril de 2026)
    FO.DataNegociacao BETWEEN '2025-01-01' AND '2026-04-30'
    
    -- Correção da lógica de Status e Operação
    AND (
        FO.StatusNota = 'C' 
        OR FO.StatusNFe LIKE '%Cancelada%' 
        OR FO.StatusNFe LIKE '%Cancelamento%' 
        OR FO.StatusNFe LIKE '%Devolução%'
        OR FO.DescricaoTipoOperacao LIKE '%DEV%' 
        OR FO.DescricaoTipoOperacao LIKE '%ESTORNO%'
    )
    
ORDER BY FO.DataNegociacao DESC;