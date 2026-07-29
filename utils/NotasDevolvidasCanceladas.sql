SELECT 
    -- Identificação do Produto e Classificação
    FI.CodProduto AS SKU,
    PR.DescricaoProduto,
    GP.NomeGrupoPai AS Familia,
    PR.Marca,
    PR.UsadoComo,
    GP.LinhaDeNegocio,
    
    -- Dados da Operação (Foco em Devolução/Cancelamento)
    FO.NumNota,
    FO.NumUnicoNota,
    FO.DataNegociacao,
    FO.DescricaoTipoOperacao,
    FO.StatusNota,
    
    -- Dados do Parceiro e Vendedor
    P.NomeParceiro,
    V.NomeVendedor,
    
    -- Valores e Quantidades
    FI.QtdNegociada,
    FI.ValorTotal
    
FROM belmicro.fato_operacoes FO WITH (NOLOCK)
INNER JOIN belmicro.fato_itens FI WITH (NOLOCK) 
    ON FO.NumUnicoNota = FI.NumUnicoNota
INNER JOIN cadastros.dim_produtos PR WITH (NOLOCK) 
    ON FI.CodProduto = PR.CodProduto
INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) 
    ON PR.CodGrupoProduto = GP.CodGrupoProduto
LEFT JOIN cadastros.dim_parceiros P WITH (NOLOCK) 
    ON FO.CodParceiro = P.CodParceiro
LEFT JOIN cadastros.dim_vendedor V WITH (NOLOCK) 
    ON FO.CodVendedor = V.CodVendedor

WHERE 
    -- 1. Filtro de Período
    FO.DataNegociacao BETWEEN '2025-11-01' AND '2025-11-30'
    
    -- 2. Filtro de Tipos de Operação Específicos (Conforme Imagem)
    AND FO.DescricaoTipoOperacao IN (
        'DEV.VENDA NF PRÓPRIA - EMP 16 (FULL)',
        'DEV.VENDA NF PRÓPRIA - EMP 2 (CG)',
        'DEV.VENDA NF PRÓPRIA - EMP 4 (ES)',
        'DEV.VENDA NF PRÓPRIA - EMP 6 (EX)',
        'DEV.VENDA NF PRÓPRIA - MATRIZ',
        'DEV.VENDA NF TERC. - EMP 2 (CG)',
        'DEV.VENDA NF TERC. - MATRIZ'
    )
    
    -- 3. Lógica Geral de Devolução/Cancelamento
    AND (
        FO.StatusNota = 'C' 
        OR FO.StatusNFe LIKE '%Cancelada%' 
        OR FO.StatusNFe LIKE '%Cancelamento%' 
        OR FO.StatusNFe LIKE '%Devolução%'
        OR FO.DescricaoTipoOperacao LIKE '%DEV%' 
        OR FO.DescricaoTipoOperacao LIKE '%ESTORNO%'
    )

    -- 4. IMPLEMENTAÇÃO DAS REGRAS DE EXPEDIÇÃO (A + B)
    AND GP.NomeGrupoPai <> 'COMPONENTES'
    AND (
        -- REGRA A: Fabricação Própria / Linhas Principais
        (
            PR.UsadoComo IN ('Venda (fabricação própria)', 'Revenda') 
            AND GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel')
            AND PR.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD')
        )
        OR 
        -- REGRA B: Revenda HQ + Linha Branca / Monitores
        (
            PR.UsadoComo IN ('Revenda', 'Venda (fabricação própria)')
            AND PR.Marca IN ('HQ', 'KONKA', '3GREEN')
            AND GP.NomeGrupoPai IN (
                'AR CONDICIONADO', 'FRIGOBAR', 'FORNO', 'NOTEBOOK', 'FRITADEIRA', 
                'REFRIGERADOR', 'GRILL E SANDUICHEIRAS', 'FREEZER', 'ADEGA', 
                'COOKTOPS', 'LAVADOURA LOUCAS', 'CERVEJEIRA', 'MAQUINA DE GELO', 
                'PANELA ELETRICA', 'MONITORES', 'TV', 'MONITOR'
            )
            -- Exclusão da sobreposição com a Regra A para manter integridade
            AND NOT (
                GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel') 
                AND PR.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD')
            )
        )
    )

ORDER BY FO.DataNegociacao DESC;