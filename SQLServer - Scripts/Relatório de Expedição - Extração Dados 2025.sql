/* RELATÓRIO DE EXPEDIÇÃO CONSOLIDADO - SKU 2025 */

SELECT
    I.CodProduto AS CodProd,
    P.DescricaoProduto,
    P.Marca AS Fornecedor,
    P.ModeloMkt AS Modelo,
    GP.NomeGrupoPai AS Familia,
    GP.LinhaDeNegocio,
    GP.NomeGrupoFamilia,
    P.UsadoComo,

    SUM(CAST(I.QtdNegociada AS BIGINT)) AS QuantidadeExpedida,
    SUM(CAST(I.ValorTotal AS DECIMAL(18,2))) AS ValorExpedido

FROM belmicro.fato_itens I WITH (NOLOCK)

INNER JOIN belmicro.fato_itens_notas_expedidas EXP WITH (NOLOCK)
    ON EXP.NumUnicoNota = I.NumUnicoNota

INNER JOIN cadastros.dim_produtos P WITH (NOLOCK)
    ON P.CodProduto = I.CodProduto

INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK)
    ON GP.CodGrupoProduto = P.CodGrupoProduto

INNER JOIN belmicro.fato_operacoes O WITH (NOLOCK)
    ON O.NumUnicoNota = EXP.NumUnicoNota

WHERE

    EXP.DataExpedicao >= '2025-01-01'
    AND EXP.DataExpedicao < '2026-01-01'

    AND EXP.DataExpedicao IS NOT NULL

    AND
    (
        (
            P.UsadoComo IN ('Venda (fabricação própria)', 'Revenda')
            AND GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel')
            AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD')
            AND GP.NomeGrupoPai <> 'COMPONENTES'
        )

        OR

        (
            P.UsadoComo IN ('Revenda', 'Venda (fabricação própria)')
            AND P.Marca IN ('HQ', 'KONKA', '3GREEN')
            AND GP.NomeGrupoPai IN (
                'AR CONDICIONADO',
                'FRIGOBAR',
                'FORNO',
                'NOTEBOOK',
                'FRITADEIRA',
                'REFRIGERADOR',
                'GRILL E SANDUICHEIRAS',
                'FREEZER',
                'ADEGA',
                'COOKTOPS',
                'LAVADOURA LOUCAS',
                'CERVEJEIRA',
                'MAQUINA DE GELO',
                'PANELA ELETRICA',
                'MONITORES',
                'TV',
                'MONITOR'
            )
            AND GP.NomeGrupoPai <> 'COMPONENTES'

            AND NOT (
                GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel')
                AND P.Marca IN (
                    'HQ',
                    '3GREEN',
                    'EASYPC',
                    'SKILL',
                    'QUANTUM',
                    'CORPC',
                    'FOXPC',
                    'AMD'
                )
            )
        )
    )

    -- EXPURGO LOGÍSTICO

    AND O.CodTipoOperacao BETWEEN 3100 AND 3396

    AND O.CodTipoOperacao NOT IN (
        3146,3155,3248,3338,3397,3249,
        3356,3311,3315,3292,3341,3376,
        3375,3205,3354,3328,3266
    )

    AND O.CodTipoOperacao NOT IN (
        3000,3005,3017,3029
    )

    AND O.CodTipoOperacao NOT IN (
        3207,3210,3230,3245
    )

    AND O.DescricaoTipoOperacao NOT LIKE '%FULL%'

GROUP BY

    I.CodProduto,
    P.DescricaoProduto,
    P.Marca,
    P.ModeloMkt,
    GP.NomeGrupoPai,
    GP.LinhaDeNegocio,
    GP.NomeGrupoFamilia,
    P.UsadoComo

ORDER BY

    QuantidadeExpedida DESC;