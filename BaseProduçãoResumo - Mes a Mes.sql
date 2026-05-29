WITH 

-- ============================================================
-- 1. PRODUCAO: Quantidade produzida por OP e Produto Acabado
-- ============================================================
PRODUCAO AS (
    SELECT
        act.OrdemProducao,
        app.CodProdutoAcabado,
        SUM(app.QuantidadeApontada) AS Qtd_Produzida
    FROM producao.fato_apontamento ap
    INNER JOIN producao.fato_atividade_op act
        ON ap.CodItemAtividade = act.CodItemAtividade
    INNER JOIN producao.fato_apontamento_produto app
        ON ap.CodApontamentoUnico = app.CodApontamentoUnico
    WHERE app.CodMotivoPerda IS NULL
    GROUP BY
        act.OrdemProducao,
        app.CodProdutoAcabado
),

-- ============================================================
-- 2. EXPEDICAO: Data de expedição por NumUnicoNota
-- ============================================================
EXPEDICAO AS (
    SELECT
        NumUnicoNota,
        MAX(DataExpedicao) AS DataExpedicao
    FROM belmicro.fato_itens_notas_expedidas
    GROUP BY NumUnicoNota
),

-- ============================================================
-- 3. SKU_ATRIBUTOS: Atributos do produto direto da dimensão
--    SEM depender de fato_operacoes — resolve o problema
--    de produtos que ainda não têm nota comercial
-- ============================================================
SKU_ATRIBUTOS AS (
    SELECT
        P.CodProduto,
        P.DescricaoProduto,
        P.Marca,
        P.UsadoComo,
        GP.NomeGrupoPai      AS Familia,
        GP.NomeGrupoProduto  AS SubFamilia,
        GP.LinhaDeNegocio
    FROM cadastros.dim_produtos P
    INNER JOIN cadastros.dim_grupo_produtos GP
        ON P.CodGrupoProduto = GP.CodGrupoProduto
    WHERE
        (
            -- Regra A: WordPC / Comprebel
            (
                GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel')
                AND P.Marca IN (
                    'HQ', '3GREEN', 'EASYPC',
                    'SKILL', 'QUANTUM',
                    'CORPC', 'FOXPC', 'AMD'
                )
                AND GP.NomeGrupoPai <> 'COMPONENTES'
            )
            OR
            -- Regra B: Eletrodomésticos HQ / KONKA / 3GREEN
            (
                P.Marca IN ('HQ', 'KONKA', '3GREEN')
                AND GP.NomeGrupoPai IN (
                    'AR CONDICIONADO', 'FRIGOBAR', 'FORNO',
                    'NOTEBOOK', 'FRITADEIRA', 'REFRIGERADOR',
                    'GRILL E SANDUICHEIRAS', 'FREEZER', 'ADEGA',
                    'COOKTOPS', 'LAVADOURA LOUCAS', 'CERVEJEIRA',
                    'MAQUINA DE GELO', 'PANELA ELETRICA',
                    'MONITORES', 'TV', 'MONITOR'
                )
            )
        )
)

-- ============================================================
-- RESULTADO FINAL
-- ============================================================
SELECT

    -- Identificadores principais
    nota.NotaFaturamento                        AS NumeroNota,
    o.NumUnicoNota,
    nota.OrdemProducao,
    opi.CodProdutoAcabado                       AS CodProduto,

    -- Descrição e atributos do produto
    sku.DescricaoProduto,
    sku.Familia,
    sku.SubFamilia,
    sku.Marca                                   AS Fornecedor_Marca,
    sku.LinhaDeNegocio                          AS LinhaDeProduto,
    sku.UsadoComo,

    -- Quantidades
    opi.QuantidadeOriginalSemAjuste             AS Qtd_OP_Planejada,
    COALESCE(prod.Qtd_Produzida, 0)             AS Qtd_Produzida,

    -- Datas
    CAST(op.DataHoraInclusao    AS DATE)        AS Data_Producao,
    CAST(op.DataHoraFinalizacao AS DATE)        AS Data_Finalizacao,
    CAST(o.DataNegociacao       AS DATE)        AS Data_Negociacao,
    CAST(exp.DataExpedicao      AS DATE)        AS Data_Expedicao,

    -- Informações adicionais da nota
    o.NumNota,
    o.StatusNota,
    o.LinhaDeNegocio                            AS LinhaDeNegocio_NF

FROM producao.fato_instancia_item_nota nota

-- OP → detalhes da Ordem de Produção
INNER JOIN producao.fato_ordem_producao op
    ON nota.OrdemProducao = op.OrdemProducao

-- OP → itens (produto acabado planejado)
INNER JOIN producao.fato_ordem_producao_item opi
    ON op.OrdemProducao = opi.OrdemProducao

-- OP → quantidades efetivamente produzidas
LEFT JOIN PRODUCAO prod
    ON op.OrdemProducao         = prod.OrdemProducao
   AND opi.CodProdutoAcabado    = prod.CodProdutoAcabado

-- Nota → dados comerciais (operação/NF)
LEFT JOIN belmicro.fato_operacoes o
    ON nota.NotaFaturamento = o.NumUnicoNota

-- Nota → data de expedição
LEFT JOIN EXPEDICAO exp
    ON nota.NotaFaturamento = exp.NumUnicoNota

-- Produto → atributos (join direto na dimensão, sem depender de nota)
INNER JOIN SKU_ATRIBUTOS sku
    ON opi.CodProdutoAcabado = sku.CodProduto

WHERE
    op.DataHoraInclusao >= '2025-01-01'
    AND op.DataHoraInclusao <= GETDATE()

ORDER BY
    Data_Producao   DESC,
    nota.OrdemProducao,
    opi.CodProdutoAcabado;