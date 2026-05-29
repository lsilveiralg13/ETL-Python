WITH PRODUCAO AS (
    SELECT
        act.OrdemProducao,
        app.CodProdutoAcabado,
        SUM(app.QuantidadeApontada) AS Qtd_Produzida
    FROM producao.fato_apontamento ap
    INNER JOIN producao.fato_atividade_op act
        ON ap.CodItemAtividade = act.CodItemAtividade
    INNER JOIN producao.fato_apontamento_produto app
        ON ap.CodApontamentoUnico = app.CodApontamentoUnico
    GROUP BY
        act.OrdemProducao,
        app.CodProdutoAcabado
),

SKU_NEGOCIO AS (
    SELECT DISTINCT
        I.CodProduto
    FROM belmicro.fato_operacoes O
    INNER JOIN belmicro.fato_itens I
        ON O.NumUnicoNota = I.NumUnicoNota
    INNER JOIN cadastros.dim_produtos P
        ON I.CodProduto = P.CodProduto
    INNER JOIN cadastros.dim_grupo_produtos GP
        ON P.CodGrupoProduto = GP.CodGrupoProduto
    WHERE
        O.CodTipoOperacao IN (1102,1119,1607)
        AND
        (
            (
                GP.LinhaDeNegocio IN ('WordPC/Skill','Comprebel')
                AND P.Marca IN ('HQ','3GREEN','EASYPC','SKILL','QUANTUM','CORPC','FOXPC','AMD')
                AND GP.NomeGrupoPai <> 'COMPONENTES'
            )
            OR
            (
                P.Marca IN ('HQ','KONKA','3GREEN')
                AND GP.NomeGrupoPai IN (
                    'AR CONDICIONADO','FRIGOBAR','FORNO','NOTEBOOK','FRITADEIRA',
                    'REFRIGERADOR','GRILL E SANDUICHEIRAS','FREEZER','ADEGA',
                    'COOKTOPS','LAVADOURA LOUCAS','CERVEJEIRA','MAQUINA DE GELO',
                    'PANELA ELETRICA','MONITORES','TV','MONITOR'
                )
            )
        )
),

DADOS_BASE AS (
    SELECT
        MONTH(op.DataHoraInclusao) AS Numero_Mes,
        RIGHT('0' + CAST(MONTH(op.DataHoraInclusao) AS VARCHAR(2)), 2) + ' - ' + 
        DATENAME(month, op.DataHoraInclusao) AS Mes_Descricao,
        YEAR(op.DataHoraInclusao) AS Ano,
        COALESCE(prod.Qtd_Produzida, 0) AS Qtd_Produzida
    FROM producao.fato_ordem_producao op
    INNER JOIN producao.fato_ordem_producao_item opi
        ON op.OrdemProducao = opi.OrdemProducao
    LEFT JOIN PRODUCAO prod
        ON op.OrdemProducao = prod.OrdemProducao
       AND opi.CodProdutoAcabado = prod.CodProdutoAcabado
    INNER JOIN SKU_NEGOCIO sku
        ON opi.CodProdutoAcabado = sku.CodProduto
    WHERE op.DataHoraInclusao >= '2025-01-01'
      AND op.DataHoraInclusao <= '2026-05-22 23:59:59'
)

-- =====================================================
-- MATRIZ DE RESULTADO PURA POR MÊS E ANO (PIVOT)
-- =====================================================
SELECT 
    Mes_Descricao AS [Mês],
    SUM(CASE WHEN Ano = 2025 THEN Qtd_Produzida ELSE 0 END) AS [2025],
    SUM(CASE WHEN Ano = 2026 THEN Qtd_Produzida ELSE 0 END) AS [2026],
    SUM(Qtd_Produzida) AS [Total Geral]
FROM DADOS_BASE
GROUP BY 
    Numero_Mes,
    Mes_Descricao
ORDER BY 
    Numero_Mes ASC;