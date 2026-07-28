-- ==============================================================================
-- DECLARAÇÃO DE TODAS AS CTES (2025 E 2026) LOGO NO INÍCIO DO CÓDIGO
-- ==============================================================================
WITH 

-- --- CTEs EXCLUSIVAS DO ANO 2025 ---
ULTIMA_ATIVIDADE_OP_25 AS (
    SELECT 
        OrdemProducao,
        MAX(CodItemAtividade) AS CodItemAtividade_Final
    FROM producao.fato_atividade_op WITH (NOLOCK)
    GROUP BY OrdemProducao
),

BASE_PRODUCAO_SLA_25 AS (
    SELECT
        act.OrdemProducao,
        app.CodProdutoAcabado                           AS CodProd,
        YEAR(MIN(ap.DataHoraApontamento))                    AS Ano, 
        MONTH(MIN(ap.DataHoraApontamento))                   AS Mes,
        DATEPART(ISO_WEEK, MIN(ap.DataHoraApontamento))      AS Semana, 
        CAST(MIN(ap.DataHoraApontamento) AS DATE)            AS Data_Producao,
        MIN(ap.DataHoraApontamento)                          AS DataHora_Apontamento,
        SUM(CAST(app.QuantidadeApontada AS INT))        AS Quantidade_Produzida
    FROM producao.fato_apontamento ap WITH (NOLOCK)
    INNER JOIN ULTIMA_ATIVIDADE_OP_25 ult ON ap.CodItemAtividade = ult.CodItemAtividade_Final
    INNER JOIN producao.fato_atividade_op act WITH (NOLOCK) ON ap.CodItemAtividade = act.CodItemAtividade
    INNER JOIN producao.fato_apontamento_produto app WITH (NOLOCK) ON ap.CodApontamentoUnico = app.CodApontamentoUnico
    WHERE ap.DataHoraApontamento IS NOT NULL
    GROUP BY act.OrdemProducao, app.CodProdutoAcabado
),

SKU_ATRIBUTOS_25 AS (
    SELECT
        P.CodProduto, P.DescricaoProduto, P.Marca AS Fornecedor, P.ModeloMkt AS Modelo,
        GP.NomeGrupoPai AS Familia, GP.LinhaDeNegocio, GP.NomeGrupoFamilia, P.UsadoComo
    FROM cadastros.dim_produtos P WITH (NOLOCK)
    INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) ON GP.CodGrupoProduto = P.CodGrupoProduto
    WHERE (P.UsadoComo IN ('Venda (fabricação própria)', 'Revenda') AND GP.LinhaDeNegocio IN ('WordPC/Skill')
           AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD') AND GP.NomeGrupoPai <> 'COMPONENTES')
),

PLANEJAMENTO_OP_25 AS (
    SELECT 
        OrdemProducao, CodProdutoAcabado,
        MAX(CAST(QuantidadeAProduzir AS INT)) AS Qtd_Planejada_Item
    FROM producao.fato_ordem_producao_item WITH (NOLOCK)
    GROUP BY OrdemProducao, CodProdutoAcabado
),

LINK_LOGISTICO_25 AS (
    SELECT OrdemProducao, MAX(NotaFaturamento) AS NumUnicoNota FROM producao.fato_instancia_item_nota WITH (NOLOCK) GROUP BY OrdemProducao
),

EXPEDICAO_MAX_25 AS (
    SELECT NumUnicoNota, MAX(DataExpedicao) AS DataExpedicao FROM belmicro.fato_itens_notas_expedidas WITH (NOLOCK) GROUP BY NumUnicoNota
),

-- --- CTEs EXCLUSIVAS DO ANO 2026 ---
BASE_PRODUCAO_SLA_26 AS (
    SELECT
        ap.OrdemProducao,
        pa.CodProdutoAcabado                                AS CodProd,
        pr.NumUnicoNotaPedido,
        YEAR(MIN(ap.DataHoraEmbalagem))                      AS Ano, 
        MONTH(MIN(ap.DataHoraEmbalagem))                     AS Mes,
        DATEPART(ISO_WEEK, MIN(ap.DataHoraEmbalagem))        AS Semana, 
        CAST(MIN(ap.DataHoraEmbalagem) AS DATE)              AS Data_Producao,
        MIN(ap.DataHoraEmbalagem)                            AS DataHora_Apontamento,
        COUNT(CASE WHEN ap.DataHoraEmbalagem IS NOT NULL THEN ap.SerieProdutoAcabado END) AS Quantidade_Produzida
    FROM producao.fato_ordem_producao_seriepa_ciclo ap WITH (NOLOCK)
    INNER JOIN producao.fato_ordem_producao pr WITH (NOLOCK) ON pr.OrdemProducao = ap.OrdemProducao
    LEFT JOIN producao.fato_ordem_producao_item pa WITH (NOLOCK) ON ap.OrdemProducao = pa.OrdemProducao
    WHERE ap.DataHoraApontamento IS NOT NULL AND ap.DataHoraEmbalagem IS NOT NULL
    GROUP BY ap.OrdemProducao, pa.CodProdutoAcabado, pr.NumUnicoNotaPedido
),
 
SKU_ATRIBUTOS_26 AS (
    SELECT
        P.CodProduto, P.DescricaoProduto, P.Marca AS Fornecedor, P.ModeloMkt AS Modelo,
        GP.NomeGrupoPai AS Familia, GP.LinhaDeNegocio, GP.NomeGrupoFamilia, P.UsadoComo
    FROM cadastros.dim_produtos P WITH (NOLOCK)
    INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) ON GP.CodGrupoProduto = P.CodGrupoProduto
    WHERE (P.UsadoComo IN ('Venda (fabricação própria)', 'Revenda') AND GP.LinhaDeNegocio IN ('WordPC/Skill')
           AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD') AND GP.NomeGrupoPai <> 'COMPONENTES')
),
 
PLANEJAMENTO_OP_26 AS (
    SELECT 
        OrdemProducao, CodProdutoAcabado,
        MAX(CAST(QuantidadeAProduzir AS INT)) AS Qtd_Planejada_Item
    FROM producao.fato_ordem_producao_item WITH (NOLOCK)
    GROUP BY OrdemProducao, CodProdutoAcabado
),

EXPEDICAO_MAX_26 AS (
    SELECT NumUnicoNota, MAX(DataExpedicao) AS DataExpedicao FROM belmicro.fato_itens_notas_expedidas WITH (NOLOCK) GROUP BY NumUnicoNota
)

-- ==============================================================================
-- EXECUÇÃO ISOLADA E EMPILHAMENTO DOS RESULTADOS (UNION ALL)
-- ==============================================================================
SELECT 
    Processo, OP, NumUnicoNota, NumeroNota, Ano, Mes, Semana, DataProducao, DataHoraApontamento,
    CodProd, DescricaoProduto, Fornecedor, Modelo, Familia, LinhaDeNegocio, NomeGrupoFamilia, UsadoComo,
    Qtd_Produzida, Qtd_Planejada, Saldo, Situacao, DataFaturamento, DataExpedicao
FROM (
    
    -- Bloco de Execução: Dados de 2025
    SELECT
        pp.DescricaoProcesso                                        AS Processo,
        PROD.OrdemProducao                                          AS OP,
        LINK.NumUnicoNota                                           AS NumUnicoNota,
        O.NumNota                                                   AS NumeroNota,
        PROD.Ano, PROD.Mes, PROD.Semana, PROD.Data_Producao        AS DataProducao,
        PROD.DataHora_Apontamento                                   AS DataHoraApontamento,
        PROD.CodProd, SKU.DescricaoProduto, SKU.Fornecedor, SKU.Modelo,
        SKU.Familia, SKU.LinhaDeNegocio, SKU.NomeGrupoFamilia, SKU.UsadoComo,
        PROD.Quantidade_Produzida                                   AS Qtd_Produzida,
        COALESCE(PLAN_OP.Qtd_Planejada_Item, 0)                     AS Qtd_Planejada,
        (COALESCE(PLAN_OP.Qtd_Planejada_Item, 0) - PROD.Quantidade_Produzida) AS Saldo,
        'PRODUZIDO'                                                 AS Situacao,
        CAST(O.DataNegociacao AS DATE)                              AS DataFaturamento,
        CAST(EXP.DataExpedicao AS DATE)                             AS DataExpedicao
    FROM BASE_PRODUCAO_SLA_25 PROD
    INNER JOIN SKU_ATRIBUTOS_25 SKU ON PROD.CodProd = SKU.CodProduto
    INNER JOIN producao.fato_ordem_producao fop WITH (NOLOCK) ON PROD.OrdemProducao = fop.OrdemProducao
    INNER JOIN producao.dim_processo_producao pp WITH (NOLOCK) ON fop.CodProcessoUnico = pp.CodProcessoUnico
    LEFT JOIN PLANEJAMENTO_OP_25 PLAN_OP ON PROD.OrdemProducao = PLAN_OP.OrdemProducao AND PROD.CodProd = PLAN_OP.CodProdutoAcabado
    LEFT JOIN LINK_LOGISTICO_25 LINK ON PROD.OrdemProducao = LINK.OrdemProducao
    LEFT JOIN belmicro.fato_operacoes O WITH (NOLOCK) ON LINK.NumUnicoNota = O.NumUnicoNota
    LEFT JOIN EXPEDICAO_MAX_25 EXP ON LINK.NumUnicoNota = EXP.NumUnicoNota
    WHERE PROD.Data_Producao >= '2025-01-01' AND PROD.Data_Producao <= CAST(GETDATE() AS DATE)
      AND (pp.DescricaoProcesso LIKE '%Monitor Maestro Plus%' OR pp.DescricaoProcesso LIKE '%Computadores Maestro Plus%')
      AND pp.DescricaoProcesso NOT LIKE '61 %'

    UNION ALL

    -- Bloco de Execução: Dados de 2026
    SELECT
        'NÃO COMPATÍVEL (2026)'                                     AS Processo, 
        PROD.OrdemProducao                                          AS OP,
        PROD.NumUnicoNotaPedido                                     AS NumUnicoNota,
        O.NumNota                                                   AS NumeroNota,
        PROD.Ano, PROD.Mes, PROD.Semana, PROD.Data_Producao        AS DataProducao,
        PROD.DataHora_Apontamento                                   AS DataHoraApontamento,
        PROD.CodProd, SKU.DescricaoProduto, SKU.Fornecedor, SKU.Modelo,
        SKU.Familia, SKU.LinhaDeNegocio, SKU.NomeGrupoFamilia, SKU.UsadoComo,
        PROD.Quantidade_Produzida                                   AS Qtd_Produzida,
        COALESCE(PLAN_OP.Qtd_Planejada_Item, 0)                     AS Qtd_Planejada,
        (COALESCE(PLAN_OP.Qtd_Planejada_Item, 0) - PROD.Quantidade_Produzida) AS Saldo,
        'PRODUZIDO'                                                 AS Situacao,
        CAST(O.DataNegociacao AS DATE)                              AS DataFaturamento,
        CAST(EXP.DataExpedicao AS DATE)                             AS DataExpedicao
    FROM BASE_PRODUCAO_SLA_26 PROD
    INNER JOIN SKU_ATRIBUTOS_26 SKU ON PROD.CodProd = SKU.CodProduto
    INNER JOIN producao.fato_ordem_producao fop WITH (NOLOCK) ON PROD.OrdemProducao = fop.OrdemProducao
    LEFT JOIN PLANEJAMENTO_OP_26 PLAN_OP ON PROD.OrdemProducao = PLAN_OP.OrdemProducao AND PROD.CodProd = PLAN_OP.CodProdutoAcabado
    LEFT JOIN belmicro.fato_operacoes O WITH (NOLOCK) ON PROD.NumUnicoNotaPedido = O.NumUnicoNota
    LEFT JOIN EXPEDICAO_MAX_26 EXP ON PROD.NumUnicoNotaPedido = EXP.NumUnicoNota
    WHERE PROD.Data_Producao >= '2025-01-01' AND PROD.Data_Producao <= CAST(GETDATE() AS DATE)

) AS CONSULTA_CONSOLIDADA

ORDER BY 
    CONSULTA_CONSOLIDADA.DataHoraApontamento DESC, 
    CONSULTA_CONSOLIDADA.OP, 
    CONSULTA_CONSOLIDADA.CodProd;