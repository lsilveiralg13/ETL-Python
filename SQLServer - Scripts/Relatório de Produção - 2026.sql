WITH 

-- ============================================================
-- 1. IDENTIFICAÇÃO APENAS DA ATIVIDADE DE FECHAMENTO DE CADA OP
-- ============================================================
ULTIMA_ATIVIDADE_OP AS (
    SELECT 
        OrdemProducao,
        MAX(CodItemAtividade) AS CodItemAtividade_Final
    FROM producao.fato_atividade_op WITH (NOLOCK)
    GROUP BY OrdemProducao
),

-- ============================================================
-- 2. BASE ATÔMICA DE PRODUÇÃO (Granularidade da SLA: 1 Linha por OP/SKU)
-- ============================================================
BASE_PRODUCAO_SLA AS (
    SELECT
        act.OrdemProducao,
        app.CodProdutoAcabado                           AS CodProd,
        
        -- Marcos temporais baseados no nascimento real do lote (Primeiro Bipe)
        YEAR(MIN(ap.DataHoraApontamento))                    AS Ano, 
        MONTH(MIN(ap.DataHoraApontamento))                   AS Mes,
        DATEPART(ISO_WEEK, MIN(ap.DataHoraApontamento))      AS Semana, 
        CAST(MIN(ap.DataHoraApontamento) AS DATE)            AS Data_Producao,
        MIN(ap.DataHoraApontamento)                          AS DataHora_Apontamento,
        
        -- Volume de Production Realizado Livre de Dobras
        SUM(CAST(app.QuantidadeApontada AS INT))        AS Quantidade_Produzida
    FROM producao.fato_apontamento ap WITH (NOLOCK)
    INNER JOIN ULTIMA_ATIVIDADE_OP ult 
        ON ap.CodItemAtividade = ult.CodItemAtividade_Final
    INNER JOIN producao.fato_atividade_op act WITH (NOLOCK) 
        ON ap.CodItemAtividade = act.CodItemAtividade
    INNER JOIN producao.fato_apontamento_produto app WITH (NOLOCK) 
        ON ap.CodApontamentoUnico = app.CodApontamentoUnico
    WHERE ap.DataHoraApontamento IS NOT NULL
    GROUP BY
        act.OrdemProducao,
        app.CodProdutoAcabado
),

-- ============================================================
-- 3. SKU_ATRIBUTOS: Regras de Marcas Próprias da Belmicro
-- ============================================================
SKU_ATRIBUTOS AS (
    SELECT
        P.CodProduto,
        P.DescricaoProduto,
        P.Marca                                         AS Fornecedor,
        P.ModeloMkt                                     AS Modelo,
        GP.NomeGrupoPai                                 AS Familia,
        GP.LinhaDeNegocio,
        GP.NomeGrupoFamilia,
        P.UsadoComo
    FROM cadastros.dim_produtos P WITH (NOLOCK)
    INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) 
        ON GP.CodGrupoProduto = P.CodGrupoProduto
    WHERE 
        -- REGRA A: WordPC / Comprebel
        (
            P.UsadoComo IN ('Venda (fabricação própria)', 'Revenda') 
            AND GP.LinhaDeNegocio IN ('WordPC/Skill')
            AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD')
            AND GP.NomeGrupoPai <> 'COMPONENTES'
        )
),

-- ============================================================
-- 4. QUANTIDADE PLANEJADA DA TABELA FILHA (Meta do Lote por SKU)
-- ============================================================
PLANEJAMENTO_OP AS (
    SELECT 
        OrdemProducao,
        CodProdutoAcabado,
        MAX(CAST(QuantidadeAProduzir AS INT)) AS Qtd_Planejada_Item
    FROM producao.fato_ordem_producao_item WITH (NOLOCK)
    GROUP BY OrdemProducao, CodProdutoAcabado
),

-- ============================================================
-- 5. LINK_LOGISTICO: Conexão direta OrdemProducao -> Faturamento
-- ============================================================
LINK_LOGISTICO AS (
    SELECT
        OrdemProducao,
        MAX(NotaFaturamento) AS NumUnicoNota
    FROM producao.fato_instancia_item_nota WITH (NOLOCK)
    GROUP BY OrdemProducao
),

-- ============================================================
-- 6. EXPEDICAO: Data máxima de despacho do lote faturado
-- ============================================================
EXPEDICAO_MAX AS (
    SELECT
        NumUnicoNota,
        MAX(DataExpedicao) AS DataExpedicao
    FROM belmicro.fato_itens_notas_expedidas WITH (NOLOCK)
    GROUP BY NumUnicoNota
)

-- ============================================================
-- CONSULTA CONSOLIDADA FINAL (SANEADA - APENAS PRODUÇÃO MAESTRO PLUS)
-- ============================================================
SELECT
    pp.DescricaoProcesso                                        AS Processo, -- Incluído
    PROD.OrdemProducao                                          AS OP,
    LINK.NumUnicoNota,
    O.NumNota                                                   AS NumeroNota,
    
    -- Escopo Temporal Saneado do Chão de Fábrica
    PROD.Ano, 
    PROD.Mes,
    PROD.Semana, 
    PROD.Data_Producao                                          AS DataProducao,
    PROD.DataHora_Apontamento                                   AS DataHoraApontamento,

    -- Metadados Completos do Produto Comercial (SKU)
    PROD.CodProd, 
    SKU.DescricaoProduto, 
    SKU.Fornecedor, 
    SKU.Modelo,
    SKU.Familia, 
    SKU.LinhaDeNegocio, 
    SKU.NomeGrupoFamilia, 
    SKU.UsadoComo,
    
    -- Volumetria Alinhada com a realidade de SLA (Mesmo nível de análise)
    PROD.Quantidade_Produzida                                   AS Qtd_Produzida,
    COALESCE(PLAN_OP.Qtd_Planejada_Item, 0)                     AS Qtd_Planejada,
    (COALESCE(PLAN_OP.Qtd_Planejada_Item, 0) - PROD.Quantidade_Produzida) AS Saldo,
    
    -- Rastreabilidade Logística de Saída
    'PRODUZIDO'                                                 AS Situacao,
    CAST(O.DataNegociacao AS DATE)                              AS DataFaturamento,
    CAST(EXP.DataExpedicao AS DATE)                             AS DataExpedicao

FROM BASE_PRODUCAO_SLA PROD

-- Valida o escopo comercial restrito às Marcas Próprias
INNER JOIN SKU_ATRIBUTOS SKU 
    ON PROD.CodProd = SKU.CodProduto

-- Cruzamentos adicionados para trazer o Processo e amarrar a trava de escopo
INNER JOIN producao.fato_ordem_producao fop WITH (NOLOCK)
    ON PROD.OrdemProducao = fop.OrdemProducao
INNER JOIN producao.dim_processo_producao pp WITH (NOLOCK)
    ON fop.CodProcessoUnico = pp.CodProcessoUnico

-- Conecta a Meta Planejada da Engenharia para evitar descompassos
LEFT JOIN PLANEJAMENTO_OP PLAN_OP 
    ON PROD.OrdemProducao = PLAN_OP.OrdemProducao 
   AND PROD.CodProd = PLAN_OP.CodProdutoAcabado

-- Acoplamento dos Módulos Logísticos por Chave Única de Lote
LEFT JOIN LINK_LOGISTICO LINK 
    ON PROD.OrdemProducao = LINK.OrdemProducao

LEFT JOIN belmicro.fato_operacoes O WITH (NOLOCK) 
    ON LINK.NumUnicoNota = O.NumUnicoNota

LEFT JOIN EXPEDICAO_MAX EXP 
    ON LINK.NumUnicoNota = EXP.NumUnicoNota

-- Filtro temporal histórico padrão + Trava do processo que limpa o Desmonte
WHERE PROD.Data_Producao >= '2025-01-01'
  AND PROD.Data_Producao <= CAST(GETDATE() AS DATE)
  AND (pp.DescricaoProcesso LIKE '%Monitor Maestro Plus%' 
       OR pp.DescricaoProcesso LIKE '%Computadores Maestro Plus%')
  AND pp.DescricaoProcesso NOT LIKE '61 %'

ORDER BY 
    PROD.DataHora_Apontamento DESC, 
    PROD.OrdemProducao, 
    PROD.CodProd;