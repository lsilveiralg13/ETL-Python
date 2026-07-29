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
-- 2. BASE ATÔMICA DE PRODUÇÃO (Seu gabarito oficial e travado)
-- ============================================================
BASE_PRODUCAO_SLA AS (
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
-- 3. SKU_ATRIBUTOS: Suas Regras Oficiais de Marcas Próprias
-- ============================================================
SKU_ATRIBUTOS AS (
    SELECT
        P.CodProduto,
        P.DescricaoProduto,
        P.Marca AS Fornecedor,
        GP.NomeGrupoPai AS Familia,
        GP.LinhaDeNegocio,
        P.UsadoComo
    FROM cadastros.dim_produtos P WITH (NOLOCK)
    INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) 
        ON GP.CodGrupoProduto = P.CodGrupoProduto
    WHERE 
        P.UsadoComo IN ('Venda (fabricação própria)', 'Revenda') 
        AND GP.LinhaDeNegocio IN ('WordPC/Skill')
        AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD')
        AND GP.NomeGrupoPai <> 'COMPONENTES'
),

-- ============================================================
-- 4. QUANTIDADE PLANEJADA DA ENGENHARIA
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
-- 5. MATRIZ DE BIPAGENS POR ETAPA (Visão Horizontal de Postos)
-- ============================================================
BIPAGENS_PIVOT AS (
    SELECT 
        fao.OrdemProducao,
        fap.CodProdutoAcabado,
        SUM(CASE WHEN pt.DescricaoPosto LIKE '%1%' OR pt.DescricaoPosto LIKE '%Prepara%' THEN CAST(fap.QuantidadeApontada AS INT) ELSE 0 END) AS Bips_Separacao,
        SUM(CASE WHEN pt.DescricaoPosto LIKE '%2%' OR pt.DescricaoPosto LIKE '%Conferen%' THEN CAST(fap.QuantidadeApontada AS INT) ELSE 0 END) AS Bips_Montagem,
        SUM(CASE WHEN pt.DescricaoPosto LIKE '%3%' OR pt.DescricaoPosto LIKE '%Qualidade%' THEN CAST(fap.QuantidadeApontada AS INT) ELSE 0 END) AS Bips_Qualidade,
        SUM(CASE WHEN pt.DescricaoPosto LIKE '%4%' OR pt.DescricaoPosto LIKE '%Runin%' OR pt.DescricaoPosto LIKE '%Run-in%' THEN CAST(fap.QuantidadeApontada AS INT) ELSE 0 END) AS Bips_Runin,
        SUM(CASE WHEN pt.DescricaoPosto LIKE '%5%' OR pt.DescricaoPosto LIKE '%Embala%' THEN CAST(fap.QuantidadeApontada AS INT) ELSE 0 END) AS Bips_Embalagem
    FROM producao.fato_atividade_op fao WITH (NOLOCK)
    INNER JOIN producao.dim_posto_trabalho pt WITH (NOLOCK) ON fao.CodCentroTrabalho = pt.CodCentroTrabalho
    INNER JOIN producao.fato_apontamento fa WITH (NOLOCK) ON fao.CodItemAtividade = fa.CodItemAtividade
    INNER JOIN producao.fato_apontamento_produto fap WITH (NOLOCK) ON fa.CodApontamentoUnico = fap.CodApontamentoUnico
    WHERE fap.QuantidadeApontada > 0
    GROUP BY fao.OrdemProducao, fap.CodProdutoAcabado
)

-- ============================================================
-- 6. CONSULTA MATRICIAL CONSOLIDADA COM MARCOS DE SLA
-- ============================================================
SELECT
    pp.DescricaoProcesso AS Processo,
    PROD.OrdemProducao AS Numero_OP,
    PROD.CodProd AS CodProduto,
    SKU.DescricaoProduto,
    SKU.Familia,
    SKU.LinhaDeNegocio,
    SKU.UsadoComo,
    
    -- Marcos Temporais e Simulação de SLA (Incluídos)
    fop.DataHoraInclusao AS Data_Inclusao_OP,
    DATEADD(DAY, 2, fop.DataHoraInclusao) AS Prazo_SLA_Simulado,
    PROD.DataHora_Apontamento AS DataHora_Apontamento_Final,
    
    -- Volumetria Principal (100% amarrada com seu BI antigo)
    PROD.Quantidade_Produzida AS Qtd_Produzida,
    COALESCE(PLAN_OP.Qtd_Planejada_Item, 0) AS Qtd_Planejada,
    (COALESCE(PLAN_OP.Qtd_Planejada_Item, 0) - PROD.Quantidade_Produzida) AS Saldo,
    
    -- Colunas Transpostas de Fluxo por Etapa
    COALESCE(BIP.Bips_Separacao, 0) AS Separacao,
    COALESCE(BIP.Bips_Montagem, 0) AS Montagem,
    COALESCE(BIP.Bips_Qualidade, 0) AS Qualidade,
    COALESCE(BIP.Bips_Runin, 0) AS Runin,
    COALESCE(BIP.Bips_Embalagem, 0) AS Embalagem

FROM BASE_PRODUCAO_SLA PROD
INNER JOIN SKU_ATRIBUTOS SKU 
    ON PROD.CodProd = SKU.CodProduto
INNER JOIN producao.fato_ordem_producao fop WITH (NOLOCK)
    ON PROD.OrdemProducao = fop.OrdemProducao
INNER JOIN producao.dim_processo_producao pp WITH (NOLOCK)
    ON fop.CodProcessoUnico = pp.CodProcessoUnico
LEFT JOIN PLANEJAMENTO_OP PLAN_OP 
    ON PROD.OrdemProducao = PLAN_OP.OrdemProducao 
   AND PROD.CodProd = PLAN_OP.CodProdutoAcabado
LEFT JOIN BIPAGENS_PIVOT BIP
    ON PROD.OrdemProducao = BIP.OrdemProducao 
   AND PROD.CodProd = BIP.CodProdutoAcabado

WHERE PROD.Data_Producao >= '2025-01-01'
  AND PROD.Data_Producao <= CAST(GETDATE() AS DATE)
  AND (pp.DescricaoProcesso LIKE '%Monitor Maestro Plus%' 
       OR pp.DescricaoProcesso LIKE '%Computadores Maestro Plus%')
  AND pp.DescricaoProcesso NOT LIKE '61 %'

ORDER BY 
    PROD.DataHora_Apontamento DESC, 
    PROD.OrdemProducao, 
    PROD.CodProd;
