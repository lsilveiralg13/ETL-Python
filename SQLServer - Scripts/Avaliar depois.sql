WITH 

-- =====================================================================================
-- PRODUTOS (Compartilhado pelas duas pontas)
-- =====================================================================================
SKU_ATRIBUTOS AS (
    SELECT
        P.CodProduto,
        P.DescricaoProduto,
        P.Marca AS Fornecedor,
        P.ModeloMkt AS Modelo,
        GP.NomeGrupoPai AS Familia,
        GP.LinhaDeNegocio,
        GP.NomeGrupoFamilia,
        P.UsadoComo
    FROM cadastros.dim_produtos P WITH (NOLOCK)
    INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) 
        ON GP.CodGrupoProduto = P.CodGrupoProduto
    WHERE P.UsadoComo IN ('Venda (fabricação própria)', 'Revenda') 
      AND GP.LinhaDeNegocio IN ('WordPC/Skill')
      AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD')
      AND GP.NomeGrupoPai <> 'COMPONENTES'
),

-- =====================================================================================
-- PLANEJAMENTO (Compartilhado pelas duas pontas)
-- =====================================================================================
PLANEJAMENTO_OP AS (
    SELECT 
        OrdemProducao,
        CodProdutoAcabado,
        MAX(CAST(QuantidadeAProduzir AS INT)) AS Qtd_Planejada_Item
    FROM producao.fato_ordem_producao_item WITH (NOLOCK)
    GROUP BY OrdemProducao, CodProdutoAcabado
),

-- =====================================================================================
-- METADADOS DA OP (Com inteligência de contingência de datas para 2025)
-- =====================================================================================
DADOS_OP AS (
    SELECT 
        op.OrdemProducao,
        op.StatusOrdemProducao,
        op.CodProcessoUnico,
        op.NumUnicoNotaPedido, 
        op.DataHoraInclusao AS DataInclusao_OP,
        op.DataHoraInicializacao AS DataInicio_OP, -- 💡 CORRIGIDO AQUI: Retornado para o seu padrão real
        op.DataHoraFinalizacao AS DataFim_OP,
        DATEADD(DAY, 2, COALESCE(op.DataHoraInicializacao, op.DataHoraInclusao)) AS Prazo_SLA_Calculado
    FROM producao.fato_ordem_producao op WITH (NOLOCK)
    WHERE op.DataHoraInclusao >= '2025-01-01'
),

-- =====================================================================================
-- EXPEDIÇÃO LOGÍSTICA
-- =====================================================================================
EXPEDICAO_MAX AS (
    SELECT
        NumUnicoNota,
        MAX(DataExpedicao) AS DataExpedicao
    FROM belmicro.fato_itens_notas_expedidas WITH (NOLOCK)
    GROUP BY NumUnicoNota
),

-- =====================================================================================
-- LINKS LOGÍSTICOS (Exclusivo da base Legada)
-- =====================================================================================
LINK_LOGISTICO_LEGADO AS (
    SELECT 
        OrdemProducao,
        MAX(NotaFaturamento) AS NumUnicoNota
    FROM producao.fato_instancia_item_nota WITH (NOLOCK)
    GROUP BY OrdemProducao
),

-- =====================================================================================
-- 1. FONTE A: BASE LEGADA (APONTAMENTOS)
-- =====================================================================================
ULTIMA_ATIVIDADE_OP AS (
    SELECT 
        OrdemProducao,
        MAX(CodItemAtividade) AS CodItemAtividade_Final
    FROM producao.fato_atividade_op WITH (NOLOCK)
    GROUP BY OrdemProducao
),

BASE_PRODUCAO_LEGADA AS (
    SELECT
        act.OrdemProducao,
        app.CodProdutoAcabado AS CodProd,
        YEAR(MIN(ap.DataHoraApontamento)) AS Ano, 
        MONTH(MIN(ap.DataHoraApontamento)) AS Mes,
        DATEPART(ISO_WEEK, MIN(ap.DataHoraApontamento)) AS Semana, 
        CAST(MIN(ap.DataHoraApontamento) AS DATE) AS Data_Producao_Real,
        MIN(ap.DataHoraApontamento) AS DataHora_Apontamento_Real,
        SUM(CAST(app.QuantidadeApontada AS INT)) AS Quantidade_Produzida
    FROM producao.fato_apontamento ap WITH (NOLOCK)
    INNER JOIN ULTIMA_ATIVIDADE_OP ult ON ap.CodItemAtividade = ult.CodItemAtividade_Final
    INNER JOIN producao.fato_atividade_op act WITH (NOLOCK) ON ap.CodItemAtividade = act.CodItemAtividade
    INNER JOIN producao.fato_apontamento_produto app WITH (NOLOCK) ON ap.CodApontamentoUnico = app.CodApontamentoUnico
    WHERE ap.DataHoraApontamento >= '2025-01-01' 
      AND ap.DataHoraApontamento <= GETDATE()
    GROUP BY act.OrdemProducao, app.CodProdutoAcabado
),

-- =====================================================================================
-- 2. FONTE B: BASE SERIAL (MAESTRO PLUS)
-- =====================================================================================
BASE_PRODUCAO_SERIAL AS (
    SELECT
        ap.OrdemProducao,
        pa.CodProdutoAcabado AS CodProd,
        YEAR(MIN(ap.DataHoraEmbalagem)) AS Ano,
        MONTH(MIN(ap.DataHoraEmbalagem)) AS Mes,
        DATEPART(ISO_WEEK, MIN(ap.DataHoraEmbalagem)) AS Semana,
        CAST(MIN(ap.DataHoraEmbalagem) AS DATE) AS Data_Producao_Real,
        MIN(ap.DataHoraEmbalagem) AS DataHora_Apontamento_Real,
        COUNT(CASE WHEN ap.DataHoraEmbalagem IS NOT NULL THEN ap.SerieProdutoAcabado END) AS Quantidade_Produzida
    FROM producao.fato_ordem_producao_seriepa_ciclo ap WITH (NOLOCK)
    INNER JOIN producao.fato_ordem_producao_item pa WITH (NOLOCK)
        ON ap.OrdemProducao = pa.OrdemProducao
    WHERE ap.DataHoraEmbalagem >= '2025-01-01' 
      AND ap.DataHoraEmbalagem <= GETDATE()
    GROUP BY ap.OrdemProducao, pa.CodProdutoAcabado
)

-- =====================================================================================
-- CONSOLIDAÇÃO FINAL COM EQUALIZAÇÃO DE GRANULARIDADE POR PEÇA (1:1 EQUIVALENTE)
-- =====================================================================================
SELECT 
    Origem_Base,
    OP, 
    CodProduto,
    Descricao,
    Familia,
    Linha,
    Ano,
    Mes,
    Semana,
    
    -- 📊 Volumes Físicos Alinhados
    Qtd_Produzida AS TOTAL_PRODUZIDO,
    Qtd_Planejada,
    Saldo,
    
    -- Metadados de Tempo
    Data_Inclusao,
    Data_Inicio,
    Limite_SLA, 
    Data_Producao, 
    DataHora_Apontamento,
    LT_Minutos,
    LT_Dias,
    Status_SLA,

    -- 💡 Separação física do veredito pelas peças reais produzidas
    CASE WHEN Status_SLA = 'DENTRO DO SLA' THEN Qtd_Produzida ELSE 0 END AS PEÇAS_SLA_OK,
    CASE WHEN Status_SLA = 'FORA DO SLA' THEN Qtd_Produzida ELSE 0 END AS PEÇAS_SLA_FORA,

    -- Dados Logísticos
    NumUnicoNota,
    NumeroNota,
    DataFaturamento,
    DataExpedicao
FROM (
    
    -- ========================================================
    -- CORPO DA BASE LEGADA
    -- ========================================================
    SELECT
        'LEGADA' AS Origem_Base,
        PROD.OrdemProducao AS OP, 
        PROD.CodProd AS CodProduto,
        SKU.DescricaoProduto AS Descricao,
        SKU.Familia,
        SKU.LinhaDeNegocio AS Linha,
        PROD.Ano,
        PROD.Mes,
        PROD.Semana,
        PROD.Quantidade_Produzida AS Qtd_Produzida,
        COALESCE(PLAN_OP.Qtd_Planejada_Item, 0) AS Qtd_Planejada,
        (COALESCE(PLAN_OP.Qtd_Planejada_Item, 0) - PROD.Quantidade_Produzida) AS Saldo,
        
        OP.DataInclusao_OP AS Data_Inclusao,
        OP.DataInicio_OP AS Data_Inicio,
        OP.Prazo_SLA_Calculado AS Limite_SLA, 
        PROD.Data_Producao_Real AS Data_Producao, 
        PROD.DataHora_Apontamento_Real AS DataHora_Apontamento,
        
        RIGHT('0' + CAST(COALESCE(DATEDIFF(SECOND, OP.DataInicio_OP, PROD.DataHora_Apontamento_Real), 0) / 3600 AS VARCHAR), 2) + ':' +
        RIGHT('0' + CAST((COALESCE(DATEDIFF(SECOND, OP.DataInicio_OP, PROD.DataHora_Apontamento_Real), 0) % 3600) / 60 AS VARCHAR), 2) + ':' +
        RIGHT('0' + CAST(COALESCE(DATEDIFF(SECOND, OP.DataInicio_OP, PROD.DataHora_Apontamento_Real), 0) % 60 AS VARCHAR), 2) AS LT_Minutos,
        COALESCE(DATEDIFF(DAY, CAST(OP.DataInicio_OP AS DATE), PROD.Data_Producao_Real), 0) AS LT_Dias,
        
        CASE 
            WHEN OP.DataInicio_OP IS NULL THEN 'OP NÃO INICIALIZADA'
            WHEN PROD.DataHora_Apontamento_Real <= OP.Prazo_SLA_Calculado THEN 'DENTRO DO SLA'
            ELSE 'FORA DO SLA'
        END AS Status_SLA,

        LINK.NumUnicoNota,
        O.NumNota AS NumeroNota,
        CAST(O.DataNegociacao AS DATE) AS DataFaturamento,
        CAST(EXP.DataExpedicao AS DATE) AS DataExpedicao

    FROM BASE_PRODUCAO_LEGADA PROD
    INNER JOIN SKU_ATRIBUTOS SKU ON PROD.CodProd = SKU.CodProduto 
    LEFT JOIN DADOS_OP OP ON PROD.OrdemProducao = OP.OrdemProducao 
    LEFT JOIN PLANEJAMENTO_OP PLAN_OP ON PROD.OrdemProducao = PLAN_OP.OrdemProducao AND PROD.CodProd = PLAN_OP.CodProdutoAcabado
    LEFT JOIN LINK_LOGISTICO_LEGADO LINK ON PROD.OrdemProducao = LINK.OrdemProducao
    LEFT JOIN belmicro.fato_operacoes O WITH (NOLOCK) ON LINK.NumUnicoNota = O.NumUnicoNota
    LEFT JOIN EXPEDICAO_MAX EXP WITH (NOLOCK) ON LINK.NumUnicoNota = EXP.NumUnicoNota

    UNION ALL

    -- ========================================================
    -- CORPO DA BASE SERIAL (Maestro Plus)
    -- ========================================================
    SELECT
        'SERIAL' AS Origem_Base,
        PROD.OrdemProducao AS OP, 
        PROD.CodProd AS CodProduto,
        SKU.DescricaoProduto AS Descricao,
        SKU.Familia,
        SKU.LinhaDeNegocio AS Linha,
        PROD.Ano,
        PROD.Mes,
        PROD.Semana,
        PROD.Quantidade_Produzida AS Qtd_Produzida,
        COALESCE(PLAN_OP.Qtd_Planejada_Item, 0) AS Qtd_Planejada,
        (COALESCE(PLAN_OP.Qtd_Planejada_Item, 0) - PROD.Quantidade_Produzida) AS Saldo,
        
        OP.DataInclusao_OP AS Data_Inclusao,
        OP.DataInicio_OP AS Data_Inicio,
        OP.Prazo_SLA_Calculado AS Limite_SLA, 
        PROD.Data_Producao_Real AS Data_Producao, 
        PROD.DataHora_Apontamento_Real AS DataHora_Apontamento,
        
        RIGHT('0' + CAST(COALESCE(DATEDIFF(SECOND, OP.DataInicio_OP, PROD.DataHora_Apontamento_Real), 0) / 3600 AS VARCHAR), 2) + ':' +
        RIGHT('0' + CAST((COALESCE(DATEDIFF(SECOND, OP.DataInicio_OP, PROD.DataHora_Apontamento_Real), 0) % 3600) / 60 AS VARCHAR), 2) + ':' +
        RIGHT('0' + CAST(COALESCE(DATEDIFF(SECOND, OP.DataInicio_OP, PROD.DataHora_Apontamento_Real), 0) % 60 AS VARCHAR), 2) AS LT_Minutos,
        COALESCE(DATEDIFF(DAY, CAST(OP.DataInicio_OP AS DATE), PROD.Data_Producao_Real), 0) AS LT_Dias,
        
        CASE 
            WHEN OP.DataInicio_OP IS NULL THEN 'OP NÃO INICIALIZADA'
            WHEN PROD.DataHora_Apontamento_Real <= OP.Prazo_SLA_Calculado THEN 'DENTRO DO SLA'
            ELSE 'FORA DO SLA'
        END AS Status_SLA,

        OP.NumUnicoNotaPedido AS NumUnicoNota,
        O.NumNota AS NumeroNota,
        CAST(O.DataNegociacao AS DATE) AS DataFaturamento,
        CAST(EXP.DataExpedicao AS DATE) AS DataExpedicao

    FROM BASE_PRODUCAO_SERIAL PROD
    INNER JOIN SKU_ATRIBUTOS SKU ON PROD.CodProd = SKU.CodProduto 
    LEFT JOIN DADOS_OP OP ON PROD.OrdemProducao = OP.OrdemProducao 
    LEFT JOIN PLANEJAMENTO_OP PLAN_OP ON PROD.OrdemProducao = PLAN_OP.OrdemProducao AND PROD.CodProd = PLAN_OP.CodProdutoAcabado
    LEFT JOIN belmicro.fato_operacoes O WITH (NOLOCK) ON OP.NumUnicoNotaPedido = O.NumUnicoNota
    LEFT JOIN EXPEDICAO_MAX EXP WITH (NOLOCK) ON OP.NumUnicoNotaPedido = EXP.NumUnicoNota

) X;