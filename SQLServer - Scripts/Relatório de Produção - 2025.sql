WITH
 
-- ============================================================
-- 2. BASE ATÔMICA DE PRODUÇÃO (Granularidade da SLA: 1 Linha por OP/SKU)
-- ============================================================
BASE_PRODUCAO_SLA AS (
SELECT
ap.OrdemProducao,
pa.CodProdutoAcabado AS CodProd,
pr.NumUnicoNotaPedido,
 
-- Marcos temporais baseados no nascimento real do lote (Primeiro Bipe)
YEAR(MIN(ap.DataHoraEmbalagem)) AS Ano,
MONTH(MIN(ap.DataHoraEmbalagem)) AS Mes,
DATEPART(ISO_WEEK, MIN(ap.DataHoraEmbalagem)) AS Semana,
CAST(MIN(ap.DataHoraEmbalagem) AS DATE) AS Data_Producao,
MIN(ap.DataHoraEmbalagem) AS DataHora_Apontamento,
 
-- CORREÇÃO: Conta quantos números de série foram embalados/produzidos
COUNT(CASE WHEN ap.DataHoraEmbalagem IS NOT NULL THEN ap.SerieProdutoAcabado END) AS Quantidade_Produzida
 
FROM producao.fato_ordem_producao_seriepa_ciclo ap WITH (NOLOCK)
INNER JOIN producao.fato_ordem_producao pr WITH (NOLOCK)
ON pr.OrdemProducao = ap.OrdemProducao
LEFT JOIN producao.fato_ordem_producao_item pa WITH (NOLOCK)
ON ap.OrdemProducao = pa.OrdemProducao
WHERE (ap.DataHoraEmbalagem IS NOT NULL OR ap.StatusSerie = 'E')
GROUP BY
ap.OrdemProducao,
pa.CodProdutoAcabado,
pr.NumUnicoNotaPedido
),
 
-- ============================================================
-- 3. SKU_ATRIBUTOS: Regras de Marcas Próprias da Belmicro
-- ============================================================
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
PROD.OrdemProducao AS OP,
PROD.NumUnicoNotaPedido,
O.NumNota AS NumeroNota,
 
-- Escopo Temporal Saneado do Chão de Fábrica
PROD.Ano,
PROD.Mes,
PROD.Semana,
PROD.Data_Producao AS DataProducao,
PROD.DataHora_Apontamento AS DataHoraApontamento,
 
-- Metadados Completos do Produto Comercial (SKU)
PROD.CodProd,
SKU.DescricaoProduto,
SKU.Fornecedor,
SKU.Modelo,
SKU.Familia,
SKU.LinhaDeNegocio,
SKU.NomeGrupoFamilia,
SKU.UsadoComo,
 
-- Volumetria Alinhada com a realidade de SLA
PROD.Quantidade_Produzida AS Qtd_Produzida,
COALESCE(PLAN_OP.Qtd_Planejada_Item, 0) AS Qtd_Planejada,
(COALESCE(PLAN_OP.Qtd_Planejada_Item, 0) - PROD.Quantidade_Produzida) AS Saldo,
 
-- Rastreabilidade Logística de Saída
'PRODUZIDO' AS Situacao,
CAST(O.DataNegociacao AS DATE) AS DataFaturamento,
CAST(EXP.DataExpedicao AS DATE) AS DataExpedicao
 
FROM BASE_PRODUCAO_SLA PROD
 
-- Valida o escopo comercial restrito às Marcas Próprias
INNER JOIN SKU_ATRIBUTOS SKU
ON PROD.CodProd = SKU.CodProduto
 
-- Cruzamentos adicionados para trazer o Processo e amarrar a trava de escopo
INNER JOIN producao.fato_ordem_producao fop WITH (NOLOCK)
ON PROD.OrdemProducao = fop.OrdemProducao
 
-- Conecta a Meta Planejada da Engenharia para evitar descompassos
LEFT JOIN PLANEJAMENTO_OP PLAN_OP
ON PROD.OrdemProducao = PLAN_OP.OrdemProducao
AND PROD.CodProd = PLAN_OP.CodProdutoAcabado
 
LEFT JOIN belmicro.fato_operacoes O WITH (NOLOCK)
ON PROD.NumUnicoNotaPedido = O.NumUnicoNota
 
LEFT JOIN EXPEDICAO_MAX EXP
ON PROD.NumUnicoNotaPedido = EXP.NumUnicoNota
 
-- Filtro temporal histórico padrão
WHERE PROD.Data_Producao >= '2025-01-01'
AND PROD.Data_Producao <= CAST(GETDATE() AS DATE)
 
ORDER BY
PROD.DataHora_Apontamento DESC,
PROD.OrdemProducao,
PROD.CodProd;