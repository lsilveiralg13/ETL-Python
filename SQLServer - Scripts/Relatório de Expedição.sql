/* RELATÓRIO DE EXPEDIÇÃO CONSOLIDADO - V5.4 (ESTRUTURA MANTIDA + COLUNAS TOP)
   Vinculando a fato_operacoes para filtrar as TOPs de expedição real.
   Adicionadas colunas CodTipoOperacao e DescricaoTipoOperacao.
*/

-- ==========================================
-- 1. CONFIGURAÇÃO DE PARÂMETROS
-- ==========================================
DECLARE @AnoConsulta  INT = 2026;         
DECLARE @MesConsulta  INT = 4;            
DECLARE @TipoConsulta VARCHAR(10) = 'ANO'; 

-- ==========================================
-- 2. CONSULTA CONSOLIDADA
-- ==========================================

-- PARTE 1: REGRA A
SELECT
    EXP.NumUnicoNota, 
    O.CodTipoOperacao, O.DescricaoTipoOperacao, -- Colunas adicionadas
    YEAR(EXP.DataExpedicao) AS Ano, MONTH(EXP.DataExpedicao) AS Mes,
    DATEPART(ISO_WEEK, EXP.DataExpedicao) AS Semana, CAST(EXP.DataExpedicao AS DATE) AS DataExpedicao,
    I.CodProduto AS CodProd, P.DescricaoProduto, P.Marca AS Fornecedor, P.ModeloMkt AS Modelo,
    GP.NomeGrupoPai AS Familia, 'EXPEDIDO' AS Situacao, I.CodLocal AS CodLocalEstoque,
    GP.LinhaDeNegocio, GP.NomeGrupoFamilia, P.UsadoComo,
    SUM(CAST(I.ValorTotal AS DECIMAL(18,2))) AS ValorTotal, SUM(CAST(I.QtdNegociada AS INT)) AS Quantidade
FROM belmicro.fato_itens I WITH (NOLOCK)
INNER JOIN belmicro.fato_itens_notas_expedidas EXP WITH (NOLOCK) ON EXP.NumUnicoNota = I.NumUnicoNota
INNER JOIN cadastros.dim_produtos P WITH (NOLOCK) ON P.CodProduto = I.CodProduto
INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) ON GP.CodGrupoProduto = P.CodGrupoProduto
-- BUSCANDO A TOP NA TABELA QUE VOCÊ ME MOSTROU ANTES
INNER JOIN belmicro.fato_operacoes O WITH (NOLOCK) ON O.NumUnicoNota = EXP.NumUnicoNota

WHERE EXP.DataExpedicao IS NOT NULL
  AND P.UsadoComo IN ('Venda (fabricação própria)', 'Revenda') 
  AND GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel')
  AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD')
  AND GP.NomeGrupoPai <> 'COMPONENTES'
  
  -- --- FILTROS DE EXPURGO LOGÍSTICO ---
  AND O.CodTipoOperacao BETWEEN 3100 AND 3396 
  AND O.CodTipoOperacao NOT IN (3146, 3155, 3248, 3338, 3397, 3249, 3356, 3311, 3315, 3292, 3341, 3376, 3375, 3205, 3354, 3328, 3266) -- Canais Full
  AND O.CodTipoOperacao NOT IN (3000, 3005, 3017, 3029)       -- Devoluções
  AND O.CodTipoOperacao NOT IN (3207, 3210, 3230, 3245)       -- Transferências
  AND O.DescricaoTipoOperacao NOT LIKE '%FULL%'
  
  AND ((@TipoConsulta = 'MES' AND YEAR(EXP.DataExpedicao) = @AnoConsulta AND MONTH(EXP.DataExpedicao) = @MesConsulta) OR (@TipoConsulta = 'ANO' AND YEAR(EXP.DataExpedicao) IN (2025, 2026)))
GROUP BY EXP.NumUnicoNota, O.CodTipoOperacao, O.DescricaoTipoOperacao, YEAR(EXP.DataExpedicao), MONTH(EXP.DataExpedicao), DATEPART(ISO_WEEK, EXP.DataExpedicao), CAST(EXP.DataExpedicao AS DATE), I.CodProduto, P.DescricaoProduto, P.Marca, P.ModeloMkt, GP.NomeGrupoPai, I.CodLocal, GP.LinhaDeNegocio, GP.NomeGrupoFamilia, P.UsadoComo

UNION ALL

-- PARTE 2: REGRA B
SELECT
    EXP.NumUnicoNota, 
    O.CodTipoOperacao, O.DescricaoTipoOperacao, -- Colunas adicionadas
    YEAR(EXP.DataExpedicao) AS Ano, MONTH(EXP.DataExpedicao) AS Mes,
    DATEPART(ISO_WEEK, EXP.DataExpedicao) AS Semana, CAST(EXP.DataExpedicao AS DATE) AS DataExpedicao,
    I.CodProduto AS CodProd, P.DescricaoProduto, P.Marca AS Fornecedor, P.ModeloMkt AS Modelo,
    GP.NomeGrupoPai AS Familia, 'EXPEDIDO' AS Situacao, I.CodLocal AS CodLocalEstoque,
    GP.LinhaDeNegocio, GP.NomeGrupoFamilia, P.UsadoComo,
    SUM(CAST(I.ValorTotal AS DECIMAL(18,2))) AS ValorTotal, SUM(CAST(I.QtdNegociada AS INT)) AS Quantidade
FROM belmicro.fato_itens I WITH (NOLOCK)
INNER JOIN belmicro.fato_itens_notas_expedidas EXP WITH (NOLOCK) ON EXP.NumUnicoNota = I.NumUnicoNota
INNER JOIN cadastros.dim_produtos P WITH (NOLOCK) ON P.CodProduto = I.CodProduto
INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) ON GP.CodGrupoProduto = P.CodGrupoProduto
INNER JOIN belmicro.fato_operacoes O WITH (NOLOCK) ON O.NumUnicoNota = EXP.NumUnicoNota

WHERE EXP.DataExpedicao IS NOT NULL
  AND P.UsadoComo IN ('Revenda', 'Venda (fabricação própria)')
  AND P.Marca IN ('HQ', 'KONKA', '3GREEN')
  AND GP.NomeGrupoPai IN ('AR CONDICIONADO', 'FRIGOBAR', 'FORNO', 'NOTEBOOK', 'FRITADEIRA', 'REFRIGERADOR', 'GRILL E SANDUICHEIRAS', 'FREEZER', 'ADEGA', 'COOKTOPS', 'LAVADOURA LOUCAS', 'CERVEJEIRA', 'MAQUINA DE GELO', 'PANELA ELETRICA', 'MONITORES', 'TV', 'MONITOR')
  AND GP.NomeGrupoPai <> 'COMPONENTES'
  
  -- --- FILTROS DE EXPURGO LOGÍSTICO ---
  AND O.CodTipoOperacao BETWEEN 3100 AND 3396 
  AND O.CodTipoOperacao NOT IN (3146, 3155, 3248, 3338, 3397, 3249, 3356, 3311, 3315, 3292, 3341, 3376, 3375, 3205, 3354, 3328, 3266)
  AND O.CodTipoOperacao NOT IN (3000, 3005, 3017, 3029)
  AND O.CodTipoOperacao NOT IN (3207, 3210, 3230, 3245)
  AND O.DescricaoTipoOperacao NOT LIKE '%FULL%'
  
  AND NOT (GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel') AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD'))
  AND ((@TipoConsulta = 'MES' AND YEAR(EXP.DataExpedicao) = @AnoConsulta AND MONTH(EXP.DataExpedicao) = @MesConsulta) OR (@TipoConsulta = 'ANO' AND YEAR(EXP.DataExpedicao) IN (2025, 2026)))
GROUP BY EXP.NumUnicoNota, O.CodTipoOperacao, O.DescricaoTipoOperacao, YEAR(EXP.DataExpedicao), MONTH(EXP.DataExpedicao), DATEPART(ISO_WEEK, EXP.DataExpedicao), CAST(EXP.DataExpedicao AS DATE), I.CodProduto, P.DescricaoProduto, P.Marca, P.ModeloMkt, GP.NomeGrupoPai, I.CodLocal, GP.LinhaDeNegocio, GP.NomeGrupoFamilia, P.UsadoComo

ORDER BY DataExpedicao DESC;
