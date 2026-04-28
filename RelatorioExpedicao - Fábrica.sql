/* RELATÓRIO DE EXPEDIÇÃO CONSOLIDADO - V4.1 (AJUSTE ANTIDUPLICIDADE)
   Adicionada trava na Parte 2 para não processar itens já cobertos pela Parte 1.
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

-- PARTE 1: REGRA A (FABRICAÇÃO PRÓPRIA / LINHAS PRINCIPAIS)
SELECT
    EXP.NumUnicoNota, YEAR(EXP.DataExpedicao) AS Ano, MONTH(EXP.DataExpedicao) AS Mes,
    DATEPART(ISO_WEEK, EXP.DataExpedicao) AS Semana, CAST(EXP.DataExpedicao AS DATE) AS DataExpedicao,
    I.CodProduto AS CodProd, P.DescricaoProduto, P.Marca AS Fornecedor, P.ModeloMkt AS Modelo,
    GP.NomeGrupoPai AS Familia, 'EXPEDIDO' AS Situacao, I.CodLocal AS CodLocalEstoque,
    GP.LinhaDeNegocio, GP.NomeGrupoFamilia, P.UsadoComo,
    SUM(CAST(I.ValorTotal AS DECIMAL(18,2))) AS ValorTotal, SUM(CAST(I.QtdNegociada AS INT)) AS Quantidade
FROM belmicro.fato_itens I WITH (NOLOCK)
INNER JOIN belmicro.fato_itens_notas_expedidas EXP WITH (NOLOCK) ON EXP.NumUnicoNota = I.NumUnicoNota
INNER JOIN cadastros.dim_produtos P WITH (NOLOCK) ON P.CodProduto = I.CodProduto
INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) ON GP.CodGrupoProduto = P.CodGrupoProduto
WHERE EXP.DataExpedicao IS NOT NULL
  AND P.UsadoComo IN ('Venda (fabricação própria)', 'Revenda') 
  AND GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel')
  AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD')
  AND GP.NomeGrupoPai <> 'COMPONENTES'
  AND ((@TipoConsulta = 'MES' AND YEAR(EXP.DataExpedicao) = @AnoConsulta AND MONTH(EXP.DataExpedicao) = @MesConsulta) OR (@TipoConsulta = 'ANO' AND YEAR(EXP.DataExpedicao) IN (2025, 2026)))
GROUP BY EXP.NumUnicoNota, YEAR(EXP.DataExpedicao), MONTH(EXP.DataExpedicao), DATEPART(ISO_WEEK, EXP.DataExpedicao), CAST(EXP.DataExpedicao AS DATE), I.CodProduto, P.DescricaoProduto, P.Marca, P.ModeloMkt, GP.NomeGrupoPai, I.CodLocal, GP.LinhaDeNegocio, GP.NomeGrupoFamilia, P.UsadoComo

UNION ALL

-- PARTE 2: REGRA B (REVENDA HQ + LINHA BRANCA / MONITORES)
SELECT
    EXP.NumUnicoNota, YEAR(EXP.DataExpedicao) AS Ano, MONTH(EXP.DataExpedicao) AS Mes,
    DATEPART(ISO_WEEK, EXP.DataExpedicao) AS Semana, CAST(EXP.DataExpedicao AS DATE) AS DataExpedicao,
    I.CodProduto AS CodProd, P.DescricaoProduto, P.Marca AS Fornecedor, P.ModeloMkt AS Modelo,
    GP.NomeGrupoPai AS Familia, 'EXPEDIDO' AS Situacao, I.CodLocal AS CodLocalEstoque,
    GP.LinhaDeNegocio, GP.NomeGrupoFamilia, P.UsadoComo,
    SUM(CAST(I.ValorTotal AS DECIMAL(18,2))) AS ValorTotal, SUM(CAST(I.QtdNegociada AS INT)) AS Quantidade
FROM belmicro.fato_itens I WITH (NOLOCK)
INNER JOIN belmicro.fato_itens_notas_expedidas EXP WITH (NOLOCK) ON EXP.NumUnicoNota = I.NumUnicoNota
INNER JOIN cadastros.dim_produtos P WITH (NOLOCK) ON P.CodProduto = I.CodProduto
INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) ON GP.CodGrupoProduto = P.CodGrupoProduto
WHERE EXP.DataExpedicao IS NOT NULL
  AND P.UsadoComo IN ('Revenda', 'Venda (fabricação própria)')
  AND P.Marca IN ('HQ', 'KONKA', '3GREEN')
  AND GP.NomeGrupoPai IN ('AR CONDICIONADO', 'FRIGOBAR', 'FORNO', 'NOTEBOOK', 'FRITADEIRA', 'REFRIGERADOR', 'GRILL E SANDUICHEIRAS', 'FREEZER', 'ADEGA', 'COOKTOPS', 'LAVADOURA LOUCAS', 'CERVEJEIRA', 'MAQUINA DE GELO', 'PANELA ELETRICA', 'MONITORES', 'TV', 'MONITOR')
  AND GP.NomeGrupoPai <> 'COMPONENTES'
  
  -- >>> CORREÇÃO AQUI: Exclui o que a Regra A já pegou para evitar duplicidade <<<
  AND NOT (GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel') AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD'))
  
  AND ((@TipoConsulta = 'MES' AND YEAR(EXP.DataExpedicao) = @AnoConsulta AND MONTH(EXP.DataExpedicao) = @MesConsulta) OR (@TipoConsulta = 'ANO' AND YEAR(EXP.DataExpedicao) IN (2025, 2026)))
GROUP BY EXP.NumUnicoNota, YEAR(EXP.DataExpedicao), MONTH(EXP.DataExpedicao), DATEPART(ISO_WEEK, EXP.DataExpedicao), CAST(EXP.DataExpedicao AS DATE), I.CodProduto, P.DescricaoProduto, P.Marca, P.ModeloMkt, GP.NomeGrupoPai, I.CodLocal, GP.LinhaDeNegocio, GP.NomeGrupoFamilia, P.UsadoComo

ORDER BY DataExpedicao DESC;