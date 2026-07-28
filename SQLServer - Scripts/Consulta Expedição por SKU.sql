/* RELATÓRIO DE EXPEDIÇÃO CONSOLIDADO PIVOTADO - V6.1
   Suporte a buscas por múltiplos produtos simultâneos (Lista de SKUs).
*/

-- ==========================================
-- 1. CONFIGURAÇÃO DE FILTRO MULTIPRODUTO
-- ==========================================
-- Crie uma tabela temporária para listar os códigos de produto desejados.
-- Se quiser trazer TODOS os produtos, basta deixar a tabela vazia (sem dar INSERT nela).
IF OBJECT_ID('tempdb..#FiltroProdutos') IS NOT NULL DROP TABLE #FiltroProdutos;
CREATE TABLE #FiltroProdutos (CodProduto INT);

-- INSIRA AQUI OS PRODUTOS QUE DESEJA PESQUISAR (Adicione quantas linhas quiser):
INSERT INTO #FiltroProdutos VALUES (1614);
INSERT INTO #FiltroProdutos VALUES (1243);
INSERT INTO #FiltroProdutos VALUES (1291);
INSERT INTO #FiltroProdutos VALUES (2);
INSERT INTO #FiltroProdutos VALUES (758);
INSERT INTO #FiltroProdutos VALUES (779);
INSERT INTO #FiltroProdutos VALUES (1610);

-- ==========================================
-- 2. CTE DE CONSOLIDAÇÃO DOS DADOS BRUTOS
-- ==========================================
;WITH CTE_Expedicao_Base AS (

    -- PARTE 1: REGRA A
    SELECT
        YEAR(EXP.DataExpedicao) AS Ano,
        I.CodProduto AS CodProd, 
        P.DescricaoProduto, 
        SUM(CAST(I.QtdNegociada AS INT)) AS Quantidade
    FROM belmicro.fato_itens I WITH (NOLOCK)
    INNER JOIN belmicro.fato_itens_notas_expedidas EXP WITH (NOLOCK) ON EXP.NumUnicoNota = I.NumUnicoNota
    INNER JOIN cadastros.dim_produtos P WITH (NOLOCK) ON P.CodProduto = I.CodProduto
    INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) ON GP.CodGrupoProduto = P.CodGrupoProduto
    INNER JOIN belmicro.fato_operacoes O WITH (NOLOCK) ON O.NumUnicoNota = EXP.NumUnicoNota
    WHERE EXP.DataExpedicao IS NOT NULL
      AND YEAR(EXP.DataExpedicao) IN (2024, 2025, 2026)
      AND P.UsadoComo IN ('Venda (fabricação própria)', 'Revenda') 
      AND GP.LinhaDeNegocio IN ('WordPC/Skill', 'Comprebel')
      AND P.Marca IN ('HQ', '3GREEN', 'EASYPC', 'SKILL', 'QUANTUM', 'CORPC', 'FOXPC', 'AMD')
      AND GP.NomeGrupoPai <> 'COMPONENTES'
      -- --- FILTROS DE EXPURGO LOGÍSTICO ---
      AND O.CodTipoOperacao BETWEEN 3100 AND 3396 
      AND O.CodTipoOperacao NOT IN (3146, 3155, 3248, 3338, 3397, 3249, 3356, 3311, 3315, 3292, 3341, 3376, 3375, 3205, 3354, 3328, 3266)
      AND O.CodTipoOperacao NOT IN (3000, 3005, 3017, 3029)       
      AND O.CodTipoOperacao NOT IN (3207, 3210, 3230, 3245)       
      AND O.DescricaoTipoOperacao NOT LIKE '%FULL%'
      -- Filtro dinâmico multilistas por produto
      AND (NOT EXISTS (SELECT 1 FROM #FiltroProdutos) OR I.CodProduto IN (SELECT CodProduto FROM #FiltroProdutos))
    GROUP BY YEAR(EXP.DataExpedicao), I.CodProduto, P.DescricaoProduto

    UNION ALL

    -- PARTE 2: REGRA B
    SELECT
        YEAR(EXP.DataExpedicao) AS Ano,
        I.CodProduto AS CodProd, 
        P.DescricaoProduto, 
        SUM(CAST(I.QtdNegociada AS INT)) AS Quantidade
    FROM belmicro.fato_itens I WITH (NOLOCK)
    INNER JOIN belmicro.fato_itens_notas_expedidas EXP WITH (NOLOCK) ON EXP.NumUnicoNota = I.NumUnicoNota
    INNER JOIN cadastros.dim_produtos P WITH (NOLOCK) ON P.CodProduto = I.CodProduto
    INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK) ON GP.CodGrupoProduto = P.CodGrupoProduto
    INNER JOIN belmicro.fato_operacoes O WITH (NOLOCK) ON O.NumUnicoNota = EXP.NumUnicoNota
    WHERE EXP.DataExpedicao IS NOT NULL
      AND YEAR(EXP.DataExpedicao) IN (2024, 2025, 2026)
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
      -- Filtro dinâmico multilistas por produto
      AND (NOT EXISTS (SELECT 1 FROM #FiltroProdutos) OR I.CodProduto IN (SELECT CodProduto FROM #FiltroProdutos))
    GROUP BY YEAR(EXP.DataExpedicao), I.CodProduto, P.DescricaoProduto
)

-- ==========================================
-- 3. APLICAÇÃO DO PIVOT CONDICIONAL (RESULTADO FINAL)
-- ==========================================
SELECT
    CodProd,
    DescricaoProduto,
    ISNULL(SUM(CASE WHEN Ano = 2025 THEN Quantidade ELSE 0 END), 0) AS [2025],
    ISNULL(SUM(CASE WHEN Ano = 2026 THEN Quantidade ELSE 0 END), 0) AS [2026]
FROM CTE_Expedicao_Base
GROUP BY CodProd, DescricaoProduto
ORDER BY CodProd ASC;

-- Limpeza da tabela temporária da memória
IF OBJECT_ID('tempdb..#FiltroProdutos') IS NOT NULL DROP TABLE #FiltroProdutos;