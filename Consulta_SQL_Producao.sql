-- ============================================================
-- RELATÓRIO: Volume de Produtos PA Expedidos
-- PREMISSAS:
--   1. Volume de PA expedidos com períodos extratificados
--   2. Períodos: Ano, Mês, Semana, Dia, Hora
--   3. PA de PCs (incluindo periféricos) + Linha HQ (todos)
-- ============================================================

SELECT
    -- PERÍODOS EXTRATIFICADOS
    YEAR(EXP.DataExpedicao)                         AS Ano,
    MONTH(EXP.DataExpedicao)                        AS Mes,
    WEEKOFYEAR(EXP.DataExpedicao)                   AS Semana,
    CAST(EXP.DataExpedicao AS DATE)                 AS DataExpedicao,
    HOUR(EXP.DataExpedicao)                         AS Hora,

    -- COLUNAS DO RELATÓRIO
    I.CodProduto                                    AS CodProd,
    P.DescricaoProduto                              AS DescricaoProduto,
    P.Marca                                         AS Fornecedor,
    P.ModeloMkt                                     AS Modelo,
    GP.NomeGrupoPai                                 AS Familia,
    'EXPEDIDO'                                      AS Situacao,
    EXP.DataExpedicao                               AS DataExpedicaoCompleta,
    I.CodLocal                                      AS CodLocalEstoque,
    L.NomeLocal                                     AS LocalEstoque,
    CAST(I.QtdNegociada AS INT)                     AS Quantidade,

    -- CONTEXTO ADICIONAL
    GP.LinhaDeNegocio,
    GP.NomeGrupoFamilia

FROM belmicro.fato_itens I

INNER JOIN belmicro.fato_itens_notas_expedidas EXP
    ON EXP.NumUnicoNota = I.NumUnicoNota

INNER JOIN cadastros.dim_produtos P
    ON P.CodProduto = I.CodProduto

INNER JOIN cadastros.dim_grupo_produtos GP
    ON GP.CodGrupoProduto = P.CodGrupoProduto

LEFT JOIN cadastros.dim_locais L
    ON L.CodLocal = I.CodLocal

WHERE EXP.DataExpedicao IS NOT NULL
  AND P.UsadoComo = 'Venda (fabricação própria)'
  AND (
      GP.LinhaDeNegocio = 'WordPC/Skill'     
      OR P.Marca = 'HQ'                       
  )

ORDER BY EXP.DataExpedicao DESC