/* RELATÓRIO DE EXPEDIÇÃO E PRODUÇÃO - PRODUTO ACABADO (v1.2)
   Filtros: Apenas Marcas Próprias | Exclui Componentes
*/

DECLARE @AnoConsulta INT = 2026;
DECLARE @MesConsulta INT = NULL;

SELECT
    YEAR(EXP.DataExpedicao)                         AS Ano,
    MONTH(EXP.DataExpedicao)                        AS Mes,
    DATEPART(ISO_WEEK, EXP.DataExpedicao)           AS Semana,
    CAST(EXP.DataExpedicao AS DATE)                 AS DataExpedicao,
    DATEPART(HOUR, EXP.DataExpedicao)               AS Hora,
    I.CodProduto                                    AS CodProd,
    P.DescricaoProduto                              AS DescricaoProduto,
    P.Marca                                         AS Fornecedor,
    P.ModeloMkt                                     AS Modelo,
    GP.NomeGrupoPai                                 AS Familia,
    'EXPEDIDO'                                      AS Situacao,
    EXP.DataExpedicao                               AS DataExpedicaoCompleta,
    I.CodLocal                                      AS CodLocalEstoque,
    CAST(I.QtdNegociada AS INT)                     AS Quantidade,
    GP.LinhaDeNegocio,
    GP.NomeGrupoFamilia

FROM belmicro.fato_itens I WITH (NOLOCK)
INNER JOIN belmicro.fato_itens_notas_expedidas EXP WITH (NOLOCK)
    ON EXP.NumUnicoNota = I.NumUnicoNota
INNER JOIN cadastros.dim_produtos P WITH (NOLOCK)
    ON P.CodProduto = I.CodProduto
INNER JOIN cadastros.dim_grupo_produtos GP WITH (NOLOCK)
    ON GP.CodGrupoProduto = P.CodGrupoProduto

WHERE EXP.DataExpedicao IS NOT NULL
  AND P.UsadoComo = 'Venda (fabricação própria)'
  -- FILTRO DE MARCAS/LINHAS PRÓPRIAS
  AND (GP.LinhaDeNegocio = 'WordPC/Skill' OR P.Marca = 'HQ')
  -- EXCLUSÃO DE COMPONENTES (PRODUTO ACABADO APENAS)
  AND GP.NomeGrupoPai <> 'COMPONENTES' 
  -- FILTROS DE TEMPO
  AND YEAR(EXP.DataExpedicao) = @AnoConsulta
  AND (@MesConsulta IS NULL OR MONTH(EXP.DataExpedicao) = @MesConsulta)

ORDER BY EXP.DataExpedicao DESC;