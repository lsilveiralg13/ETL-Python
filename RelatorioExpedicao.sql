/* RELATÓRIO DE EXPEDIÇÃO E PRODUÇÃO - VERSÃO ROTINA (v1.4)
   Configuração: Escolha entre ver o ano fechado ou um mês específico.
*/

-- ==========================================
-- 1. CONFIGURAÇÃO DE PARÂMETROS (AJUSTE AQUI)
-- ==========================================
DECLARE @AnoConsulta  INT = 2026;         -- Ano desejado
DECLARE @MesConsulta  INT = 4;            -- Mês desejado
DECLARE @TipoConsulta VARCHAR(10) = 'ANO'; -- Opções: 'MES' ou 'ANO'

/* DICA: 
   Se @TipoConsulta = 'MES', ele filtra o Ano e o Mês escolhidos.
   Se @TipoConsulta = 'ANO', ele ignora o mês e traz o ano inteiro (2025 e 2026).
*/

-- ==========================================
-- 2. CONSULTA PRINCIPAL
-- ==========================================
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
    CAST(I.ValorTotal AS DECIMAL(18,2))             AS ValorTotal,
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
  AND (GP.LinhaDeNegocio = 'WordPC/Skill' OR P.Marca = 'HQ')
  AND GP.NomeGrupoPai <> 'COMPONENTES' 

  -- LÓGICA DE FILTRO DINÂMICO
  AND (
      (@TipoConsulta = 'MES' AND YEAR(EXP.DataExpedicao) = @AnoConsulta AND MONTH(EXP.DataExpedicao) = @MesConsulta)
      OR 
      (@TipoConsulta = 'ANO' AND YEAR(EXP.DataExpedicao) IN (2025, 2026))
  )

ORDER BY EXP.DataExpedicao DESC;