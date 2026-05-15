/* CONSOLIDADO MENSAL DE PRODUÇÃO - 2025 x 2026 */

WITH BASE AS (

    SELECT 
        YEAR(O.DataNegociacao) AS Ano,
        MONTH(O.DataNegociacao) AS Mes,
        SUM(I.QtdNegociada) AS Qtd_Total_na_OP
    FROM belmicro.fato_operacoes AS O
    INNER JOIN belmicro.fato_itens AS I 
        ON O.NumUnicoNota = I.NumUnicoNota
    INNER JOIN cadastros.dim_produtos AS P 
        ON I.CodProduto = P.CodProduto
    INNER JOIN cadastros.dim_grupo_produtos AS G 
        ON P.CodGrupoProduto = G.CodGrupoProduto

    WHERE O.DataNegociacao >= '2025-01-01'
      AND O.DataNegociacao <= GETDATE()
      AND O.CodTipoOperacao IN (1102,1119,1607)
      AND G.NomeGrupoPai IN ('DESKTOP', 'ALL IN ONE', 'MONITORES', 'TV') 
      AND G.LinhadeNegocio = 'WordPC/Skill'

    GROUP BY 
        YEAR(O.DataNegociacao),
        MONTH(O.DataNegociacao)

)

SELECT
    CASE Mes
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro'
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
    END AS Mes,

    SUM(CASE WHEN Ano = 2025 THEN Qtd_Total_na_OP ELSE 0 END) AS [2025],
    SUM(CASE WHEN Ano = 2026 THEN Qtd_Total_na_OP ELSE 0 END) AS [2026]

FROM BASE

GROUP BY Mes

ORDER BY 
    CASE Mes
        WHEN 1 THEN 1
        WHEN 2 THEN 2
        WHEN 3 THEN 3
        WHEN 4 THEN 4
        WHEN 5 THEN 5
        WHEN 6 THEN 6
        WHEN 7 THEN 7
        WHEN 8 THEN 8
        WHEN 9 THEN 9
        WHEN 10 THEN 10
        WHEN 11 THEN 11
        WHEN 12 THEN 12
    END;