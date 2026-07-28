SELECT 
    O.CodTipoOperacao,
    COUNT(DISTINCT O.NumNota) AS Qtd_Notas_OPs,
    SUM(I.QtdNegociada) AS Volume_Total_Pecas,
    COUNT(*) AS Total_Linhas_no_Banco
FROM belmicro.fato_operacoes AS O
INNER JOIN belmicro.fato_itens AS I 
    ON O.NumUnicoNota = I.NumUnicoNota
WHERE O.DataEntradaSaida >= '2025-01-01'
  AND O.CodTipoOperacao IN (1607, 1600, 1603, 1604)
GROUP BY O.CodTipoOperacao
ORDER BY Volume_Total_Pecas DESC;