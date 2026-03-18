SELECT
    regiao AS `Região`,
    tipo_negociacao AS `Forma de Pagamento`,
    COUNT(tipo_negociacao) AS `QTD Tipo Negociação`,
    CONCAT('R$ ', FORMAT(COALESCE(SUM(valor_faturado), 0), 2)) AS `QTD_Faturado`
FROM
    staging_faturamento_multimarcas
WHERE
    chave_mes = 'AGOSTO' AND chave_ano = 2025 AND regiao = 'Sudeste'
    AND tipo_negociacao NOT IN ('MM - SHOWROOM 30/60/90/120', 'MM - SHOWROOM 30/60/90/120/150')
GROUP BY
	regiao,
    tipo_negociacao
ORDER BY
	regiao,
	`QTD Tipo Negociação` DESC
    LIMIT 5;