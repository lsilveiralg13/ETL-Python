SELECT 
	`Cod. Parceiro`,
    `Parceiro`,
    `Vendedor`,
	Faturado
FROM (
    SELECT 
		codigo_parceiro AS `Cod. Parceiro`,
		nome_parceiro AS `Parceiro`,
        vendedor AS `Vendedor`,
		CONCAT('R$ ', FORMAT(COALESCE(SUM(valor_faturado), 0), 2, 'de_DE')) AS Faturado,
        COALESCE(SUM(valor_faturado), 0) AS Ordenado,
		ROW_NUMBER() OVER (PARTITION BY vendedor ORDER BY COALESCE(SUM(valor_faturado), 0) DESC) as rn
	FROM staging_faturamento_multimarcas
	WHERE vendedor IN ('ERIKHA', 'GLENDASOUZA', 'ISABELLASILVA', 'JOSIANEVIEIRA', 'LUCIANAPEREIRA', 'MARCELAVAZ', 'NELIANE')
	AND (
			(data_negociacao >= '2025-01-01' AND data_negociacao < '2025-07-01')
		)
	AND tipo_venda IN ('PRONTA ENTREGA', 'SHOWROOM')
GROUP BY codigo_parceiro, nome_parceiro, vendedor
) AS subquery
WHERE rn <= 10
ORDER BY `Vendedor`, Ordenado DESC;

    