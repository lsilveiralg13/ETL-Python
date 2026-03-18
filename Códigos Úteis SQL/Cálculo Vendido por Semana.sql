SELECT
	DATE(data_negociacao) as `Data da Venda`,
	CASE
		WHEN DAYOFWEEK(data_negociacao) = 1 THEN 'Domingo'
        WHEN DAYOFWEEK(data_negociacao) = 2 THEN 'Segunda-Feira'
        WHEN DAYOFWEEK(data_negociacao) = 3 THEN 'Terça-Feira'
        WHEN DAYOFWEEK(data_negociacao) = 4 THEN 'Quarta-Feira'
        WHEN DAYOFWEEK(data_negociacao) = 5 THEN 'Quinta-Feira'
        WHEN DAYOFWEEK(data_negociacao) = 6 THEN 'Sexta-Feira'
        WHEN DAYOFWEEK(data_negociacao) = 7 THEN 'Sábado'
        ELSE 'Data Desconhecida'
	END AS `Dia da Semana`,
    CONCAT('R$ ', FORMAT(COALESCE(SUM(valor_faturado), 0), 2, 'de_DE')) AS `Total Faturado`
    FROM staging_faturamento_multimarcas
    WHERE data_negociacao >= '2025-07-28 00:00:00'
    AND data_negociacao <= '2025-07-31 23:59:59'
    GROUP BY DATE (data_negociacao), `Dia da Semana`
    ORDER BY `Data da Venda` ASC;