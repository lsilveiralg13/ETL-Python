TRUNCATE TABLE carteira_is;
INSERT INTO carteira_is (vendedor, Ativas, Bloqueadas, Inativas, `Base de Ativos`)
SELECT
    COALESCE(vendedor, 'TOTAL GERAL') AS vendedor,
    COUNT(CASE WHEN status_cliente = 'Loja Ativa' THEN 1 ELSE NULL END) AS Ativas,
    COUNT(CASE WHEN status_cliente = 'Loja Bloqueada' THEN 1 ELSE NULL END) AS Bloqueadas,
    COUNT(CASE WHEN status_cliente = 'Loja Inativa' THEN 1 ELSE NULL END) AS Inativas,
    COUNT(CASE WHEN status_cliente = 'Loja Ativa' THEN 1 ELSE NULL END) +
    COUNT(CASE WHEN status_cliente = 'Loja Bloqueada' THEN 1 ELSE NULL END) AS `Base de Ativos`
FROM staging_cadastro_clientes
GROUP BY vendedor WITH ROLLUP;