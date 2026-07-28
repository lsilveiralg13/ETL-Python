SELECT
	p.descricaopredio AS `Descrição do Prédio`,
    COUNT(s.numsala) AS `Quantidade de Salas`
FROM
	predio p
JOIN sala s ON p.codpredio = s.codpredio
GROUP BY p.codpredio, p.descricaopredio
HAVING COUNT(s.numsala) > 3
ORDER BY `Quantidade de Salas` DESC;