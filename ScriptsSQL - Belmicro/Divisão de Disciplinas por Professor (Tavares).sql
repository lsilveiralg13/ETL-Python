SELECT 
	p.nomeprof AS `Professor`,
	d.nomedisc AS `Nome da Disciplina`,
    pt.siglatur AS `Sigla da Turma`
FROM 
	professor p
JOIN profturma pt ON p.codprof = pt.codprof
JOIN disciplina d ON pt.coddepto = d.coddepto AND pt.numdisc = d.numdisc
WHERE p.nomeprof LIKE '%Tavares%';
    