WITH CTE_DETALHAMENTO AS (
    SELECT
        p.nomeprof AS `Professor`,
        d.nomedisc AS `Disciplina`,
        pt.siglatur AS `Turma`,
        pr.descricaopredio AS `Prédio`,
        s.descricaosala AS `Sala`,
        h.diasem AS `Dia da semana`,
        h.horainicio AS `Horario de inicio`
    FROM
        professor p
    JOIN profturma pt ON p.codprof = pt.codprof
    JOIN disciplina d ON pt.coddepto = d.coddepto AND pt.numdisc = d.numdisc
    JOIN horario h ON pt.coddepto = h.coddepto AND pt.numdisc = h.numdisc AND pt.anosem = h.anosem AND pt.siglatur = h.siglatur
    JOIN predio pr ON h.codpredio = pr.codpredio
    JOIN sala s ON h.codpredio = s.codpredio AND h.numsala = s.numsala 
)
SELECT
    JSON_PRETTY(
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'docente', `Professor`,
                'detalhes_curso', JSON_OBJECT(
                    'materia', `Disciplina`,
                    'turma_sigla', `Turma`
                ),
                'localizacao', JSON_OBJECT(
                    'bloco', `Prédio`,
                    'sala_numero', `Sala`
                ),
                'agenda', JSON_OBJECT(
                    'dia_cod', `Dia da semana`,
                    'inicio', `Horario de inicio`
                )
            )
        )
    ) AS relatorio_academico_json
FROM CTE_DETALHAMENTO;