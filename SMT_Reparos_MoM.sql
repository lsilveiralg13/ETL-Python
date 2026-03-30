DELIMITER $$

DROP PROCEDURE IF EXISTS SMT_Reparos_MoM$$

CREATE PROCEDURE SMT_Reparos_MoM (
    IN p_ano INT
)
BEGIN
    WITH Mensal_Consolidado AS (
        SELECT 
            chave_ano,
            chave_mes,
            -- FIELD retorna a posição da string na lista (JANEIRO=1, FEVEREIRO=2...)
            FIELD(UPPER(TRIM(chave_mes)), 
                'JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO', 
                'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO'
            ) AS mes_n,
            COUNT(*) AS total_reparos 
        FROM staging_reparos
        GROUP BY chave_ano, chave_mes, mes_n
    ),
    Calculo_Crescimento AS (
        SELECT 
            chave_ano,
            chave_mes,
            mes_n,
            total_reparos,
            -- Agora o ORDER BY usa o número (mes_n) e não o nome
            LAG(total_reparos) OVER (ORDER BY chave_ano, mes_n) AS total_anterior
        FROM Mensal_Consolidado
    )
    SELECT 
        chave_ano AS Ano,
        UPPER(chave_mes) AS Mes,
        total_reparos AS 'Qtd Atual',
        IFNULL(total_anterior, 0) AS 'Qtd Anterior',
        (CAST(total_reparos AS SIGNED) - CAST(IFNULL(total_anterior, 0) AS SIGNED)) AS 'Dif. Absoluta',
        CASE 
            WHEN total_anterior IS NULL OR total_anterior = 0 THEN '0.00%' 
            ELSE CONCAT(
                FORMAT(((total_reparos - total_anterior) / total_anterior) * 100, 2), 
                '%'
            )
        END AS 'Crescimento (%)'
    FROM Calculo_Crescimento
    WHERE chave_ano = p_ano
    ORDER BY mes_n ASC; -- Garante a exibição de Janeiro a Dezembro
END $$

DELIMITER ;