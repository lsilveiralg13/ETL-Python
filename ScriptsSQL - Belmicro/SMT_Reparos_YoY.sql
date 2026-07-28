DELIMITER $$

DROP PROCEDURE IF EXISTS SMT_Reparos_YoY $$

CREATE PROCEDURE SMT_Reparos_YoY (
    IN p_ano INT
)
BEGIN
    WITH Mensal_Consolidado AS (
        SELECT 
            chave_ano,
            chave_mes,
            -- Transformamos o nome em número para o LAG "pular" 12 meses
            FIELD(UPPER(TRIM(chave_mes)), 
                'JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO', 
                'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO'
            ) AS mes_n,
            COUNT(*) AS total_reparos 
        FROM staging_reparos
        GROUP BY chave_ano, chave_mes, mes_n
    ),
    Calculo_SameStore AS (
        SELECT 
            chave_ano,
            chave_mes,
            mes_n,
            total_reparos,
            -- O PULO DO GATO: LAG com offset 12 (busca o mesmo mês no ano anterior)
            LAG(total_reparos, 1) OVER (PARTITION BY mes_n ORDER BY chave_ano) AS total_ano_anterior
        FROM Mensal_Consolidado
    )
    SELECT 
        chave_ano AS Ano,
        UPPER(chave_mes) AS Mes,
        total_reparos AS 'Qtd Atual',
        IFNULL(total_ano_anterior, 0) AS 'Qtd Ano Anterior',
        (CAST(total_reparos AS SIGNED) - CAST(IFNULL(total_ano_anterior, 0) AS SIGNED)) AS 'Dif. Absoluta YoY',
        CASE 
            WHEN total_ano_anterior IS NULL OR total_ano_anterior = 0 THEN '0.00%' 
            ELSE CONCAT(
                FORMAT(((total_reparos - total_ano_anterior) / total_ano_anterior) * 100, 2), 
                '%'
            )
        END AS 'Crescimento SameStore (%)'
    FROM Calculo_SameStore
    WHERE chave_ano = p_ano
    ORDER BY mes_n ASC;
END $$

DELIMITER ;