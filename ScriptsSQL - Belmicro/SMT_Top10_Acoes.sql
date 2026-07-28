DELIMITER $$

DROP PROCEDURE IF EXISTS SMT_Top10_Acoes $$

CREATE PROCEDURE SMT_Top10_Acoes (
    IN p_mes VARCHAR(20), 
    IN p_ano INT
)
BEGIN
    WITH Total_Filtrado AS (
        SELECT COUNT(*) AS total 
        FROM staging_reparos
        WHERE (p_ano = 0 OR chave_ano = p_ano)
          AND (p_mes = '0' OR UPPER(chave_mes) = UPPER(p_mes))
    )
    SELECT 
        IF(p_mes = '0', 'ACUMULADO', UPPER(chave_mes)) AS 'MÊS',
        chave_ano AS 'ANO',
        
        CASE 
            WHEN UPPER(TRIM(acao_realizada)) = 'SEM AÇÃO' THEN 'SEM AÇÃO - NO FAULT FOUND'
            WHEN UPPER(TRIM(acao_realizada)) = 'AGUARDANDO COMPONENTE' THEN 'PENDENTE - AGUARDANDO PEÇA'
            ELSE UPPER(TRIM(acao_realizada)) 
        END AS 'AÇÕES',
        
        COUNT(*) AS 'QTD',
        
        CONCAT(
            IFNULL(
                ROUND((COUNT(*) / NULLIF((SELECT total FROM Total_Filtrado), 0)) * 100, 2), 
                0
            ), 
            '%'
        ) AS '% SOB TOTAL'
        
    FROM  
        staging_reparos
    WHERE 
        (p_ano = 0 OR chave_ano = p_ano)
        AND (p_mes = '0' OR UPPER(chave_mes) = UPPER(p_mes))
        
    GROUP BY 
		chave_mes,
        chave_ano,
        acao_realizada
        
    ORDER BY 
        4 DESC
    LIMIT 10;
END $$

DELIMITER ;