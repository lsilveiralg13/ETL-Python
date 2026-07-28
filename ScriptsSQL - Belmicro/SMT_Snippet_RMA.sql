DELIMITER $$

DROP PROCEDURE IF EXISTS SMT_Snippet_RMA $$

CREATE PROCEDURE SMT_Snippet_RMA ( 
)
BEGIN
    WITH Calculo_Reparos AS (
        SELECT 
            chave_ano,
            COUNT(*) AS Total_Geral,
            SUM(CASE WHEN origem = 'PRODUÇÃO' THEN 1 ELSE 0 END) AS qtd_producao,
            SUM(CASE WHEN origem = 'ASSISTÊNCIA' THEN 1 ELSE 0 END) AS qtd_assistencia,
            SUM(CASE WHEN origem = 'SMT' THEN 1 ELSE 0 END) AS qtd_smt,
            COUNT(DISTINCT tecnico) AS total_tecnicos
        FROM 
            staging_reparos
        GROUP BY 
            chave_ano WITH ROLLUP -- O segredo está aqui!
    )
    SELECT 
        IFNULL(CAST(chave_ano AS CHAR), 'TOTAL GERAL') AS Ano,
        Total_Geral AS 'QTD TOTAL',
        qtd_producao AS 'QTD PRODUÇÃO',
        qtd_assistencia AS 'QTD ASSISTÊNCIA',
        qtd_smt AS 'QTD SMT',
        total_tecnicos AS 'TÉCNICOS ATIVOS',
        ROUND(Total_Geral / NULLIF(total_tecnicos, 0), 0) AS 'MÉDIA POR TÉCNICO',
        CONCAT(IFNULL(ROUND((qtd_producao / NULLIF(Total_Geral, 0)) * 100, 2), 0), '%') AS '% PRODUÇÃO',
        CONCAT(IFNULL(ROUND((qtd_assistencia / NULLIF(Total_Geral, 0)) * 100, 2), 0), '%') AS '% ASSISTÊNCIA',
		CONCAT(IFNULL(ROUND((qtd_smt / NULLIF(Total_Geral, 0)) * 100, 2), 0), '%') AS '% SMT'
    FROM 
        Calculo_Reparos
    ORDER BY 
        (chave_ano IS NULL) ASC, chave_ano ASC;
END $$

DELIMITER ;