DELIMITER $$

DROP PROCEDURE IF EXISTS PROD_Produtividade_FPY $$

CREATE PROCEDURE PROD_Produtividade_PCS (
    IN p_ano INT,
    IN p_mes VARCHAR(20)
)
BEGIN
    -- Forçando os parâmetros para Maiúsculo e sem espaços nas pontas
    SET p_ano = UPPER(TRIM(p_ano));
    SET p_mes = UPPER(TRIM(p_mes));

    SELECT 
        p.chave_ano AS Ano, 
        p.chave_mes AS Mes, 
        
        SUM(COALESCE(p.qtd_op, 0)) AS 'TOTAL DE SKUs', 
        SUM(COALESCE(p.embalagem, 0)) AS 'SKUs FINALIZADAS', 
        (SUM(COALESCE(p.qtd_op, 0)) - SUM(COALESCE(p.embalagem, 0))) AS 'SKUs EM PRODUÇÃO',
        SUM(COALESCE(p.reparo, 0)) AS 'TOTAL DE REPAROS', 
        SUM(COALESCE(p.runin, 0)) AS 'TOTAL RUNIN',

        ROUND( 
            (SUM(COALESCE(p.qualidade, 0)) - SUM(COALESCE(p.reparo, 0))) / NULLIF(SUM(COALESCE(p.qualidade, 0)), 0) * 100,  
        2) AS 'FPY (%)',

        ROUND( 
            (SUM(COALESCE(p.embalagem, 0)) / NULLIF(SUM(COALESCE(p.qtd_op, 0)), 0)) * 100,  
        2) AS '% EFICIÊNCIA OP' 

    FROM staging_producao_pcs p
    
    WHERE 
        -- Compara limpando espaços e forçando Upper em ambos os lados
        (p_ano = NULL OR UPPER(TRIM(p.chave_ano)) = p_ano) AND
        (p_mes = 'TODOS' OR UPPER(TRIM(p.chave_mes)) = p_mes)

    GROUP BY p.chave_ano, p.chave_mes 

    ORDER BY 
        p.chave_ano DESC, 
        FIELD(UPPER(TRIM(p.chave_mes)), 'JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO', 'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO') ASC;
END$$

DELIMITER ;