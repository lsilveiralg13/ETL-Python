DELIMITER $$

DROP PROCEDURE IF EXISTS PROD_Eficiencia_Fluxo $$

CREATE PROCEDURE PROD_Eficiencia_Fluxo (
    IN p_mes VARCHAR(20),
    IN p_ano VARCHAR(10)
)
BEGIN
    SET p_ano = UPPER(TRIM(p_ano));
    SET p_mes = UPPER(TRIM(p_mes));

    SELECT 
        p.chave_ano AS Ano, 
        p.chave_mes AS Mes, 
        
        -- 1. Eficiência de Teste (Run-in Conversion)
        -- Dos que passaram na qualidade, quantos chegaram ao teste de estresse?
        ROUND(
            (SUM(COALESCE(p.runin, 0)) / NULLIF(SUM(COALESCE(p.qualidade, 0)), 0)) * 100,
        2) AS '% CONVERSÃO RUN-IN',
        
        -- 2. Eficiência de Finalização
        -- Dos que estavam em teste, quantos foram efetivamente embalados?
        ROUND(
            (SUM(COALESCE(p.embalagem, 0)) / NULLIF(SUM(COALESCE(p.runin, 0)), 0)) * 100,
        2) AS '% EFICIÊNCIA FINALIZAÇÃO',

        -- Colunas de Apoio (Volumes de Fluxo)
        SUM(COALESCE(p.qualidade, 0)) AS 'SAÍDA MONTAGEM',
        SUM(COALESCE(p.runin, 0)) AS 'EM TESTE (RUN-IN)',
        SUM(COALESCE(p.embalagem, 0)) AS 'PRONTO (EMBALAGEM)'

    FROM staging_producao_pcs p
    
    WHERE 
        (p_ano = 'TODOS' OR UPPER(TRIM(p.chave_ano)) = p_ano) AND
        (p_mes = 'TODOS' OR UPPER(TRIM(p.chave_mes)) = p_mes)

    GROUP BY p.chave_ano, p.chave_mes 

    ORDER BY 
        p.chave_ano DESC, 
        FIELD(UPPER(TRIM(p.chave_mes)), 'JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO', 'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO') ASC;
END$$

DELIMITER ;