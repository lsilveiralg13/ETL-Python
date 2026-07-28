DELIMITER $$

DROP PROCEDURE IF EXISTS PROD_Produtividade_Volume $$

CREATE PROCEDURE PROD_Produtividade_Volume (
    IN p_mes VARCHAR(20),
    IN p_ano VARCHAR(10) -- Alterado para VARCHAR para suportar o 'TODOS'
)
BEGIN
    SET p_ano = UPPER(TRIM(p_ano));
    SET p_mes = UPPER(TRIM(p_mes));

    SELECT 
        p.chave_ano AS Ano, 
        p.chave_mes AS Mes, 
        
        -- Entrada: O que foi planejado
        SUM(COALESCE(p.qtd_op, 0)) AS 'PLANEJADO (QTD OPs)',

        -- 1. Volume Total de Produção (Throughput)
        -- O que foi efetivamente realizado
        SUM(COALESCE(p.embalagem, 0)) AS 'THROUGHPUT', 
        
        -- 2. Backlog Atual (Pendentes)
        -- Lógica Corrigida: Planejado - Realizado
        (SUM(COALESCE(p.qtd_op, 0)) - SUM(COALESCE(p.embalagem, 0))) AS 'BACKLOG',
        
        -- 3. Capacidade Utilizada
        -- Percentual do que foi realizado sobre o planejado
        ROUND(
            (SUM(COALESCE(p.embalagem, 0)) / NULLIF(SUM(COALESCE(p.qtd_op, 0)), 0)) * 100, 
        2) AS '% CAPACIDADE UTILIZADA'

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