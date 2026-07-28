DELIMITER $$

DROP PROCEDURE IF EXISTS Scrap_Por_Peça $$

CREATE PROCEDURE Scrap_Por_Peça (
    IN p_mes VARCHAR(20),
    IN p_ano INT
)
BEGIN

    SELECT 
        sku AS 'SKU',
        produto_desc AS 'DESCRIÇÃO PRODUTO',
        chave_mes AS 'MÊS',
        chave_ano AS 'ANO',
        COUNT(*) AS 'ENTRADAS',

        SUM(CASE WHEN situacao = 'SCRAP' THEN 1 ELSE 0 END) AS 'TOTAL SCRAP',

        CONCAT(
            ROUND(
                (SUM(CASE WHEN situacao = 'SCRAP' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 
                2
            ),
            '%'
        ) AS '% DE SCRAP'

    FROM staging_reparos

    WHERE chave_ano = p_ano
      AND (chave_mes = p_mes OR p_mes = 'TUDO')

    GROUP BY sku, produto_desc, chave_mes, chave_ano

    HAVING COUNT(*) > 5
       AND SUM(CASE WHEN situacao = 'SCRAP' THEN 1 ELSE 0 END) > 0

    ORDER BY 

        -- Ordem cronológica quando for "TUDO"
        CASE 
            WHEN p_mes = 'TUDO' THEN 
                CASE chave_mes
                    WHEN 'JANEIRO' THEN 1
                    WHEN 'FEVEREIRO' THEN 2
                    WHEN 'MARÇO' THEN 3
                    WHEN 'ABRIL' THEN 4
                    WHEN 'MAIO' THEN 5
                    WHEN 'JUNHO' THEN 6
                    WHEN 'JULHO' THEN 7
                    WHEN 'AGOSTO' THEN 8
                    WHEN 'SETEMBRO' THEN 9
                    WHEN 'OUTUBRO' THEN 10
                    WHEN 'NOVEMBRO' THEN 11
                    WHEN 'DEZEMBRO' THEN 12
                END
        END ASC,

        -- Ordem por percentual quando NÃO for "TUDO"
        CASE 
            WHEN p_mes <> 'TUDO' THEN 
                (SUM(CASE WHEN situacao = 'SCRAP' THEN 1 ELSE 0 END) / COUNT(*))
        END DESC;

END $$

DELIMITER ;