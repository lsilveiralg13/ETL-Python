DELIMITER $$

DROP PROCEDURE IF EXISTS SMT_Scrap_SKU $$

CREATE PROCEDURE SMT_Scrap_SKU (
    IN p_mes VARCHAR(20),
    IN p_ano INT
)
BEGIN

    SELECT 
        r.sku AS `SKU`,
        e.produto AS `DESCRIÇÃO PRODUTO`,
        r.chave_mes AS `MÊS`,
        r.chave_ano AS `ANO`,
        COUNT(*) AS `ENTRADAS`,

        SUM(CASE WHEN r.situacao = 'SCRAP' THEN 1 ELSE 0 END) AS `TOTAL SCRAP`,

        CONCAT(
            ROUND(
                (SUM(CASE WHEN r.situacao = 'SCRAP' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 
                2
            ),
            '%'
        ) AS `% DE SCRAP`

    FROM staging_reparos r

    LEFT JOIN (
        SELECT DISTINCT sku, produto
        FROM staging_estoque_belmicro
    ) e 
        ON r.sku = e.sku

    WHERE 
        r.chave_ano = p_ano
        AND (r.chave_mes = p_mes OR p_mes = 'TUDO')
        AND e.produto IS NOT NULL

    GROUP BY 
        r.sku, 
        e.produto,
        r.chave_mes, 
        r.chave_ano

    HAVING COUNT(*) > 5
       AND SUM(CASE WHEN r.situacao = 'SCRAP' THEN 1 ELSE 0 END) > 0

    ORDER BY 

        -- Ordem cronológica quando for "TUDO"
        CASE 
            WHEN p_mes = 'TUDO' THEN 
                CASE r.chave_mes
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
                (SUM(CASE WHEN r.situacao = 'SCRAP' THEN 1 ELSE 0 END) / COUNT(*))
        END DESC;

END $$

DELIMITER ;