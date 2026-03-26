DELIMITER $$

DROP PROCEDURE IF EXISTS SMT_Reincidencia_Reparos $$

CREATE PROCEDURE SMT_Reincidencia_Reparos (
    IN p_ano INT
)
BEGIN

    SELECT 
        r.num_serie AS `NUM. SERIE`,
        r.sku AS `SKU`,
        e.produto AS `DESCRIÇÃO`,
        COUNT(*) AS `REINCIDÊNCIA`,

        GROUP_CONCAT(DISTINCT r.tecnico SEPARATOR ' -> ') AS `TÉCNICOS`,

        MIN(r.data_reparo) AS `PRIMEIRA ENTRADA`,
        MAX(r.data_reparo) AS `ÚLTIMA ENTRADA`

    FROM staging_reparos r

    LEFT JOIN (
        SELECT DISTINCT sku, produto
        FROM staging_estoque_belmicro
    ) e 
        ON r.sku = e.sku

    WHERE 
        r.chave_ano = p_ano
        AND r.num_serie IS NOT NULL 
        AND r.num_serie <> ''
        AND r.sku IS NOT NULL
        AND r.sku <> ''
        AND e.produto IS NOT NULL

    GROUP BY 
        r.num_serie, 
        r.sku,
        e.produto

    HAVING COUNT(*) > 1

    ORDER BY `REINCIDÊNCIA` DESC;

END $$

DELIMITER ;