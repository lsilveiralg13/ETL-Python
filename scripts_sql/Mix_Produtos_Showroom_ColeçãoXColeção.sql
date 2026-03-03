CREATE DEFINER=`root`@`localhost` PROCEDURE `Mix_Produtos_Showroom_ColeçãoXColeção`(
    IN p_evento_1 VARCHAR(100),
    IN p_evento_2 VARCHAR(100)
)
BEGIN
    SELECT
        sf.tipo_evento AS Tipo_Evento,
        SUM(CASE WHEN sf.grupo_produto = 'BOLSAS' THEN sf.Quantidade ELSE 0 END) AS QTD_BOLSAS,
        SUM(CASE WHEN sf.grupo_produto = 'SAPATOS' THEN sf.Quantidade ELSE 0 END) AS QTD_SAPATOS,
        SUM(CASE WHEN sf.grupo_produto = 'ACESSORIOS' THEN sf.Quantidade ELSE 0 END) AS QTD_ACESSORIOS,
        SUM(CASE WHEN sf.grupo_produto = 'SACOLAS' THEN sf.Quantidade ELSE 0 END) AS QTD_SACOLAS,
        (SUM(CASE WHEN sf.grupo_produto = 'BOLSAS' THEN sf.Quantidade ELSE 0 END) +
         SUM(CASE WHEN sf.grupo_produto = 'SAPATOS' THEN sf.Quantidade ELSE 0 END) +
         SUM(CASE WHEN sf.grupo_produto = 'ACESSORIOS' THEN sf.Quantidade ELSE 0 END) +
         SUM(CASE WHEN sf.grupo_produto = 'SACOLAS' THEN sf.Quantidade ELSE 0 END)) AS TOTAL,
        CONCAT(
            FORMAT(
                (SUM(CASE WHEN sf.grupo_produto = 'BOLSAS' THEN sf.Quantidade ELSE 0 END) /
                 NULLIF(SUM(CASE WHEN sf.grupo_produto = 'SAPATOS' THEN sf.Quantidade ELSE 0 END), 0)) * 100,
            2), '%'
        ) AS `%B_P`
    FROM
        staging_mix_produtos_showroom sf
    WHERE
        sf.grupo_produto IN ('SAPATOS', 'BOLSAS', 'SACOLAS', 'ACESSORIOS')
        AND sf.tipo_evento IN (p_evento_1, p_evento_2)
    GROUP BY
        sf.tipo_evento
    ORDER BY
        TOTAL DESC;
END;