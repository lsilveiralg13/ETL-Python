CREATE DEFINER=`root`@`localhost` PROCEDURE `Mix_Produtos_Showroom`(
    IN p_tipo_evento VARCHAR(100) -- Agora aceita SOMENTE o tipo de evento

)
BEGIN
    SELECT
        sf.apelido_vendedor AS Vendedor,
        SUM(CASE WHEN sf.grupo_produto = 'BOLSAS' THEN sf.Quantidade ELSE 0 END) AS QTD_BOLSAS,
        SUM(CASE WHEN sf.grupo_produto = 'SAPATOS' THEN sf.Quantidade ELSE 0 END) AS QTD_SAPATOS,
        SUM(CASE WHEN sf.grupo_produto = 'ACESSORIOS' THEN sf.Quantidade ELSE 0 END) AS QTD_ACESSORIOS,
        SUM(CASE WHEN sf.grupo_produto = 'SACOLAS' THEN sf.Quantidade ELSE 0 END) AS QTD_SACOLAS,
        (SUM(CASE WHEN sf.grupo_produto = 'BOLSAS' THEN sf.Quantidade ELSE 0 END) +
         SUM(CASE WHEN sf.grupo_produto = 'SAPATOS' THEN sf.Quantidade ELSE 0 END) +
         SUM(CASE WHEN sf.grupo_produto = 'ACESSORIOS' THEN sf.Quantidade ELSE 0 END) +
         SUM(CASE WHEN sf.grupo_produto = 'SACOLAS' THEN sf.Quantidade ELSE 0 END)) AS TOTAL_GERAL,
        CONCAT(
            FORMAT(
                (SUM(CASE WHEN sf.grupo_produto = 'BOLSAS' THEN sf.Quantidade ELSE 0 END) /
                 NULLIF(SUM(CASE WHEN sf.grupo_produto = 'SAPATOS' THEN sf.Quantidade ELSE 0 END), 0)) * 100,
            2), '%'
        ) AS `%Bolsa_Sapatos` -- Nova coluna para o percentual de Bolsas sobre Sapatos
    FROM
        staging_mix_produtos_showroom sf
    WHERE
        sf.tipo_evento = p_tipo_evento -- Filtro agora é EXCLUSIVAMENTE pelo tipo de evento
        AND sf.grupo_produto IN ('SAPATOS', 'BOLSAS', 'SACOLAS', 'ACESSORIOS')
    GROUP BY
        sf.apelido_vendedor
    ORDER BY
        TOTAL_GERAL DESC; -- Classificação agora por TOTAL_GERAL em ordem decrescente
END;