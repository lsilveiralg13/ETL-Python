CREATE DEFINER=`root`@`localhost` PROCEDURE `Parcial_Showroom_Completa`(
    IN p_tipo_evento VARCHAR(100)
)
BEGIN
    SELECT
        sf.`TIPO_EVENTO` AS TIPO_EVENTO,
        sf.vendedor AS vendedor,
        
        CONCAT('R$ ', FORMAT(ms.Meta_Vendedora, 2, 'de_DE')) AS Meta, -- Renomeado para Meta
        CONCAT('R$ ', FORMAT(SUM(sf.valor_total_showroom), 2, 'de_DE')) AS Vendido,
        CONCAT('R$ ', FORMAT(GREATEST(0, (ms.Meta_Vendedora - SUM(sf.valor_total_showroom))), 2, 'de_DE')) AS `GAP (R$)`,
        SUM(sf.qtd_itens) AS Itens,
        COUNT(DISTINCT sf.codigo_parceiro) AS Conversao,
        CONCAT('R$ ', FORMAT((SUM(sf.valor_total_showroom) / NULLIF(COUNT(DISTINCT sf.codigo_parceiro), 0)), 2, 'de_DE')) AS VLM,
        CONCAT(FORMAT((SUM(sf.valor_total_showroom) / NULLIF(ms.Meta_Vendedora, 0)) * 100, 2), '%') AS Atingimento
    FROM
        staging_showroom_multimarcas sf
    JOIN
        metas_showroom ms
        ON sf.`TIPO_EVENTO` = ms.Tipo_Evento
        AND sf.vendedor = ms.Nome_Vendedora
    WHERE
        sf.`TIPO_EVENTO` = p_tipo_evento
    GROUP BY
        sf.`TIPO_EVENTO`,
        sf.vendedor,
        ms.Meta_Vendedora
    ORDER BY
        SUM(sf.valor_total_showroom) DESC,
        sf.vendedor ASC;
END;