CREATE DEFINER=`root`@`localhost` PROCEDURE `Comparativo_Evento_Showroom`(
	IN p_current_event_type VARCHAR(100),
    IN p_previous_event_type VARCHAR(100)
)
BEGIN 
    -- Declara variáveis para armazenar os valores agregados do tipo de evento atual
    DECLARE v_current_vendido DECIMAL(18, 2);
    DECLARE v_current_compradores INT;
    DECLARE v_current_vlm DECIMAL(18, 2);

    -- Declara variáveis para armazenar os valores agregados do tipo de evento anterior
    DECLARE v_previous_vendido DECIMAL(18, 2);
    DECLARE v_previous_compradores INT;
    DECLARE v_previous_vlm DECIMAL(18, 2);

    -- Calcula os agregados para o tipo de evento atual
    SELECT
        COALESCE(SUM(ssm.valor_total_showroom), 0),
        COALESCE(COUNT(DISTINCT ssm.codigo_parceiro), 0),
        COALESCE(SUM(ssm.valor_total_showroom) / NULLIF(COUNT(DISTINCT ssm.codigo_parceiro), 0), 0)
    INTO
        v_current_vendido, v_current_compradores, v_current_vlm
    FROM
        staging_showroom_multimarcas ssm
    WHERE
        ssm.tipo_evento = p_current_event_type;

    -- Calcula os agregados para o tipo de evento anterior
    SELECT
        COALESCE(SUM(ssm.valor_total_showroom), 0),
        COALESCE(COUNT(DISTINCT ssm.codigo_parceiro), 0),
        COALESCE(SUM(ssm.valor_total_showroom) / NULLIF(COUNT(DISTINCT ssm.codigo_parceiro), 0), 0)
    INTO
        v_previous_vendido, v_previous_compradores, v_previous_vlm
    FROM
        staging_showroom_multimarcas ssm
    WHERE
        ssm.tipo_evento = p_previous_event_type;

    -- Consulta SELECT principal para exibir os dados
	SELECT
		ssm.tipo_evento AS Evento,
        CONCAT('R$ ', FORMAT(COALESCE(SUM(ssm.valor_total_showroom), 0), 2, 'de_DE')) AS Vendido,
        COALESCE(COUNT(DISTINCT ssm.codigo_parceiro), 0) AS Compradores,
        CONCAT('R$ ', FORMAT(COALESCE(SUM(ssm.valor_total_showroom) / NULLIF(COUNT(DISTINCT ssm.codigo_parceiro), 0), 0), 2, 'de_DE')) AS VLM
        -- As colunas de crescimento foram removidas
	FROM staging_showroom_multimarcas ssm
    WHERE ssm.tipo_evento IN (p_current_event_type, p_previous_event_type)
    GROUP BY ssm.tipo_evento
    ORDER BY ssm.tipo_evento ASC;
END;