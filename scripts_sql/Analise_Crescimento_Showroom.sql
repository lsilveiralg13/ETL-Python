CREATE DEFINER=`root`@`localhost` PROCEDURE `Analise_Crescimento_Showroom`(
    IN p_current_showroom_event VARCHAR(100),
    IN p_previous_showroom_event VARCHAR(100),
    IN p_current_start_date DATE,
    IN p_previous_start_date DATE,
    IN p_num_days INT
)
BEGIN
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_dates_comparison (
        current_day_date DATE,
        previous_day_date DATE
    );

    TRUNCATE TABLE temp_dates_comparison;

    SET @counter = 0;
    SET @current_date_iterator = p_current_start_date;
    SET @previous_date_iterator = p_previous_start_date;

    WHILE @counter < p_num_days DO
        -- Adiciona condição para excluir dias específicos
        IF @current_date_iterator NOT IN ('2024-06-29', '2024-06-30') THEN
            INSERT INTO temp_dates_comparison (current_day_date, previous_day_date)
            VALUES (@current_date_iterator, @previous_date_iterator);
        END IF;

        SET @current_date_iterator = DATE_ADD(@current_date_iterator, INTERVAL 1 DAY);
        SET @previous_date_iterator = DATE_ADD(@previous_date_iterator, INTERVAL 1 DAY);
        SET @counter = @counter + 1;
    END WHILE;

    SELECT
        tdc.current_day_date AS Data_Atual,
        tdc.previous_day_date AS Data_Anterior,
        
        CONCAT('R$ ', FORMAT(
            COALESCE(SUM(CASE WHEN ssm.tipo_evento = p_current_showroom_event AND CAST(ssm.data_negociacao AS DATE) = tdc.current_day_date THEN ssm.valor_total_showroom ELSE 0 END), 0)
        , 2, 'de_DE')) AS Vendas_Dia_Atual,
        
        CONCAT('R$ ', FORMAT(
            COALESCE(SUM(CASE WHEN ssm.tipo_evento = p_previous_showroom_event AND CAST(ssm.data_negociacao AS DATE) = tdc.previous_day_date THEN ssm.valor_total_showroom ELSE 0 END), 0)
        , 2, 'de_DE')) AS Vendas_Dia_Anterior,
        
        CONCAT(
            FORMAT(
                CASE
                    WHEN COALESCE(SUM(CASE WHEN ssm.tipo_evento = p_previous_showroom_event AND CAST(ssm.data_negociacao AS DATE) = tdc.previous_day_date THEN ssm.valor_total_showroom ELSE 0 END), 0) = 0 THEN
                        CASE
                            WHEN COALESCE(SUM(CASE WHEN ssm.tipo_evento = p_current_showroom_event AND CAST(ssm.data_negociacao AS DATE) = tdc.current_day_date THEN ssm.valor_total_showroom ELSE 0 END), 0) = 0 THEN 0.00
                            ELSE 999999.99
                        END
                    ELSE
                        ((COALESCE(SUM(CASE WHEN ssm.tipo_evento = p_current_showroom_event AND CAST(ssm.data_negociacao AS DATE) = tdc.current_day_date THEN ssm.valor_total_showroom ELSE 0 END), 0) /
                          SUM(CASE WHEN ssm.tipo_evento = p_previous_showroom_event AND CAST(ssm.data_negociacao AS DATE) = tdc.previous_day_date THEN ssm.valor_total_showroom ELSE 0 END)) - 1) * 100
                END
            , 2), '%'
        ) AS Pct_Cresc
    FROM
        temp_dates_comparison tdc
    LEFT JOIN
        staging_showroom_multimarcas ssm ON 
        (CAST(ssm.data_negociacao AS DATE) = tdc.current_day_date AND ssm.tipo_evento = p_current_showroom_event)
        OR
        (CAST(ssm.data_negociacao AS DATE) = tdc.previous_day_date AND ssm.tipo_evento = p_previous_showroom_event)
    GROUP BY
        tdc.current_day_date, tdc.previous_day_date
    ORDER BY
        tdc.current_day_date ASC;

    DROP TEMPORARY TABLE IF EXISTS temp_dates_comparison;

END;