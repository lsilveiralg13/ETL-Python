CREATE DEFINER=`root`@`localhost` PROCEDURE `Analise_Mix_Cliente_Showroom`(
    IN p_codigo_parceiro BIGINT
)
BEGIN
    WITH EventMixRawData AS (
        SELECT
            smps.cod_parceiro AS codigo_parceiro,
            smps.nome_parceiro AS nome_parceiro,
            smps.TIPO_EVENTO,
            smps.grupo_produto AS grupo_produto,
            smps.Quantidade AS quantidade_item,
            smps.valor_total AS valor_total_evento
        FROM
            faturamento_multimarcas_dw.staging_mix_produtos_showroom smps
        WHERE
            smps.cod_parceiro = p_codigo_parceiro
            AND smps.TIPO_EVENTO IN ('INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026')
            AND smps.grupo_produto IN ('BOLSAS', 'SAPATOS')
            AND smps.Quantidade > 0
    ),
    AggregatedEventData AS (
        SELECT
            ERD.codigo_parceiro,
            ERD.nome_parceiro,
            ERD.TIPO_EVENTO,
            SUM(CASE WHEN ERD.grupo_produto = 'SAPATOS' THEN ERD.quantidade_item ELSE 0 END) AS QTD_Sapatos,
            SUM(CASE WHEN ERD.grupo_produto = 'BOLSAS' THEN ERD.quantidade_item ELSE 0 END) AS QTD_Bolsas,
            SUM(ERD.valor_total_evento) AS TotalVendaEventoUnformatted
        FROM
            EventMixRawData ERD
        GROUP BY
            ERD.codigo_parceiro,
            ERD.nome_parceiro,
            ERD.TIPO_EVENTO
    )
    SELECT
        AED.codigo_parceiro AS `Cod.`,
        AED.nome_parceiro AS `Parceiro`,
        AED.TIPO_EVENTO AS `Tipo Evento`,

        AED.QTD_Sapatos,
        AED.QTD_Bolsas,
        
        CONCAT('R$ ', FORMAT(AED.TotalVendaEventoUnformatted, 2, 'de_DE')) AS `Valor Total Evento`,
        
        -- Coluna de Crescimento ajustada para exibir '0.00%' quando N/A ou valor anterior for 0
        CASE
            WHEN LAG(AED.TotalVendaEventoUnformatted, 1) OVER (
                PARTITION BY AED.codigo_parceiro
                ORDER BY FIELD(AED.TIPO_EVENTO, 'INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026')
            ) IS NULL OR LAG(AED.TotalVendaEventoUnformatted, 1) OVER (
                PARTITION BY AED.codigo_parceiro
                ORDER BY FIELD(AED.TIPO_EVENTO, 'INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026')
            ) = 0 THEN '0.00%'
            ELSE
                CONCAT(FORMAT(
                    ((AED.TotalVendaEventoUnformatted - LAG(AED.TotalVendaEventoUnformatted, 1) OVER (
                        PARTITION BY AED.codigo_parceiro
                        ORDER BY FIELD(AED.TIPO_EVENTO, 'INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026')
                    )) / LAG(AED.TotalVendaEventoUnformatted, 1) OVER (
                        PARTITION BY AED.codigo_parceiro
                        ORDER BY FIELD(AED.TIPO_EVENTO, 'INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026')
                    )) * 100, 2
                ), '%')
        END AS `Crescimento %`,
        
        CONCAT(FORMAT(
            COALESCE(
                (AED.QTD_Bolsas / NULLIF(AED.QTD_Bolsas + AED.QTD_Sapatos, 0)) * 100,
                0
            ), 2
        ), '%') AS `% Bolsas / Sapatos`,
        
        CONCAT(FORMAT(
            COALESCE(
                (AED.QTD_Sapatos / NULLIF(AED.QTD_Bolsas + AED.QTD_Sapatos, 0)) * 100,
                0
            ), 2
        ), '%') AS `% Sapatos / Bolsas`

    FROM
        AggregatedEventData AED
    ORDER BY
        AED.codigo_parceiro,
        FIELD(AED.TIPO_EVENTO, 'INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026');
END;