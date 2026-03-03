CREATE DEFINER=`root`@`localhost` PROCEDURE `Historico_Showroom`()
BEGIN
    SELECT
        CombinedData.Evento_Showroom,
        CombinedData.Ano_Evento,
        COALESCE(CombinedData.Agendados_Count, 0) AS Agendados,
        COALESCE(CombinedData.Convertidos_Count, 0) AS Convertidos,
        CONCAT('R$ ', FORMAT(CombinedData.Orcado_Valor, 2, 'de_DE')) AS Orçado,
        CONCAT('R$ ', FORMAT(CombinedData.Faturado_Valor, 2, 'de_DE')) AS Faturado,
        CONCAT('R$ ', FORMAT(CombinedData.Orcado_Valor - CombinedData.Faturado_Valor, 2, 'de_DE')) AS Não_Faturado,
        CONCAT(
            FORMAT(
                COALESCE(
                    ((CombinedData.Orcado_Valor - CombinedData.Faturado_Valor) / NULLIF(CombinedData.Orcado_Valor, 0)) * 100,
                    0.00
                ), 2
            ), '%'
        ) AS PerdaPercentual
    FROM
        (
            SELECT
                COALESCE(ssm_agg.tipo_evento, sfm_agg.tipo_showroom) AS Evento_Showroom,
                COALESCE(ssm_agg.chave_ano, sfm_agg.chave_ano) AS Ano_Evento,
                COALESCE(ssm_agg.Total_Vendas_Showroom_Orcado, 0) AS Orcado_Valor,
                COALESCE(sfm_agg.Total_Faturamento_Showroom_Realizado, 0) AS Faturado_Valor,
                COALESCE(ssm_agg.Total_Clientes_Agendados, 0) AS Agendados_Count,
                COALESCE(sfm_agg.Total_Clientes_Convertidos, 0) AS Convertidos_Count
            FROM
                (SELECT
                    tipo_evento,
                    chave_ano,
                    SUM(valor_total_showroom) AS Total_Vendas_Showroom_Orcado,
                    COUNT(DISTINCT codigo_parceiro) AS Total_Clientes_Agendados
                FROM
                    staging_showroom_multimarcas
                GROUP BY
                    tipo_evento, chave_ano
                ) AS ssm_agg
            LEFT JOIN
                (SELECT
                    tipo_showroom,
                    chave_ano,
                    SUM(valor_faturado) AS Total_Faturamento_Showroom_Realizado,
                    COUNT(DISTINCT codigo_parceiro) AS Total_Clientes_Convertidos
                FROM
                    staging_faturamento_multimarcas
                GROUP BY
                    tipo_showroom, chave_ano
                ) AS sfm_agg
            ON ssm_agg.tipo_evento = sfm_agg.tipo_showroom
            AND ssm_agg.chave_ano = sfm_agg.chave_ano
        ) AS CombinedData
    ORDER BY
        CombinedData.Ano_Evento ASC, CombinedData.Evento_Showroom ASC;
END;