CREATE DEFINER=`root`@`localhost` PROCEDURE `Analise_Faturamento_Detalhado_Showroom`()
BEGIN
    WITH FaturamentoPorShowroomTipo AS (
        SELECT
            sf.codigo_parceiro,
            sf.nome_parceiro,
            sf.tipo_showroom,
            SUM(sf.valor_faturado) AS Faturamento_Tipo_Showroom
        FROM
            faturamento_multimarcas_dw.staging_faturamento_multimarcas sf
        WHERE
            sf.valor_faturado IS NOT NULL AND sf.valor_faturado > 0
        GROUP BY
            sf.codigo_parceiro,
            sf.nome_parceiro,
            sf.tipo_showroom
    )
    SELECT
        FPST.codigo_parceiro,
        FPST.nome_parceiro,
        CONCAT('R$ ', FORMAT(SUM(CASE WHEN FPST.tipo_showroom = 'INVERNO 2024' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END), 2, 'de_DE')) AS Faturado_INV24,
        CONCAT('R$ ', FORMAT(SUM(CASE WHEN FPST.tipo_showroom = 'VERÃO 2025' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END), 2, 'de_DE')) AS Faturado_VR25,
        CONCAT('R$ ', FORMAT(SUM(CASE WHEN FPST.tipo_showroom = 'INVERNO 2025' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END), 2, 'de_DE')) AS Faturado_INV25,
        CONCAT('R$ ', FORMAT(SUM(CASE WHEN FPST.tipo_showroom = 'VERÃO 2026' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END), 2, 'de_DE')) AS Faturado_VR26,
        -- 1. Inclusão do INVERNO 2026
        CONCAT('R$ ', FORMAT(SUM(CASE WHEN FPST.tipo_showroom = 'INVERNO 2026' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END), 2, 'de_DE')) AS Faturado_INV26,
        
        CONCAT('R$ ', FORMAT(
            (
                SUM(CASE WHEN FPST.tipo_showroom IN ('INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026', 'INVERNO 2026') THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END)
            ), 2, 'de_DE'
        )) AS Total_Tudo,
        
        (
            (CASE WHEN SUM(CASE WHEN FPST.tipo_showroom = 'INVERNO 2024' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN SUM(CASE WHEN FPST.tipo_showroom = 'VERÃO 2025' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN SUM(CASE WHEN FPST.tipo_showroom = 'INVERNO 2025' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN SUM(CASE WHEN FPST.tipo_showroom = 'VERÃO 2026' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN SUM(CASE WHEN FPST.tipo_showroom = 'INVERNO 2026' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) > 0 THEN 1 ELSE 0 END)
        ) AS Participacoes,

        CONCAT('R$ ', FORMAT(
            (
                SUM(CASE WHEN FPST.tipo_showroom IN ('INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026', 'INVERNO 2026') THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END)
                / NULLIF(
                    (
                        (CASE WHEN SUM(CASE WHEN FPST.tipo_showroom = 'INVERNO 2024' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) > 0 THEN 1 ELSE 0 END) +
                        (CASE WHEN SUM(CASE WHEN FPST.tipo_showroom = 'VERÃO 2025' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) > 0 THEN 1 ELSE 0 END) +
                        (CASE WHEN SUM(CASE WHEN FPST.tipo_showroom = 'INVERNO 2025' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) > 0 THEN 1 ELSE 0 END) +
                        (CASE WHEN SUM(CASE WHEN FPST.tipo_showroom = 'VERÃO 2026' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) > 0 THEN 1 ELSE 0 END) +
                        (CASE WHEN SUM(CASE WHEN FPST.tipo_showroom = 'INVERNO 2026' THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) > 0 THEN 1 ELSE 0 END)
                    )
                , 0)
            ), 2, 'de_DE'
        )) AS VLM,
        
        CASE
            WHEN SUM(CASE WHEN FPST.tipo_showroom IN ('INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026', 'INVERNO 2026') THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) >= 75000.00 THEN 'Saudável (Alto Faturamento)'
            WHEN SUM(CASE WHEN FPST.tipo_showroom IN ('INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026', 'INVERNO 2026') THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) >= 30000.00 THEN 'Em Crescimento (Médio Faturamento)'
            ELSE 'A Observar (Baixo Faturamento)'
        END AS Status_Cliente
    FROM
        FaturamentoPorShowroomTipo FPST
    GROUP BY
        FPST.codigo_parceiro,
        FPST.nome_parceiro
    HAVING
        SUM(CASE WHEN FPST.tipo_showroom IN ('INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026', 'INVERNO 2026') THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) > 0
    ORDER BY
        -- 2. Ordenação do maior faturamento total para o menor
        SUM(CASE WHEN FPST.tipo_showroom IN ('INVERNO 2024', 'VERÃO 2025', 'INVERNO 2025', 'VERÃO 2026', 'INVERNO 2026') THEN FPST.Faturamento_Tipo_Showroom ELSE 0 END) DESC,
        FPST.nome_parceiro ASC;
END;