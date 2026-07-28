CREATE DEFINER=`root`@`localhost` PROCEDURE `Devolucao_%`(
    IN p_mes VARCHAR(20),
    IN p_ano INT
)
BEGIN
    -- 1. CTE para processar os números puros
    WITH BaseCalculo AS (
        SELECT
            motivo,
            SUM(valor_faturado) AS Valor_Numerico,
            COUNT(*) AS Qtde_Motivo
        FROM
            staging_devolucoes_multimarcas
        WHERE
            chave_mmm = p_mes
            AND chave_aaa = p_ano
        GROUP BY motivo
    ),
    TotalGeral AS (
        SELECT 
            SUM(Valor_Numerico) AS Soma_Total,
            SUM(Qtde_Motivo) AS Itens_Total 
        FROM BaseCalculo
    )

    -- 2. Seleção dos Motivos (as colunas de ordem ficam escondidas aqui)
    SELECT Motivo, `Valor(R$) devolvido`, Qtde, `% Repres.` FROM (
        SELECT
            IFNULL(motivo, 'Não Informado') AS Motivo,
            CONCAT('R$ ', FORMAT(Valor_Numerico, 2, 'de_DE')) AS `Valor(R$) devolvido`,
            Qtde_Motivo AS Qtde,
            CONCAT(FORMAT((Qtde_Motivo * 100.0 / (SELECT Itens_Total FROM TotalGeral)), 2, 'de_DE'), '%') AS `% Repres.`,
            Valor_Numerico AS ord_valor,
            Qtde_Motivo AS ord_qtde
        FROM BaseCalculo
        ORDER BY ord_valor DESC, ord_qtde DESC
    ) AS ParteSuperior

    UNION ALL

    -- 3. Linha do Total Geral
    SELECT
        'TOTAL GERAL' AS Motivo,
        CONCAT('R$ ', FORMAT(Soma_Total, 2, 'de_DE')) AS `Valor(R$) devolvido`,
        Itens_Total AS Qtde,
        '100,00%' AS `% Repres.`
    FROM TotalGeral;

END;