CREATE DEFINER=`root`@`localhost` PROCEDURE `Analise_Mix_Cliente`(
    IN p_codigo_parceiro BIGINT,
    IN p_ano INT, -- Novo parâmetro para o ano
    IN p_mes_1 VARCHAR(255),
    IN p_mes_2 VARCHAR(255)
)
BEGIN
    WITH MonthlyMixData AS (
        SELECT
            smpv.codigo_parceiro_item,
            smpv.nome_parceiro_item,
            smpv.chave_mes,
            smpv.macrogrupo_produto,
            smpv.quantidade_total_item,
            smpv.valor_total_item
        FROM
            faturamento_multimarcas_dw.staging_mix_produtos_vendidos smpv
        WHERE
            smpv.codigo_parceiro_item = p_codigo_parceiro
            AND smpv.chave_ano = p_ano -- Adicionado filtro por ano
            AND smpv.chave_mes IN (p_mes_1, p_mes_2)
            AND smpv.macrogrupo_produto IN ('BOLSAS', 'SAPATOS')
            AND smpv.quantidade_total_item > 0
    )
    SELECT
        MMD.codigo_parceiro_item AS `Cod. Parceiro`,
        MMD.nome_parceiro_item AS `Nome Parceiro`,
        MMD.chave_mes AS `Mês`, -- Coluna para indicar o mês da linha

        SUM(CASE WHEN MMD.macrogrupo_produto = 'SAPATOS' THEN MMD.quantidade_total_item ELSE 0 END) AS QTD_Sapatos,
        SUM(CASE WHEN MMD.macrogrupo_produto = 'BOLSAS' THEN MMD.quantidade_total_item ELSE 0 END) AS QTD_Bolsas,
        
        -- Nova coluna: Valor Total do Pedido (somando valor_total_item para Bolsas e Sapatos)
        CONCAT('R$ ', FORMAT(SUM(CASE WHEN MMD.macrogrupo_produto IN ('BOLSAS', 'SAPATOS') THEN MMD.valor_total_item ELSE 0 END), 2, 'de_DE')) AS `Valor Total do Pedido`,
        
        CONCAT(FORMAT(
            COALESCE(
                (SUM(CASE WHEN MMD.macrogrupo_produto = 'BOLSAS' THEN MMD.quantidade_total_item ELSE 0 END) /
                 NULLIF(SUM(CASE WHEN MMD.macrogrupo_produto IN ('BOLSAS', 'SAPATOS') THEN MMD.quantidade_total_item ELSE 0 END), 0)) * 100,
                0
            ), 2
        ), '%') AS `% Bolsas / Sapatos`,
        
        CONCAT(FORMAT(
            COALESCE(
                (SUM(CASE WHEN MMD.macrogrupo_produto = 'SAPATOS' THEN MMD.quantidade_total_item ELSE 0 END) /
                 NULLIF(SUM(CASE WHEN MMD.macrogrupo_produto IN ('BOLSAS', 'SAPATOS') THEN MMD.quantidade_total_item ELSE 0 END), 0)) * 100,
                0
            ), 2
        ), '%') AS `% Sapatos / Bolsas`

    FROM
        MonthlyMixData MMD
    GROUP BY
        MMD.codigo_parceiro_item,
        MMD.nome_parceiro_item,
        MMD.chave_mes
    ORDER BY
        MMD.codigo_parceiro_item,
        FIELD(MMD.chave_mes, p_mes_1, p_mes_2);
END;