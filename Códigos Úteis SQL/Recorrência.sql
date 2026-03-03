CREATE DEFINER=`root`@`localhost` PROCEDURE `Recorrência_Periodo_Sem_Compra`(IN p_faixa_dias VARCHAR(20))
BEGIN
    SELECT
        src.codigo_parceiro AS "Cod",
        src.nome_parceiro AS "NOME PARCEIRO",
        src.status_lojista AS "STATUS LOJISTA",
        src.cnpj AS "CNPJ",
        src.apelido_vendedor AS "VENDEDOR",
        src.dias_sem_compra AS "DIAS S/ COMPRA",
        src.qtde_pedidos_90_dias AS "QTDE PEDIDOS",
        -- Nova coluna: Soma do faturamento dos últimos 3 meses
        COALESCE(SUM(sfm.valor_faturado), 0.00) AS "FATURAMENTO 3 MESES",
        src.data_ultima_compra AS "ÚLTIMA COMPRA",
        src.data_cadastro AS "CADASTRO",
        -- Colunas adicionadas para a análise de inadimplência e limite de crédito
        sim.valor_liquido AS "LIMITE DISPONÍVEL",
        sfm.limite_credito_disponivel AS "VALOR INADIMPLÊNCIA"
    FROM
        staging_recorrencia_clientes src
    LEFT JOIN
        staging_faturamento_multimarcas sfm ON src.codigo_parceiro = sfm.codigo_parceiro
                                              AND sfm.data_negociacao >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
    LEFT JOIN
        staging_inadimplencia_multimarcas sim ON src.codigo_parceiro = sim.codigo_parceiro
    LEFT JOIN
        staging_financeiro_multimarcas sfm ON src.codigo_parceiro = sfm.codigo_parceiro
    WHERE
        (src.qtde_pedidos_90_dias = 0 OR src.qtde_pedidos_90_dias IS NULL)
        AND src.apelido_vendedor NOT IN ('<SEM VENDEDOR>', 'ALEX SANDRO')
        AND src.status_lojista = 'Loja Ativa'
        -- NOVO FILTRO 1: Exclui clientes com valor líquido pendente (inadimplentes)
        AND (sim.valor_liquido IS NULL OR sim.valor_liquido <= 0)
        -- NOVO FILTRO 2: Exclui clientes com limite de crédito disponível <= 0
        AND (sfm.limite_credito_disponivel IS NULL OR sfm.limite_credito_disponivel > 0)
        -- Seleção pela faixa de dias sem compra
        AND (
            CASE p_faixa_dias
                WHEN '30 dias' THEN src.dias_sem_compra <= 30
                WHEN '45 dias' THEN src.dias_sem_compra > 30 AND src.dias_sem_compra <= 45
                WHEN '60 dias' THEN src.dias_sem_compra > 45 AND src.dias_sem_compra <= 60
                WHEN '90 dias' THEN src.dias_sem_compra > 60 AND src.dias_sem_compra <= 90
                WHEN 'acima de 90 dias' THEN src.dias_sem_compra > 90
                ELSE FALSE -- Retorna falso para qualquer valor inválido de p_faixa_dias
            END
        )
    GROUP BY
        src.codigo_parceiro,
        src.nome_parceiro,
        src.status_lojista,
        src.cnpj,
        src.apelido_vendedor,
        src.dias_sem_compra,
        src.qtde_pedidos_90_dias,
        src.data_ultima_compra,
        src.data_cadastro
    HAVING
        COALESCE(SUM(sfm.valor_faturado), 0.00) = 0.00
    ORDER BY
        src.apelido_vendedor, src.dias_sem_compra DESC;

END