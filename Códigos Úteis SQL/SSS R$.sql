SELECT
    CONCAT('R$ ', FORMAT(SUM(sfm.valor_faturado), 2, 'de_DE')) AS FaturamentoBaseAtivos_2024,
    (
        SELECT
            COUNT(DISTINCT scc.codigo_parceiro)
        FROM
            staging_cadastro_clientes scc
        WHERE
            scc.status_cliente IN ('Loja Ativa', 'Loja Bloqueada')
            AND scc.data_cadastro_cliente <= '2024-07-31'
    ) AS TotalClientesBaseAtivos_2024
FROM
    faturamento_multimarcas_dw.staging_faturamento_multimarcas sfm
INNER JOIN (
    SELECT DISTINCT scc.codigo_parceiro
    FROM staging_cadastro_clientes scc
    WHERE scc.status_cliente IN ('Loja Ativa', 'Loja Bloqueada')
      AND scc.data_cadastro_cliente <= '2024-07-31'
) AS ClientesBaseAtivos ON sfm.codigo_parceiro = ClientesBaseAtivos.codigo_parceiro
WHERE
    sfm.chave_ano = 2024
    AND sfm.chave_mes = 'JULHO';