CREATE DEFINER=`root`@`localhost` PROCEDURE `Atualizar Base de Ativos`()
BEGIN
    -- 1. LIMPAR A TABELA DE DESTINO ANTES DE INSERIR NOVOS DADOS
    -- Isso evita o erro de Chave Primária Duplicada (Error Code 1062)
    TRUNCATE TABLE dim_status_cliente;

    -- 2. INSERÇÃO DOS DADOS (Totais Individuais + Total Geral Filtrado)
    INSERT INTO dim_status_cliente (
        `Apelido (Vendedor)`,
        `Lojas Ativas`,
        `Lojas Bloqueadas`,
        `Lojas Inativas`,
        `Saldo de Lojas`
    )

    -- PARTE 1: TOTAIS INDIVIDUAIS POR VENDEDOR
    SELECT
        t.`Apelido (Vendedor)`,
        SUM(CASE WHEN t.STATUS = 'Loja Ativa' THEN 1 ELSE 0 END) AS `Lojas Ativas`,
        SUM(CASE WHEN t.STATUS = 'Loja Bloqueada' THEN 1 ELSE 0 END) AS `Lojas Bloqueadas`,
        SUM(CASE WHEN t.STATUS = 'Loja Inativa' THEN 1 ELSE 0 END) AS `Lojas Inativas`,
        SUM(CASE WHEN t.STATUS IN ('Loja Ativa', 'Loja Bloqueada') THEN 1 ELSE 0 END) AS `Saldo de Lojas`
    FROM
        dim_cadastro AS t
    WHERE
        t.`Apelido (Vendedor)` <> 'ALEX SANDRO'
        AND t.`Apelido (Vendedor)` <> '<SEM VENDEDOR>'
    GROUP BY
        t.`Apelido (Vendedor)`

    UNION ALL

    -- PARTE 2: TOTAL GERAL FILTRADO (Linha única)
    SELECT
        'TOTAL GERAL', -- Valor constante para a coluna de Apelido (Vendedor)
        SUM(CASE WHEN t.STATUS = 'Loja Ativa' THEN 1 ELSE 0 END),
        SUM(CASE WHEN t.STATUS = 'Loja Bloqueada' THEN 1 ELSE 0 END),
        SUM(CASE WHEN t.STATUS = 'Loja Inativa' THEN 1 ELSE 0 END),
        SUM(CASE WHEN t.STATUS IN ('Loja Ativa', 'Loja Bloqueada') THEN 1 ELSE 0 END)
    FROM
        dim_cadastro AS t
    WHERE
        t.`Apelido (Vendedor)` <> 'ALEX SANDRO'
        AND t.`Apelido (Vendedor)` <> '<SEM VENDEDOR>';
        
END;