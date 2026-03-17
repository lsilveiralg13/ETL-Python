CREATE DEFINER=`root`@`localhost` PROCEDURE `Faturamento`(
    IN p_mes_nome VARCHAR(20),     -- ex: 'NOVEMBRO'
    IN p_ano      INT              -- ex: 2025
)
BEGIN
    DECLARE v_mes_num TINYINT;

    -- normaliza o mês para maiúsculo
    SET p_mes_nome = UPPER(p_mes_nome);

    -- converte nome do mês em número (1–12)
    SET v_mes_num = CASE p_mes_nome
        WHEN 'JANEIRO'   THEN 1
        WHEN 'FEVEREIRO' THEN 2
        WHEN 'MARÇO'     THEN 3
        WHEN 'MARCO'     THEN 3
        WHEN 'ABRIL'     THEN 4
        WHEN 'MAIO'      THEN 5
        WHEN 'JUNHO'     THEN 6
        WHEN 'JULHO'     THEN 7
        WHEN 'AGOSTO'    THEN 8
        WHEN 'SETEMBRO'  THEN 9
        WHEN 'OUTUBRO'   THEN 10
        WHEN 'NOVEMBRO'  THEN 11
        WHEN 'DEZEMBRO'  THEN 12
        ELSE NULL
    END;

    IF v_mes_num IS NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Nome de mês inválido. Use JANEIRO, FEVEREIRO, ... DEZEMBRO.';
    END IF;

    /*
      v = vendas por vendedor no mês/ano escolhido
      cad = carteira (ativos + bloqueados) por vendedor
      m = metas do mês/ano
    */
    
    -- NOVO SELECT COM ROLLUP PARA TOTAIS GERAIS
    SELECT
        -- Nome Vendedor: Usa 'SOMA' se for a linha de total
        IFNULL(Resumo.Vendedor, 'SOMA') AS `Nome Vendedor`,

        -- META por Inside Sales (SOMA)
        REPLACE(
            REPLACE(
                REPLACE(FORMAT(SUM(Resumo.Meta_IS), 2), ',', 'X'), -- Usa o campo numérico Meta_IS
                '.',
                ','
            ),
            'X',
            '.'
        ) AS `Meta por I.S`,

        -- FATURAMENTO (SOMA)
        REPLACE(
            REPLACE(
                REPLACE(FORMAT(SUM(Resumo.Faturamento), 2), ',', 'X'), -- Usa o campo numérico Faturamento
                '.',
                ','
            ),
            'X',
            '.'
        ) AS `Faturamento`,

        -- ATINGIMENTO (%) - Recalculado (Soma Faturamento / Soma Meta)
        CONCAT(
            REPLACE(
                REPLACE(
                    REPLACE(
                        FORMAT(
                            (SUM(Resumo.Faturamento) / NULLIF(SUM(Resumo.Meta_IS), 0)) * 100,
                            2
                        ),
                        ',',
                        'X'
                    ),
                    '.',
                    ','
                ),
                'X',
                '.'
            ),
            ' %'
        ) AS `Atingimento (%)`,

        -- CONVERSÕES (SOMA)
        SUM(Resumo.Convertidos) AS `Conversões`,

        -- BASE DE ATIVOS (SOMA)
        SUM(Resumo.Carteira) AS `Base de Ativos`,

        -- TAXA DE CONVERSÃO (%) - Recalculada (Soma Convertidos / Soma Base)
        CONCAT(
            REPLACE(
                REPLACE(
                    REPLACE(
                        FORMAT(
                            (SUM(Resumo.Convertidos) / NULLIF(SUM(Resumo.Carteira), 0)) * 100,
                            2
                        ),
                        ',',
                        'X'
                    ),
                    '.',
                    ','
                ),
                'X',
                '.'
            ),
            ' %'
        ) AS `Taxa de Conversão (%)`,
        
        -- TICKET MÉDIO - Recalculado (Soma Faturamento / Soma Convertidos)
        REPLACE(
            REPLACE(
                REPLACE(
                    FORMAT(
                        SUM(Resumo.Faturamento) / NULLIF(SUM(Resumo.Convertidos), 0),
                        2
                    ),
                    ',',
                    'X'
                ),
                '.',
                ','
            ),
            'X',
            '.'
        ) AS `Ticket Médio`
        
    FROM (
        -- SELECT ORIGINAL (COM NOMES DE COLUNAS SIMPLES E SEM FORMATAÇÃO PT-BR)
        SELECT
            v.Vendedor AS Vendedor, -- Nome simples para referência no SELECT externo
            m.`Meta por Inside Sales` AS Meta_IS, -- Nome simples para referência
            v.Faturamento,
            v.Convertidos,
            cad.Carteira
        FROM (
            -- Faturamento e convertidos por vendedor no mês
            SELECT
                COALESCE(f.`Apelido (Vendedor)`, f.`Vendedor`) AS Vendedor,
                SUM(f.`Vlr. Nota`)                                 AS Faturamento,
                COUNT(DISTINCT f.`Parceiro`)                       AS Convertidos
            FROM fato_base_de_dados AS f
            WHERE
                f.`Chave_Ano` = p_ano
                AND UPPER(f.`Chave_Mes`) = p_mes_nome
            GROUP BY
                COALESCE(f.`Apelido (Vendedor)`, f.`Vendedor`)
        ) AS v
        LEFT JOIN (
            -- Carteira: clientes ATIVO + BLOQUEADO por vendedor
            SELECT
                c.`Apelido (Vendedor)`      AS Vendedor,
                COUNT(DISTINCT c.`Cód. Parceiro`) AS Carteira
            FROM dim_cadastro AS c
            WHERE UPPER(c.`Status`) IN ('Loja Ativa','Loja Bloqueada')
            GROUP BY
                c.`Apelido (Vendedor)`
        ) AS cad
            ON cad.Vendedor = v.Vendedor
        LEFT JOIN dim_metas AS m
            ON m.Ano = p_ano
           AND m.Mes = v_mes_num
    ) AS Resumo
    
    GROUP BY
        Resumo.Vendedor WITH ROLLUP -- Aplica ROLLUP ao nome simples da coluna
    
    ORDER BY
        FIELD(IFNULL(Resumo.Vendedor, 'SOMA'), 'SOMA'), -- Garante que 'SOMA' fique no final
        SUM(Resumo.Faturamento) DESC;
        
END;