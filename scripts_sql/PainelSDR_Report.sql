CREATE DEFINER=`root`@`localhost` PROCEDURE `PainelSDR Report`(
    IN p_mes VARCHAR(20),
    IN p_ano INT
)
BEGIN
    
    WITH DadosSDRAgregados AS (
        SELECT
            COALESCE(scc.chave_aaa, spp.chave_ano, msdr.chave_ano) AS Ano,
            COALESCE(scc.chave_mmm, spp.chave_mes, msdr.chave_mes) AS Mes,
            COALESCE(scc.nome_sdr, spp.nome_sdr, msdr.nome_sdr) AS NomeSDR,
            COALESCE(scc.qtd_clientes_cadastrados, 0) AS QtdCadastrosNum,
            COALESCE(spp.qtd_novas_lojas_primeiro_pedido_premio, 0) AS PrimPedidoNum,
            COALESCE(spp.total_faturado_sdr, 0.00) AS TotalVendidoNum,
            
            CONCAT('R$ ', FORMAT(COALESCE(spp.total_faturado_sdr, 0.00) / NULLIF(COALESCE(spp.qtd_novas_lojas_primeiro_pedido_premio, 0), 0), 2, 'de_DE')) AS VLMRealFormatado,
            CONCAT('R$ ', FORMAT(COALESCE(spp.total_faturado_sdr, 0.00), 2, 'de_DE')) AS TotalVendidoFormatado,
            CONCAT(
                FORMAT(
                    COALESCE(
                        (COALESCE(spp.qtd_novas_lojas_primeiro_pedido_premio, 0) / NULLIF(COALESCE(scc.qtd_clientes_cadastrados, 0), 0)) * 100,
                        0.00
                    ), 2
                ), '%'
            ) AS ConversaoFormatada
            
        FROM
            (
                SELECT
                    chave_aaa, chave_mmm, nome_sdr, COUNT(DISTINCT codigo_parceiro) AS qtd_clientes_cadastrados
                FROM staging_cadastro_clientes
                WHERE nome_sdr IS NOT NULL AND nome_sdr != '' AND chave_mmm = p_mes AND chave_aaa = p_ano
                AND nome_sdr NOT IN ('GLENDASOUZA', 'ERIKHA', 'LUCIANAPEREIRA', 'JOSIANEVIEIRA', 'ISABELLASILVA', 'NELIANE', 'MARCELAVAZ', 'ALEX SANDRO')
                GROUP BY chave_aaa, chave_mmm, nome_sdr
            ) AS scc
        LEFT JOIN
            (
                SELECT
                    chave_ano, chave_mes, nome_sdr, 
                    SUM(premio) AS qtd_novas_lojas_primeiro_pedido_premio,
                    SUM(valor_faturado) AS total_faturado_sdr
                FROM staging_primeiro_pedido
                WHERE nome_sdr IS NOT NULL AND nome_sdr != ''
                AND YEAR(data_faturamento) = p_ano
                AND MONTH(data_faturamento) = (
                    CASE LOWER(p_mes)
                        WHEN 'janeiro' THEN 1 WHEN 'fevereiro' THEN 2 WHEN 'março' THEN 3 WHEN 'abril' THEN 4
                        WHEN 'maio' THEN 5 WHEN 'junho' THEN 6 WHEN 'julho' THEN 7 WHEN 'agosto' THEN 8
                        WHEN 'setembro' THEN 9 WHEN 'outubro' THEN 10 WHEN 'novembro' THEN 11 WHEN 'dezembro' THEN 12
                        ELSE 0 END
                )
                AND nome_sdr NOT IN ('GLENDASOUZA', 'ERIKHA', 'LUCIANAPEREIRA', 'JOSIANEVIEIRA', 'ISABELLASILVA', 'NELIANE', 'MARCELAVAZ', 'ALEX SANDRO')
                GROUP BY chave_ano, chave_mes, nome_sdr
            ) AS spp 
            ON scc.chave_aaa = spp.chave_ano AND scc.chave_mmm = spp.chave_mes AND scc.nome_sdr = spp.nome_sdr
        LEFT JOIN
            metas_sdrs AS msdr 
            ON COALESCE(scc.chave_aaa, spp.chave_ano) = msdr.chave_ano
            AND COALESCE(scc.chave_mmm, spp.chave_mes) = msdr.chave_mes
            AND COALESCE(scc.nome_sdr, spp.nome_sdr) = msdr.nome_sdr
            AND msdr.nome_sdr NOT IN ('GLENDASOUZA', 'ERIKHA', 'LUCIANAPEREIRA', 'JOSIANEVIEIRA', 'ISABELLASILVA', 'NELIANE', 'MARCELAVAZ', 'ALEX SANDRO')
    )

    -- SELECT EXTERNO para ocultar colunas de ordenação
    SELECT 
        RelatorioFinal.Ano,
        RelatorioFinal.Mes,
        RelatorioFinal.NomeSDR,
        RelatorioFinal.QtdCadastros,
        RelatorioFinal.`Primeiro Pedido`,
        RelatorioFinal.TotalVendido,
        RelatorioFinal.VLMReal,
        RelatorioFinal.`%Conversão`
    FROM
    (
        -- PARTE 1: DETALHE POR SDR
        SELECT
            DadosSDR.Ano,
            DadosSDR.Mes,
            DadosSDR.NomeSDR,
            FORMAT(DadosSDR.QtdCadastrosNum, 0) AS QtdCadastros,
            FORMAT(DadosSDR.PrimPedidoNum, 0) AS `Primeiro Pedido`,
            DadosSDR.TotalVendidoFormatado AS TotalVendido,
            DadosSDR.VLMRealFormatado AS VLMReal,
            DadosSDR.ConversaoFormatada AS `%Conversão`,
            DadosSDR.PrimPedidoNum AS PrimPedidoNumSort, 
            0 AS OrdemFiltro
        FROM
            DadosSDRAgregados AS DadosSDR

        UNION ALL

        -- PARTE 2: LINHA DE TOTAL GERAL
        SELECT
            'TOTAL GERAL' AS Ano,
            '---' AS Mes,
            '---' AS NomeSDR,
            
            FORMAT(SUM(QtdCadastrosNum), 0) AS QtdCadastros,
            
            FORMAT(SUM(PrimPedidoNum), 0) AS `Primeiro Pedido`,
            
            CONCAT('R$ ', FORMAT(SUM(TotalVendidoNum), 2, 'de_DE')) AS TotalVendido,
            
            CONCAT('R$ ', FORMAT(SUM(TotalVendidoNum) / NULLIF(SUM(PrimPedidoNum), 0), 2, 'de_DE')) AS VLMReal,
            
            CONCAT(
                FORMAT(
                    (SUM(PrimPedidoNum) / NULLIF(SUM(QtdCadastrosNum), 0)) * 100
                , 2), '%'
            ) AS `%Conversão`,
            
            SUM(PrimPedidoNum) AS PrimPedidoNumSort, 
            1 AS OrdemFiltro
        FROM
            DadosSDRAgregados
    ) AS RelatorioFinal

    ORDER BY
        RelatorioFinal.OrdemFiltro,
        RelatorioFinal.PrimPedidoNumSort DESC;
END;