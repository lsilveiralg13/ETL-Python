CREATE DEFINER=`root`@`localhost` PROCEDURE `PainelSDR Gerencial`(
    IN p_mes VARCHAR(20),
    IN p_ano INT
)
BEGIN
    SELECT
        Ano,
        Mes,
        NomeSDR,
        MetaPedidos,
        QtdCadastros,
        `Pri.Pedido`,
        VLMReal,
        `%Conversão`,
        CONCAT('R$ ', 
            FORMAT(
                CASE 
                    WHEN PrimPedidoNum BETWEEN 0 AND 2 THEN 0.00
                    WHEN PrimPedidoNum BETWEEN 3 AND 7 THEN PrimPedidoNum * 80.00
                    WHEN PrimPedidoNum BETWEEN 8 AND 11 THEN PrimPedidoNum * 110.00
                    WHEN PrimPedidoNum BETWEEN 12 AND 14 THEN PrimPedidoNum * 130.00
                    WHEN PrimPedidoNum >= 15 THEN PrimPedidoNum * 140.00
                    ELSE 0.00
                END
            , 2, 'de_DE')
        ) AS Premio,
        CONCAT('R$ ', 
            FORMAT(
                (CASE 
                    WHEN PrimPedidoNum BETWEEN 0 AND 2 THEN 0.00
                    WHEN PrimPedidoNum BETWEEN 3 AND 7 THEN PrimPedidoNum * 80.00
                    WHEN PrimPedidoNum BETWEEN 8 AND 11 THEN PrimPedidoNum * 110.00
                    WHEN PrimPedidoNum BETWEEN 12 AND 14 THEN PrimPedidoNum * 130.00
                    WHEN PrimPedidoNum >= 15 THEN PrimPedidoNum * 140.00
                    ELSE 0.00
                END) + 2290.00
            , 2, 'de_DE')
        ) AS `Premio + Salario`
    FROM
        (
            SELECT
                COALESCE(scc.chave_aaa, spp.chave_ano, msdr.chave_ano) AS Ano,
                COALESCE(scc.chave_mmm, spp.chave_mes, msdr.chave_mes) AS Mes,
                COALESCE(scc.nome_sdr, spp.nome_sdr, msdr.nome_sdr) AS NomeSDR,
                COALESCE(scc.qtd_clientes_cadastrados, 0) AS QtdCadastros,
                FORMAT(COALESCE(msdr.Meta_Nv_Loja_Por_SDR, 0), 0) AS MetaPedidos,
                COALESCE(spp.qtd_novas_lojas_primeiro_pedido_premio, 0) AS `Pri.Pedido`,
                COALESCE(spp.qtd_novas_lojas_primeiro_pedido_premio, 0) AS PrimPedidoNum,
                CONCAT('R$ ', FORMAT(COALESCE(spp.valor_medio_primeiro_pedido, 0.00), 2, 'de_DE')) AS VLMReal,
                CONCAT(
                    FORMAT(
                        COALESCE(
                            (COALESCE(spp.qtd_novas_lojas_primeiro_pedido_premio, 0) / NULLIF(COALESCE(scc.qtd_clientes_cadastrados, 0), 0)) * 100,
                            0.00
                        ), 2
                    ), '%'
                ) AS `%Conversão`
            FROM
                (
                    SELECT
                        chave_aaa,
                        chave_mmm,
                        nome_sdr,
                        COUNT(DISTINCT codigo_parceiro) AS qtd_clientes_cadastrados
                    FROM
                        staging_cadastro_clientes
                    WHERE
                        nome_sdr IS NOT NULL AND nome_sdr != ''
                        AND chave_mmm = p_mes  
                        AND chave_aaa = p_ano  
                        AND nome_sdr NOT IN ('GLENDASOUZA', 'ERIKHA', 'LUCIANAPEREIRA', 'JOSIANEVIEIRA', 'ISABELLASILVA', 'NELIANE', 'MARCELAVAZ')
                    GROUP BY
                        chave_aaa, chave_mmm, nome_sdr
                ) AS scc
            LEFT JOIN
                (
                    SELECT
                        chave_ano,
                        chave_mes,
                        nome_sdr,
                        SUM(premio) AS qtd_novas_lojas_primeiro_pedido_premio,
                        SUM(valor_faturado) / COUNT(DISTINCT numero_pedido) AS valor_medio_primeiro_pedido
                    FROM
                        staging_primeiro_pedido
                    WHERE
                        nome_sdr IS NOT NULL AND nome_sdr != ''
                        AND YEAR(data_faturamento) = p_ano
                        AND MONTH(data_faturamento) = (
                            CASE LOWER(p_mes)
                                WHEN 'janeiro' THEN 1
                                WHEN 'fevereiro' THEN 2
                                WHEN 'março' THEN 3
                                WHEN 'abril' THEN 4
                                WHEN 'maio' THEN 5
                                WHEN 'junho' THEN 6
                                WHEN 'julho' THEN 7
                                WHEN 'agosto' THEN 8
                                WHEN 'setembro' THEN 9
                                WHEN 'outubro' THEN 10
                                WHEN 'novembro' THEN 11
                                WHEN 'dezembro' THEN 12
                                ELSE 0 
                            END
                        )
                        AND nome_sdr NOT IN ('GLENDASOUZA', 'ERIKHA', 'LUCIANAPEREIRA', 'JOSIANEVIEIRA', 'ISABELLASILVA', 'NELIANE', 'MARCELAVAZ')
                    GROUP BY
                        chave_ano, chave_mes, nome_sdr
                ) AS spp ON scc.chave_aaa = spp.chave_ano
                            AND scc.chave_mmm = spp.chave_mes
                            AND scc.nome_sdr = spp.nome_sdr
            LEFT JOIN
                metas_sdrs AS msdr ON COALESCE(scc.chave_aaa, spp.chave_ano) = msdr.chave_ano
                                    AND COALESCE(scc.chave_mmm, spp.chave_mes) = msdr.chave_mes
                                    AND COALESCE(scc.nome_sdr, spp.nome_sdr) = msdr.nome_sdr
                                    AND msdr.nome_sdr NOT IN ('GLENDASOUZA', 'ERIKHA', 'LUCIANAPEREIRA', 'JOSIANEVIEIRA', 'ISABELLASILVA', 'NELIANE', 'MARCELAVAZ')
        ) AS DadosSDR
    ORDER BY
        `Pri.Pedido` DESC, Ano DESC, Mes, NomeSDR;
END;