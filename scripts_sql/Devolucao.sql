CREATE DEFINER=`root`@`localhost` PROCEDURE `Devolucao`(IN p_mes VARCHAR(20), IN p_ano INT)
BEGIN
    -- Consulta consolidada por vendedor com ofensores individuais
    SELECT
        vendedor AS Vendedor,
        -- Converte para o formato de moeda de_DE (ponto no milhar, vírgula no decimal)
        CONCAT('R$ ', FORMAT(SUM(valor_faturado), 2, 'de_DE')) AS `Valor(R$)`,
        COUNT(vendedor) AS Qtde,
        p_mes AS chave_mmm,
        p_ano AS chave_aaa,
        -- Busca o motivo mais frequente ESPECÍFICO deste vendedor
        (SELECT motivo 
         FROM staging_devolucoes_multimarcas s2 
         WHERE s2.vendedor = s1.vendedor 
           AND s2.chave_mmm = p_mes 
           AND s2.chave_aaa = p_ano 
         GROUP BY motivo 
         ORDER BY COUNT(*) DESC 
         LIMIT 1) AS Motivo_Ofensor,
        -- Busca o cliente que mais gerou devolução ESPECÍFICO deste vendedor
        (SELECT nome_parceiro 
         FROM staging_devolucoes_multimarcas s3 
         WHERE s3.vendedor = s1.vendedor 
           AND s3.chave_mmm = p_mes 
           AND s3.chave_aaa = p_ano 
         GROUP BY nome_parceiro 
         ORDER BY COUNT(*) DESC 
         LIMIT 1) AS Cliente_Ofensor
    FROM
        staging_devolucoes_multimarcas s1
    WHERE 
        chave_mmm = p_mes AND chave_aaa = p_ano
    GROUP BY 
        vendedor
	ORDER BY
		Qtde DESC;
    
END;