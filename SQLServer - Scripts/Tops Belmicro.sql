SELECT DISTINCT
    FO.CodTipoOperacao,
    FO.DescricaoTipoOperacao
FROM belmicro.fato_operacoes FO WITH (NOLOCK)
WHERE 
    -- Filtro focado em trazer o universo de Devoluções, Cancelamentos e Estornos
    FO.DescricaoTipoOperacao LIKE '%DEV%'
    OR FO.DescricaoTipoOperacao LIKE '%CANCEL%'
    OR FO.DescricaoTipoOperacao LIKE '%ESTORNO%'
    OR FO.StatusNota = 'C'
    OR FO.StatusNFe LIKE '%Cancelada%'
    OR FO.StatusNFe LIKE '%Cancelamento%'
    OR FO.StatusNFe LIKE '%Devolução%'
ORDER BY FO.CodTipoOperacao ASC;