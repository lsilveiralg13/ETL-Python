WITH CTE_DADOS AS (
    SELECT 
        e.NAME AS empresa,
        SUM(CASE WHEN s.DESCRIPTION LIKE '%ATIVO%' THEN CAST(s.VALUE AS DECIMAL(15,2)) ELSE 0 END) AS v_ativo,
        SUM(CASE WHEN s.DESCRIPTION LIKE '%PASSIVO%' THEN CAST(s.VALUE AS DECIMAL(15,2)) ELSE 0 END) AS v_passivo,
        COUNT(*) AS qtd
    FROM source s
    INNER JOIN empresa e ON e.CODE = s.CODE_EMPRESA
    WHERE s.ID_RELATORIO = '1'
      AND s.DATA >= '2023-01-01' AND s.DATA < '2023-02-01'
      AND EXISTS (
          SELECT 1 FROM centro_custo cc 
          WHERE cc.CODE = s.CODE_CENTRO_CUSTO AND cc.SN_ATIVO = 'S'
      )
    GROUP BY e.NAME
)
SELECT 
    JSON_PRETTY(
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'empresa', empresa, 
                'total_ativo', ROUND(v_ativo, 2),
                'total_passivo', ROUND(v_passivo, 2),
                'check_integridade', ROUND(v_ativo + v_passivo, 2),
                'status', CASE WHEN ABS(v_ativo + v_passivo) < 0.01 THEN 'Íntegro' ELSE 'Inconsistente' END,
                'detalhes', JSON_OBJECT('registros_processados', qtd) -- Exemplo de objeto aninhado
            )
        )
    ) AS json_robusto
FROM CTE_DADOS;
        