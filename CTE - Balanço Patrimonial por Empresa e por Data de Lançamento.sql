-- ETAPA 1: Criar um CTE para consolidar o balanço patrimonial por empresa e por data de lançamento
WITH CTE_BASE AS (
	SELECT
		CONCAT('R$ ', FORMAT(CAST(s.VALUE AS DECIMAL(15,2)), 2, 'pt_BR')) AS `Valor`,
        -- É importante recriar a função CAST para trazer a soma numérica, em uma eventual soma de valores, pois valores convertidos em R$ podem ser refatorados em STRING
        CAST(s.VALUE AS DECIMAL(15,2)) AS `Valor Numérico`,
        s.DATA,
        e.CODE AS `Código Empresa`,
        e.NAME AS `Nome Empresa`,
        r.NAME AS `Tipo de Relatório`
-- Etapa 2. Definindo quais serão os JOINS
	FROM
		source s
	JOIN
		empresa e
			ON e.CODE = s.CODE_EMPRESA
	JOIN
		centro_custo cc
			ON cc.CODE = s.CODE_CENTRO_CUSTO
	JOIN 
		relatorio r
			ON r.CODE = s.ID_RELATORIO
	WHERE
		cc.SN_ATIVO = 'S'
        AND r.SN_ATIVO = 'S'
        AND r.NAME = 'BALANÇO PATRIMONIAL'
        AND s.DATA >= '2023-01-01'
        AND s.DATA <= '2023-02-01'
)
SELECT * FROM CTE_BASE;