CREATE DEFINER=`root`@`localhost` PROCEDURE `Verificador_de_Integridade`()
BEGIN

    SELECT
        t.table_name AS `Tabela`,
        
        -- Verifica se a Chave Primária existe
        IF(pk.constraint_name IS NOT NULL, '✅ SIM', '❌ NÃO') AS `Possui PK`,
        COALESCE(pk.columns, '---') AS `Colunas PK`,
        
        -- Verifica se existe uma coluna de identificação (id ou id_dli)
        IF(cols.has_id > 0, '✅ SIM', '⚠️ NÃO') AS `Possui ID`,
        
        -- Verifica se existem índices extras para performance
        IF(idx.index_count > 0, '✅ SIM', '❌ NÃO') AS `Possui IDX`,
        COALESCE(idx.index_count, 0) AS `Qtd Index`,
        
        -- Mostra o volume de dados atual
        t.table_rows AS `Qtd de Registros`
        
    FROM
        information_schema.tables t
    LEFT JOIN (
        SELECT
            table_name,
            constraint_name,
            GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ', ') AS columns
        FROM
            information_schema.key_column_usage
        WHERE table_schema = 'faturamento_multimarcas_dw'
        AND constraint_name = 'PRIMARY'
        GROUP BY table_name
    ) pk ON t.table_name = pk.table_name
    LEFT JOIN (
        SELECT
            table_name,
            COUNT(*) AS has_id
        FROM information_schema.columns
        WHERE table_schema = 'faturamento_multimarcas_dw'
        AND (column_name = 'id' OR column_name = 'id_dli')
        GROUP BY table_name
    ) cols ON t.table_name = cols.table_name
    LEFT JOIN (
        SELECT
            table_name,
            COUNT(DISTINCT index_name) AS index_count
        FROM information_schema.statistics
        WHERE table_schema = 'faturamento_multimarcas_dw'
        AND index_name <> 'PRIMARY'
        GROUP BY table_name
    ) idx ON t.table_name = idx.table_name

    WHERE 
        t.table_schema = 'faturamento_multimarcas_dw'
        AND t.table_type = 'BASE TABLE'
        AND t.table_name NOT LIKE 'old_%'
    ORDER BY t.table_name;

END;