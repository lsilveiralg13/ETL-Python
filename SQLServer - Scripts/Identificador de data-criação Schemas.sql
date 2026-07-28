SELECT 
    s.name AS SchemaName,
    o.name AS TableName,
    o.create_date AS DataCriacao,
    o.modify_date AS UltimaModificacao
FROM sys.objects o
INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.type = 'U' -- 'U' filtra apenas Tabelas do Usuário (User Tables)
  AND s.name = 'producao'
ORDER BY o.create_date DESC;