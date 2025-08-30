DECLARE @tableName NVARCHAR(128)
DECLARE @constraintName NVARCHAR(128)
DECLARE @columnName NVARCHAR(128)
DECLARE @referencedTableName NVARCHAR(128)
DECLARE @referencedColumnName NVARCHAR(128)
DECLARE @sql NVARCHAR(MAX)
DECLARE @schemaName NVARCHAR(128)
DECLARE @schemaName NVARCHAR(128)

-- Set the schema name you want to filter by
SET @schemaName = 'dn'  -- Change this to your desired schema

-- Cursor to fetch the foreign keys from the system views
DECLARE fk_cursor CURSOR FOR
SELECT 
    fk.name AS FK_constraint_name,
    tp.name AS Table_name,
    c.name AS Column_name,
    ref_tab.name AS Referenced_table_name,
    ref_col.name AS Referenced_column_name
FROM 
    sys.foreign_keys fk
INNER JOIN 
    sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
INNER JOIN 
    sys.tables tp ON tp.object_id = fkc.parent_object_id
INNER JOIN 
    sys.columns c ON c.column_id = fkc.parent_column_id AND c.object_id = tp.object_id
INNER JOIN 
    sys.tables ref_tab ON ref_tab.object_id = fkc.referenced_object_id
INNER JOIN 
    sys.columns ref_col ON ref_col.column_id = fkc.referenced_column_id AND ref_col.object_id = ref_tab.object_id
WHERE 
    SCHEMA_NAME(tp.schema_id) = @schemaName  -- Filter by the schema
ORDER BY 
    tp.name, fk.name

-- Open the cursor
OPEN fk_cursor

-- Fetch each row
FETCH NEXT FROM fk_cursor INTO @constraintName, @tableName, @columnName, @referencedTableName, @referencedColumnName

-- Loop through all foreign key constraints
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Construct the ALTER TABLE script to add the foreign key
    SET @sql = 'ALTER TABLE [' + @schemaName + '].[' + @tableName + '] WITH CHECK ADD CONSTRAINT [' + @constraintName + '] FOREIGN KEY([' + @columnName + ']) REFERENCES [' + @schemaName + '].[' + @referencedTableName + '] ([' + @referencedColumnName + '])'
    PRINT @sql  -- This will print the generated ALTER TABLE statement for each FK
    SET @sql = 'ALTER TABLE [' + @schemaName + '].[' + @tableName + '] CHECK CONSTRAINT [' + @constraintName + ']'
    PRINT @sql  -- This will print the CHECK CONSTRAINT statement for each FK

    -- Fetch the next foreign key
    FETCH NEXT FROM fk_cursor INTO @constraintName, @tableName, @columnName, @referencedTableName, @referencedColumnName
END

-- Close and deallocate the cursor
CLOSE fk_cursor
DEALLOCATE fk_cursor




--Drop Constraint
SELECT 
    'ALTER TABLE ' + s.name + '.' + t.name + ' DROP CONSTRAINT ' + fk.name + ';'
FROM 
    sys.foreign_keys AS fk
INNER JOIN 
    sys.tables AS t ON fk.parent_object_id = t.object_id
INNER JOIN 
    sys.schemas AS s ON t.schema_id = s.schema_id
where  s.name = 'dn'

ORDER BY 
    s.name, t.name, fk.name;
	
	
	
	select 'delete from ' + TABLE_SCHEMA+'.' +TABLE_NAME +';' from INFORMATION_SCHEMA.TABLES
where TABLE_TYPE =  'BASE TABLE'
order by TABLE_SCHEMA