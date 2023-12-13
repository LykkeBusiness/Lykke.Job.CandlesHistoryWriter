## 2.6.0 - Nova 2. Delivery 38 (December 13, 2023)
### What's changed
* LT-5056: Database blocked due to an active_transaction.

### Deployment
1. Run the following SQL script on the database. It's a script that creates non-clustered indexes on all candles tables in a database. It's important to note that creating indexes can be a resource-intensive operation, so this script should be used with caution on large databases or during peak usage times.
    ```sql
    DECLARE @TableName NVARCHAR(255)
    DECLARE @IndexName NVARCHAR(255)
    DECLARE @SQL NVARCHAR(MAX)
    DECLARE table_cursor CURSOR FOR
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'Candles' AND TABLE_NAME LIKE 'candleshistory_%'
    OPEN table_cursor
    FETCH NEXT FROM table_cursor INTO @TableName
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @IndexName = 'IX_' + @TableName + '_TimeInterval'
        -- Check if the index exists
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = @IndexName AND object_id = OBJECT_ID('Candles.' + @TableName))
        BEGIN
            PRINT 'Skipping table: ' + @TableName + '. Index ' + @IndexName + ' already exists.'
        END
        ELSE
        BEGIN
            BEGIN TRY
                SET @SQL = 'CREATE NONCLUSTERED INDEX ' + @IndexName + ' ON Candles.' + @TableName + '(TimeInterval)'
                EXEC sp_executesql @SQL
                PRINT 'Index ' + @IndexName + ' created successfully for table: ' + @TableName
            END TRY
            BEGIN CATCH
                PRINT 'Error encountered while creating index for table: ' + @TableName + '. Error message: ' + ERROR_MESSAGE()
            END CATCH
        END
        FETCH NEXT FROM table_cursor INTO @TableName
    END
    CLOSE table_cursor
    DEALLOCATE table_cursor
    ```
2. An addition to `CleanupSettings`: a new key named `Timeout` has been introduced. If it's not specified, a default value of 1 hour will be automatically applied. Here's an example of how to set a 10-minute timeout:
   ```json
   {
    "CleanupSettings": {
        "Timeout": "00:10:00"
    }
   }
    ```



## 2.5.0 - Nova 2. Delivery 36 (2023-08-31)
### What's changed
* LT-4906: Update nugets.


**Full change log**: https://github.com/lykkecloud/lykke.job.candleshistorywriter/compare/v2.3.3...v2.5.0
