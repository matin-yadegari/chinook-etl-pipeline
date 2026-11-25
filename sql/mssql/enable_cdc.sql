-- Enable CDC on the Chinook database and on all 11 tables (idempotent).

USE Chinook;
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Chinook' AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO

DECLARE @tables TABLE (name sysname);
INSERT INTO @tables (name) VALUES
    ('Album'), ('Artist'), ('Customer'), ('Employee'), ('Genre'), ('Invoice'),
    ('InvoiceLine'), ('MediaType'), ('Playlist'), ('PlaylistTrack'), ('Track');

DECLARE @name sysname;
DECLARE table_cursor CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM @tables;
OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @name;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM cdc.change_tables ct
        JOIN sys.tables t ON t.object_id = ct.source_object_id
        WHERE t.name = @name AND SCHEMA_NAME(t.schema_id) = 'dbo'
    )
    BEGIN
        EXEC sys.sp_cdc_enable_table
            @source_schema        = N'dbo',
            @source_name          = @name,
            @role_name            = NULL,      -- no gating role
            @supports_net_changes = 0;
        PRINT 'CDC enabled on dbo.' + @name;
    END
    FETCH NEXT FROM table_cursor INTO @name;
END
CLOSE table_cursor;
DEALLOCATE table_cursor;
GO

-- Keep 7 days of changes (10080 minutes; default is 3 days) so a long ETL outage does not lose data.
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 10080;
GO
