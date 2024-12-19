## 2.14.0 - Nova 2. Delivery 48 (December 19, 2024)
### What's changed
* LT-5951: Update refit to 8.x version.
* LT-5878: Keep schema for appsettings.json up to date.


## 2.13.0 - Nova 2. Delivery 47 (November 15, 2024)
### What's changed
* LT-5853: Update messagepack to 2.x version.
* LT-5782: Add assembly load logger.
* LT-5759: Migrate to quorum queues.


## 2.12.0 - Nova 2. Delivery 46 (September 26, 2024)
### What's changed
* LT-5602: Migrate to net 8.
* LT-5517: Update rabbitmq broker library with new rabbitmq.client and templates.


## 2.11.0 - Nova 2. Delivery 45 (September 02, 2024)
### What's changed
* LT-5672: Redis timeout.
* LT-5566: Implement rfactor saga.

### Deployment

Add new configuration key `CacheCandlesAssetsBatchSize`. It controls the size of the asset pairs batch when caching candles which affects startup performance. For better performace it is recommended to use values > 10. This will allow the utilize client host resources and use Redis connection more optimal. However, the particular value is a matter of experiments on particular environment setup.
Example:
```
    "HistoryTicksCacheSize": 200,
    "CacheCleanUpPeriod": "00:03:00",
    "CacheCandlesAssetsBatchSize": 100,
```


## 2.10.0 - Nova 2. Delivery 44 (August 16, 2024)
### What's changed
* LT-5415: Add diagnostic variables to create a readable dump.


## 2.9.0 - Nova 2. Delivery 41 (April 01, 2024)
### What's changed
* LT-5445: Update packages.


## 2.8.0 - Nova 2. Delivery 40 (February 28, 2024)
### What's changed
* LT-5283: Step: deprecated packages validation is failed.
* LT-5200: Update lykke.httpclientgenerator to 5.6.2.


## 2.7.1 - Nova 2. Delivery 39. Hotfix 2 (February 7, 2024)
### What's changed
* LT-5239: Update vulnerable packages

## 2.7.0 - Nova 2. Delivery 39 (January 29, 2024)
### What's changed
* LT-5169: Add history of releases into `changelog.md`


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

## v2.3.3 - Nova 2. Delivery 32
## What's changed
* LT-4410: Do not let the host keep running if startupmanager failed to start.


**Full change log**: https://github.com/lykkecloud/lykke.job.candleshistorywriter/compare/v2.2.2...v2.3.3

## v2.2.2 - Nova 2. Delivery 28. Hotfix 3
* LT-4315: Upgrade LykkeBiz.Logs.Serilog to 3.3.1

## v2.2.1 - Nova 2. Delivery 28. Hotfix 2
## What's changed
* LT-4299: Component crash.


**Full change log**: https://github.com/lykkecloud/lykke.job.candleshistorywriter/compare/v2.2.0...v2.2.1

## v2.2.0 - Nova 2. Delivery 28
## What's Changed
* LT-3721: NET 6 migration

### Deployment
* NET 6 runtime is required
* Dockerfile is updated to use native Microsoft images (see [DockerHub](https://hub.docker.com/_/microsoft-dotnet-runtime/))

**Full Changelog**: https://github.com/LykkeBusiness/Lykke.Job.CandlesHistoryWriter/compare/v2.1.3...v2.2.0

## v2.1.3 - Nova 2. Delivery 24
## What's Changed
* fix(LT-3941): avoided using IDatabase in a singleton scope by @tarurar in https://github.com/LykkeBusiness/Lykke.Job.CandlesHistoryWriter/pull/45
* LT-3900: [CandlesHistoryWriter] Upgrade Lykke.HttpClientGenerator nuget by @lykke-vashetsin in https://github.com/LykkeBusiness/Lykke.Job.CandlesHistoryWriter/pull/44


**Full Changelog**: https://github.com/LykkeBusiness/Lykke.Job.CandlesHistoryWriter/compare/v2.1.0...v2.1.3

## v2.1.1 - Nova 2. Delivery 18.
### Tasks

* LT-3588: Remove the delay when pushing candles into the internal queue

### Deployment

The consequence of removing the delay leads to the possibility of increasing host memory in case DB processing is slow and/or not effective.

## v2.1.0 - Delivery 11
### Tasks

* LT-3208: change naming convention for stored procedure

### Deployment

* delete Candles.SP_Cleanup from db

## v1.5.5 - Sharding
### Tasks

* LT-2339: Implement single shard candles subscriber per instance

### Deployment

A new key has been added to the settings: Rabbit.CandlesSubscription.ShardName.

### Description

The updated CandlesWriter will serve for one shard (defined in LT-2338: [CandlesProducer] Implement candles sharding based on asset pair regexp), either named shard or default.

The new settings are optional, if nothing provided the CandlesWriter will subscribe to default exchange which is lykke.mt.candles-v2.default and use the default queue lykke.mt.candles-v2.default.candlesproducer to read messages from.

Please note, it is the responsibility of the DevOps engineer to configure the number of instances of CandlesWriter to satisfy sharding settings of CandlesProducer. On the whole, we gonna need n+1 instances of CandlesWriter where n is the number of shards configured in CandlesProducer + 1 instance for default shard.

Warning: Since CandlesWriter is not a stateless job it is highly undesirable to run multiple instances of it for a single shard name, in this case, we might face issues with candles writer. 

## v1.5.4 - Bugfixes 
### Tasks

* LT-2274: Don't fail a caching candles history
* LT-2311: [DEMO] Candle History stopped to consume messages
* LT-2332: No more consumer on Bookkeeper queue


### Deployment

New optional settings key has been added:  CandlesHistoryWriter.Rabbit.Prefetch. 
The default value of 100 will be used if not set.

## v1.5.1 - Bugfix
### Tasks

* LT-2273: The tables for assets in SQL aren't creating

## v1.5.0 - Migration to .NET 3.1
### Tasks

LT-2179: Migrate to 3.1 Core and update DL libraries

## v1.4.11 - Bugfixes
### Tasks

LT-2013: Improve Alpine docker files
LT-2160: Fix threads leak with RabbitMq subscribers and publishers

## v1.4.9 - Improvements
### Tasks

LT-1987: Migrate to Alpine docker images in MT Core services
LT-1917: Lykke.Job.CandlesHistoryWriter: add WebHostLogger
LT-1943: Message resend API for not critical brokers

## v1.4.7 - Cache configuration + bugfix
### Tasks
LT-1784: Chart per hours issue candles DEADLINE 25.10.19
LT-1739: Configuration of candles cache.
LT-1756: update .net to 2.2

## v1.4.5 - Fix candles intervals
LT-1624: Fix candles intervals

## v1.4.4 - Candles cleanup feature - remove SQL job
Bugfix
LT-1622: update BookKeeper package

## v1.4.3 

## v1.4.1 - Candles cleanup feature
*LT-1570*: Candles cleanup

### Settings changes. 
Add to MtCandlesHistoryWriter section:
"Cqrs": {
      "ConnectionString": "amqp://margintrading:margintrading@rabbit-mt.mt.svc.cluster.local:5672",
      "RetryDelay": "00:00:02",
      "EnvironmentName": "dev"
    },
"CleanupSettings": {
      "Enabled": true,
      "NumberOfTi1": 0,
      "NumberOfTi60": 0,
      "NumberOfTi300": 0,
      "NumberOfTi900": 0,
      "NumberOfTi1800": 0,
      "NumberOfTi3600": 0,
      "NumberOfTi7200": 0,
      "NumberOfTi21600": 0,
      "NumberOfTi43200": 0,
      "NumberOfTi86400": 0,
      "NumberOfTi604800": 0,
      "NumberOfTi3000000": 0,
      "NumberOfTiDefault": 10000
    }
If NumberOfTiDefault is not set 10000 will be applied. If any of NumberOfTi* is not set (or 0) NumberOfTiDefault will be applied.

## v1.4.0 - License
### Tasks
LT-1541: Update licenses in lego service to be up to latest requirements

## v1.3.5 - Secured API clients
### Tasks
MTC-809: Secure all "visible" endpoints in mt-core

### Deployment
Add new property ApiKey to Assets section (optional, if settings service does not use API key):
```json
"Assets": 
  {
    "ServiceUrl": "settings service url",
    "CacheExpirationPeriod": "00:05:00",
    "ApiKey": "settings service secret key"
  },
```

## v1.3.4 - Optimizations
### Tasks

MTC-781: Optimize candles

## v1.3.3 - Updated projects versions
No changes

## v1.3.1 - Fixes error with dashes in asset ID
### Bugfix

MTC-589 : SQL error in CandleHistoryWriter tag v1.2.5

## v1.3.0 - Auto schema creation
CandlesHistoryWriter create schema if not exists (MTC-503)

## v1.2.4 - Monitoring service completely optional

## v1.2.3 - Bugfixes
### Bugfixes
- Fixed Serilog text logs (MTC-461)
- Create table fails in Writer for assetPair with dash (MTC-465)

## v1.2.2 - Commented unused code
Fix Fortify comment (MTC-439)

## v1.2.1 - Deployment and maintenance improvements Edit
Introduced:
Text logs
Kestrel configuration (see README)

New settings:
UseSerilog": true

## v1.2.0 - New configuration of connection strings
Latest config example: [candles-writer.appsettings.json.zip](https://github.com/lykkecloud/Lykke.Job.CandlesHistoryWriter/files/2500905/candles-writer.appsettings.json.zip)

## v1.1.0 - Bug fixes
No config changes

## v1.0.7 - No Azure dependencies, SQL
[Configs.zip](https://github.com/lykkecloud/Lykke.Job.CandlesHistoryWriter/files/2251279/Configs.zip)
