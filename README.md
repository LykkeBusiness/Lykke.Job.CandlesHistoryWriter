[![.NET](https://github.com/LykkeBusiness/Lykke.Job.CandlesHistoryWriter/actions/workflows/build.yml/badge.svg)](https://github.com/LykkeBusiness/Lykke.Job.CandlesHistoryWriter/actions/workflows/build.yml)

# Lykke.Job.CandlesHistoryWriter

### Settings ###

Settings schema is:
<!-- MARKDOWN-AUTO-DOCS:START (CODE:src=./template.json) -->
<!-- The below code snippet is automatically added from ./template.json -->
```json
{
  "APP_UID": "Integer",
  "ASPNETCORE_ENVIRONMENT": "String",
  "Assets": {
    "ApiKey": "String",
    "CacheExpirationPeriod": "DateTime",
    "ServiceUrl": "String"
  },
  "ENVIRONMENT": "String",
  "ENV_INFO": "String",
  "Kestrel": {
    "EndPoints": {
      "Http": {
        "Url": "String"
      }
    }
  },
  "MtCandlesHistoryWriter": {
    "AssetsCache": {
      "ApiKey": "String",
      "ExpirationPeriod": "DateTime"
    },
    "CacheCandlesAssetsBatchSize": "Integer",
    "CacheCandlesAssetsRetryCount": "Integer",
    "CacheCleanUpPeriod": "DateTime",
    "CleanupSettings": {
      "Enabled": "Boolean",
      "NumberOfTi1": "Integer",
      "NumberOfTi1800": "Integer",
      "NumberOfTi21600": "Integer",
      "NumberOfTi300": "Integer",
      "NumberOfTi3000000": "Integer",
      "NumberOfTi3600": "Integer",
      "NumberOfTi43200": "Integer",
      "NumberOfTi60": "Integer",
      "NumberOfTi604800": "Integer",
      "NumberOfTi7200": "Integer",
      "NumberOfTi86400": "Integer",
      "NumberOfTi900": "Integer",
      "NumberOfTiDefault": "Integer"
    },
    "Cqrs": {
      "ConnectionString": "String",
      "EnvironmentName": "String",
      "RetryDelay": "DateTime"
    },
    "Db": {
      "FeedHistoryConnectionString": "String",
      "LogsConnectionString": "String",
      "SnapshotsConnectionString": "String",
      "StorageMode": "String"
    },
    "ErrorManagement": {
      "NotifyOnCantStoreAssetPair": "Boolean",
      "NotifyOnCantStoreAssetPairTimeout": "DateTime"
    },
    "HistoryTicksCacheSize": "Integer",
    "Migration": {
      "MigrationEnabled": "Boolean",
      "Quotes": {
        "CandlesToDispatchLengthThrottlingThreshold": "Integer",
        "ThrottlingDelay": "DateTime"
      },
      "Trades": {
        "CandlesPersistenceQueueLimit": "Integer",
        "SqlCommandTimeout": "DateTime",
        "SqlQueryBatchSize": "Integer",
        "SqlTradesDataSourceConnString": "String"
      }
    },
    "Persistence": {
      "CandlesToDispatchLengthPersistThreshold": "Integer",
      "CandlesToDispatchLengthThrottlingThreshold": "Integer",
      "MaxBatchesToPersistQueueLength": "Integer",
      "MaxBatchSize": "Integer",
      "NumberOfSaveThreads": "Integer",
      "PersistPeriod": "DateTime",
      "ThrottlingEnqueueDelay": "DateTime"
    },
    "QueueMonitor": {
      "BatchesToPersistQueueLengthWarning": "Integer",
      "CandlesToDispatchQueueLengthWarning": "Integer",
      "ScanPeriod": "DateTime"
    },
    "Rabbit": {
      "CandlesSubscription": {
        "ConnectionString": "String",
        "Namespace": "String"
      }
    },
    "UseSerilog": "Boolean"
  },
  "MyMonitoringUrl": "String",
  "RedisSettings": {
    "Configuration": "String"
  },
  "serilog": {
    "minimumLevel": {
      "default": "String"
    }
  },
  "TZ": "String"
}
```
<!-- MARKDOWN-AUTO-DOCS:END -->
