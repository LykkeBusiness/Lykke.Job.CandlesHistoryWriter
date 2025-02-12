﻿// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using Autofac;
using Autofac.Extensions.DependencyInjection;
using AzureStorage.Blob;
using AzureStorage.Tables;
using Common.Log;
using Lykke.Common;
using Lykke.HttpClientGenerator;
using Lykke.Job.CandleHistoryWriter.Repositories.Candles;
using Lykke.Job.CandleHistoryWriter.Repositories.Cleanup;
using Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.MeFeedHistory;
using Lykke.Job.CandleHistoryWriter.Repositories.Snapshots;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Service.Assets.Client;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.MeFeedHistory;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration.HistoryProviders;
using Lykke.Job.CandlesHistoryWriter.Services;
using Lykke.Job.CandlesHistoryWriter.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders.MeFeedHistory;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;
using Lykke.SettingsReader;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Moq;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.RabbitMqBroker;
using Lykke.RabbitMqBroker.Subscriber;
using Lykke.RabbitMqBroker.Subscriber.MessageReadStrategies;
using MarginTrading.AssetService.Contracts;
using Microsoft.Extensions.Logging;

namespace Lykke.Job.CandlesHistoryWriter.DependencyInjection
{
    public class JobModule : Module
    {
        private readonly IServiceCollection _services;
        private readonly MarketType _marketType;
        private readonly CandlesHistoryWriterSettings _settings;
        private readonly AssetsSettings _assetSettings;
        private readonly RedisSettings _redisSettings;
        private readonly IReloadingManager<DbSettings> _dbSettings;
        private readonly ILog _log;
        private readonly MonitoringServiceClientSettings _monitoringServiceClient;
        private readonly CandlesShardRemoteSettings _candlesShardRemoteSettings;
        private readonly RabbitMqSubscriptionSettings _subscriptionSettings;

        public JobModule(
            MarketType marketType,
            CandlesHistoryWriterSettings settings,
            AssetsSettings assetSettings,
            RedisSettings redisSettings,
            MonitoringServiceClientSettings monitoringServiceClient,
            IReloadingManager<DbSettings> dbSettings,
            CandlesShardRemoteSettings candlesShardRemoteSettings,
            ILog log)
        {
            _services = new ServiceCollection();
            _marketType = marketType;
            _settings = settings;
            _assetSettings = assetSettings;
            _redisSettings = redisSettings;
            _monitoringServiceClient = monitoringServiceClient;
            _dbSettings = dbSettings;
            _candlesShardRemoteSettings = candlesShardRemoteSettings;
            _log = log;
            _subscriptionSettings = RabbitMqSubscriptionSettings
                .CreateForSubscriber(_settings.Rabbit.CandlesSubscription.ConnectionString,
                    _settings.Rabbit.CandlesSubscription.Namespace,
                    $"candles-v2.{_candlesShardRemoteSettings.Name}",
                    _settings.Rabbit.CandlesSubscription.Namespace, "candleshistory");
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterInstance(_log)
                .As<ILog>()
                .SingleInstance();


            builder.RegisterType<Clock>().As<IClock>();

            if (_monitoringServiceClient != null)
            {
                builder.RegisterInstance(_monitoringServiceClient).AsSelf().SingleInstance();
            }

            RegisterResourceMonitor(builder);

            RegisterRedis(builder);

            RegisterAssets(builder);

            RegisterCandles(builder);

            _services.AddSingleton<IPoisonQueueHandler, ParallelExecutionGuardPoisonQueueDecorator>(p =>
                new ParallelExecutionGuardPoisonQueueDecorator(
                    new PoisonQueueHandler(
                        _subscriptionSettings.ConnectionString,
                        p.GetService<IConnectionProvider>(),
                        PoisonQueueConsumerConfigurationOptions.Create(
                            PoisonQueueName.Create(_subscriptionSettings.QueueName),
                            ExchangeName.Create(_subscriptionSettings.ExchangeName),
                            RoutingKey.Create(_subscriptionSettings.RoutingKey)
                        ),
                        p.GetService<ILoggerFactory>())));

            builder.Populate(_services);
        }

        private void RegisterResourceMonitor(ContainerBuilder builder)
        {
            var monitorSettings = _settings.ResourceMonitor;
            if (monitorSettings != null && _monitoringServiceClient != null
                && !string.IsNullOrEmpty(_monitoringServiceClient.MonitoringServiceUrl))
            {
                switch (monitorSettings.MonitorMode)
                {
                    case ResourceMonitorMode.Off:
                        // Do not register any resource monitor.
                        break;

                    case ResourceMonitorMode.AppInsightsOnly:
                        builder.RegisterResourcesMonitoring(_log);
                        break;

                    case ResourceMonitorMode.AppInsightsWithLog:
                        builder.RegisterResourcesMonitoringWithLogging(
                            _log,
                            monitorSettings.CpuThreshold,
                            monitorSettings.RamThreshold);
                        break;
                }
            }

        }

        private void RegisterRedis(ContainerBuilder builder)
        {
            builder.Register(c => ConnectionMultiplexer.Connect(_redisSettings.Configuration))
                .As<IConnectionMultiplexer>()
                .SingleInstance();
        }


        private void RegisterAssets(ContainerBuilder builder)
        {
            if (_marketType == MarketType.Spot)
            {
                _services.RegisterAssetsClient(AssetServiceSettings.Create(
                   new Uri(_assetSettings.ServiceUrl),
                   _settings.AssetsCache.ExpirationPeriod),
               _log);

                builder.RegisterType<AssetPairsManager>()
                   .As<IAssetPairsManager>()
                   .SingleInstance();
            }
            else
            {
                builder.RegisterClient<IAssetPairsApi>(_assetSettings.ServiceUrl, builderConfigure =>
                {
                    if (!string.IsNullOrWhiteSpace(_assetSettings.ApiKey))
                    {
                        builderConfigure = builderConfigure.WithApiKey(_assetSettings.ApiKey);
                    }

                    return builderConfigure;
                });

                builder.RegisterType<MtAssetPairsManager>()
                 .As<IAssetPairsManager>()
                 .SingleInstance();
            }
        }

        private void RegisterCandles(ContainerBuilder builder)
        {
            builder.RegisterInstance(_candlesShardRemoteSettings)
                .AsSelf()
                .SingleInstance();

            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<HealthLogger>()
                .As<IStartable>()
                .SingleInstance()
                .AutoActivate();

            if (_settings.Db.StorageMode == StorageMode.SqlServer)
            {
                builder.RegisterType<SqlCandlesHistoryRepository>()
                    .As<ICandlesHistoryRepository>()
                    .WithParameter(TypedParameter.From(_dbSettings.Nested(s => s.SnapshotsConnectionString)))
                    .SingleInstance();

                builder.RegisterType<CandlesHistoryBackupService>()
                    .As<ICandlesHistoryBackupService>()
                    .WithParameter(TypedParameter.From(_dbSettings.Nested(s => s.SnapshotsConnectionString)))
                    .SingleInstance();
            }
            else if (_settings.Db.StorageMode == StorageMode.Azure)
            {
                builder.RegisterType<CandlesHistoryRepository>()
                    .As<ICandlesHistoryRepository>()
                    .WithParameter(TypedParameter.From(_dbSettings.Nested(s => s.SnapshotsConnectionString)))
                    .SingleInstance();
            }

            if (_settings.Migration != null)
            {
                builder.RegisterType<StartupManager>()
                    .As<IStartupManager>()
                    .WithParameter(TypedParameter.From(_settings.Migration.MigrationEnabled))
                    .SingleInstance();

                builder.RegisterType<ShutdownManager>()
                    .As<IShutdownManager>()
                    .WithParameter(TypedParameter.From(_settings.Migration.MigrationEnabled))
                    .SingleInstance();
            }
            else
            {
                builder.RegisterType<StartupManager>()
                    .As<IStartupManager>()
                    .WithParameter(TypedParameter.From(false))
                    .SingleInstance();

                builder.RegisterType<ShutdownManager>()
                    .As<IShutdownManager>()
                    .WithParameter(TypedParameter.From(false))
                    .SingleInstance();
            }


            builder.RegisterType<SnapshotSerializer>()
                .As<ISnapshotSerializer>()
                .SingleInstance();

            // Now creating a silent -or- logging candles checker object.
            // CandlesChecker -- logs notifications on candles without properly configured connection strings for asset pair using the specified timeout between similar notifications.
            // CandlesHistorySilent -- does not log notifications.
            if (_settings.ErrorManagement.NotifyOnCantStoreAssetPair)
                builder.RegisterType<CandlesChecker>()
                    .As<ICandlesChecker>()
                    .WithParameter(TypedParameter.From(_settings.ErrorManagement.NotifyOnCantStoreAssetPairTimeout))
                    .SingleInstance();
            else
                builder.RegisterType<CandlesCheckerSilent>()
                    .As<ICandlesChecker>()
                    .SingleInstance();

            builder.AddRabbitMqConnectionProvider();

            builder.AddRabbitMqListener<CandlesUpdatedEvent, CandlesUpdatesHandler>(
                    _subscriptionSettings, (subscriber, context) =>
                    {
                        if (_settings.Rabbit.Prefetch.HasValue)
                        {
                            subscriber.SetPrefetchCount(_settings.Rabbit.Prefetch.Value);
                        }
                    })
                .AddOptions(RabbitMqListenerOptions<CandlesUpdatedEvent>.MessagePack.NoLoss);

            builder.RegisterType<CandlesManager>()
                .As<ICandlesManager>()
                .SingleInstance();

            builder.RegisterType<CandlesAmountManager>()
                .WithParameter(TypedParameter.From(_settings.CleanupSettings))
                .WithParameter(TypedParameter.From(_settings.HistoryTicksCacheSize))
                .As<ICandlesAmountManager>()
                .SingleInstance();

            builder.RegisterType<RedisCandlesCacheService>()
                .As<ICandlesCacheService>()
                .WithParameter(TypedParameter.From(_marketType))
                .SingleInstance();

            builder.RegisterType<CandlesPersistenceManager>()
                .As<ICandlesPersistenceManager>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.Persistence));

            builder.RegisterType<CandlesPersistenceQueue>()
                .As<ICandlesPersistenceQueue>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.Persistence))
                .WithParameter(TypedParameter.From(_settings.Db.StorageMode));

            builder.RegisterType<QueueMonitor>()
                .As<IStartable>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.QueueMonitor))
                .AutoActivate();


            builder.RegisterInstance(new CachingSettings(_settings.CacheCandlesAssetsBatchSize, _settings.CacheCandlesAssetsRetryCount))
                .As<CachingSettings>()
                .SingleInstance();
            
            builder.RegisterType<CandlesCacheInitializationService>()
                .As<ICandlesCacheInitializationService>()
                .SingleInstance();

            RegisterCandlesCleanup(builder);

            builder.RegisterType<RedisCacheTruncator>()
                .AsSelf()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_marketType))
                .WithParameter(TypedParameter.From(_settings.CacheCleanupPeriod));

            builder.RegisterType<CandlesShardValidator>()
                .As<ICandlesShardValidator>()
                .SingleInstance();

            RegisterCandlesMigration(builder);

            RegisterCandlesFiltration(builder);
        }

        private void RegisterCandlesMigration(ContainerBuilder builder)
        {
            if (_settings.Migration != null)
            {
                if (!string.IsNullOrWhiteSpace(_dbSettings.CurrentValue.FeedHistoryConnectionString))
                {
                    builder.RegisterType<FeedHistoryRepository>()
                        .As<IFeedHistoryRepository>()
                        .WithParameter(TypedParameter.From(AzureTableStorage<FeedHistoryEntity>.Create(
                            _dbSettings.ConnectionString(x => x.FeedHistoryConnectionString),
                            "FeedHistory",
                            _log,
                            maxExecutionTimeout: TimeSpan.FromMinutes(5))))
                        .SingleInstance();
                }
                builder.RegisterType<CandlesMigrationManager>()
                    .AsSelf()
                    .WithParameter(TypedParameter.From(_settings.Migration))
                    .SingleInstance();

                builder.RegisterType<CandlesesHistoryMigrationService>()
                    .As<ICandlesHistoryMigrationService>()
                    .SingleInstance();

                builder.RegisterType<MigrationCandlesGenerator>()
                    .AsSelf()
                    .SingleInstance();

                builder.RegisterType<EmptyMissedCandlesGenerator>()
                    .As<IMissedCandlesGenerator>()
                    .SingleInstance();

                builder.RegisterType<HistoryProvidersManager>()
                    .As<IHistoryProvidersManager>()
                    .SingleInstance();

                RegisterHistoryProvider<MeFeedHistoryProvider>(builder);

                builder.RegisterType<TradesMigrationHealthService>()
                    .AsSelf()
                    .SingleInstance();

                builder.RegisterType<TradesMigrationService>()
                    .As<ITradesMigrationService>()
                    .WithParameter(TypedParameter.From(_settings.Migration.Trades.SqlTradesDataSourceConnString))
                    .WithParameter(TypedParameter.From(_settings.Migration.Trades.SqlQueryBatchSize))
                    .WithParameter(TypedParameter.From(_settings.Migration.Trades.SqlCommandTimeout))
                    .WithParameter(TypedParameter.From(_settings.Migration.Trades.CandlesPersistenceQueueLimit))
                    .SingleInstance();

                builder.RegisterType<TradesMigrationManager>()
                    .AsSelf()
                    .WithParameter(TypedParameter.From(_settings.Migration.Trades.SqlQueryBatchSize))
                    .WithParameter(TypedParameter.From(_settings.Migration.MigrationEnabled))
                    .SingleInstance();
            }

        }

        private void RegisterCandlesFiltration(ContainerBuilder builder)
        {
            builder.RegisterType<CandlesFiltrationService>()
                .As<ICandlesFiltrationService>()
                .SingleInstance();

            builder.RegisterType<CandlesFiltrationManager>()
                .AsSelf()
                .SingleInstance();
        }

        private static void RegisterHistoryProvider<TProvider>(ContainerBuilder builder)
            where TProvider : IHistoryProvider
        {
            builder.RegisterType<TProvider>()
                .Named<IHistoryProvider>(typeof(TProvider).Name);
        }

        private void RegisterCandlesCleanup(ContainerBuilder builder)
        {
            if (_settings.Db.StorageMode == StorageMode.SqlServer)
            {
                builder.Register<ICandlesPersistenceQueueSnapshotRepository>(ctx =>
                        new SqlCandlesPersistenceQueueSnapshotRepository(_dbSettings.CurrentValue.SnapshotsConnectionString))
                    .SingleInstance();

                builder.RegisterType<SqlCandlesCleanup>()
                    .WithParameter(TypedParameter.From(_settings.CleanupSettings))
                    .WithParameter(TypedParameter.From(_settings.Db.SnapshotsConnectionString))
                    .AsImplementedInterfaces()
                    .SingleInstance();
            }
            else if (_settings.Db.StorageMode == StorageMode.Azure)
            {
                builder.RegisterType<CandlesPersistenceQueueSnapshotRepository>()
                    .As<ICandlesPersistenceQueueSnapshotRepository>()
                    .WithParameter(TypedParameter.From(AzureBlobStorage.Create(
                        _dbSettings.ConnectionString(x => x.SnapshotsConnectionString), TimeSpan.FromMinutes(10))));

                builder.Register(ctx => Mock.Of<SqlCandlesCleanup>())
                    .As<ICandlesCleanup>()
                    .SingleInstance();
            }
            builder.RegisterDecorator<CandlesCleanupTimer, ICandlesCleanup>();
            builder.RegisterDecorator<CandlesCleanupFailureLogger, ICandlesCleanup>();
            builder.RegisterDecorator<CandlesCleanupMultiRunGuard, ICandlesCleanup>();
            builder.RegisterDecorator<ICandlesCleanup>(
                (ctx, decoratee) =>
                    new CandlesCleanupEnabledGuard(decoratee, _settings.CleanupSettings, ctx.Resolve<ILog>()),
                fromKey: "CandlesCleanupMultiRunGuard");
        }
    }
}
