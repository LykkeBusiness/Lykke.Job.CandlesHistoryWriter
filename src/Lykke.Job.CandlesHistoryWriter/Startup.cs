// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Autofac;
using Autofac.Extensions.DependencyInjection;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Common.ApiLibrary.Middleware;
using Lykke.HttpClientGenerator.Infrastructure;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.DependencyInjection;
using Lykke.SettingsReader;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Converters;
using Lykke.Job.CandlesHistoryWriter.Models;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Logs.MsSql;
using Lykke.Logs.MsSql.Repositories;
using Lykke.Logs.Serilog;
using Lykke.Snow.Common.Correlation;
using Lykke.Snow.Common.Correlation.Cqrs;
using Microsoft.Extensions.Logging;
using Lykke.Snow.Common.Startup.Log;
using Lykke.Snow.Common.Startup.Hosting;
using MarginTrading.SettingsService.Contracts;
using Microsoft.Extensions.Hosting;
using Microsoft.OpenApi.Models;

namespace Lykke.Job.CandlesHistoryWriter
{
    [UsedImplicitly]
    public class Startup
    {
        private IReloadingManager<AppSettings> _mtSettingsManager;
        
        private CandlesShardRemoteSettings _candlesShardSettings;
        private IWebHostEnvironment Environment { get; }
        private ILifetimeScope ApplicationContainer { get; set; }
        private IConfigurationRoot Configuration { get; }
        private ILog Log { get; set; }

        private const string ApiVersion = "v1";
        private const string ApiTitle = "Candles History Writer Job";

        public Startup(IWebHostEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("env.json", true)
                .AddSerilogJson(env)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
            Environment = env;
        }

        [UsedImplicitly]
        public void ConfigureServices(IServiceCollection services)
        {
            try
            {
                services
                    .AddControllers()
                    .AddNewtonsoftJson(options =>
                    {
                        options.SerializerSettings.Converters.Add(new StringEnumConverter());
                        options.SerializerSettings.ContractResolver =
                            new Newtonsoft.Json.Serialization.DefaultContractResolver();
                    });

                services.AddSwaggerGen(options =>
                {
                    options.SwaggerDoc(ApiVersion, new OpenApiInfo {Version = ApiVersion, Title = ApiTitle});
                });
                
                LoadConfiguration();

                Log = CreateLog(Configuration, GetRelevantCandlesHistoryWriterSettings());

                services.AddSingleton<ILoggerFactory>(x => new WebHostLoggerFactory(Log));
                var correlationContextAccessor = new CorrelationContextAccessor();
                services.AddSingleton(correlationContextAccessor);
                services.AddSingleton<CqrsCorrelationManager>();
                
                services.AddApplicationInsightsTelemetry();
            }
            catch (Exception ex)
            {
                Log?.WriteFatalErrorAsync(nameof(Startup), nameof(ConfigureServices), "", ex).Wait();
                throw;
            }
        }

        private IReloadingManager<CandlesHistoryWriterSettings> GetRelevantCandlesHistoryWriterSettings()
        {
            return _mtSettingsManager.CurrentValue.CandlesHistoryWriter != null
                ? _mtSettingsManager.Nested(x => x.CandlesHistoryWriter)
                : _mtSettingsManager.Nested(x => x.MtCandlesHistoryWriter);
        }

        private void LoadConfiguration()
        {
            // load service settings
            _mtSettingsManager = Configuration.LoadSettings<AppSettings>();

            // load candles sharding settings from settings service
            var candlesSettingsClientBuilder = HttpClientGenerator.HttpClientGenerator
                .BuildForUrl(_mtSettingsManager.CurrentValue.Assets.ServiceUrl)
                .WithAdditionalCallsWrapper(new ExceptionHandlerCallsWrapper());

            if (!string.IsNullOrWhiteSpace(_mtSettingsManager.CurrentValue.Assets.ApiKey))
            {
                candlesSettingsClientBuilder =
                    candlesSettingsClientBuilder.WithApiKey(_mtSettingsManager.CurrentValue.Assets.ApiKey);
            }

            var candlesSettingsClient = candlesSettingsClientBuilder
                .Create()
                .Generate<ICandlesSettingsApi>();

            var remoteSettings = candlesSettingsClient
                .GetConsumerSettingsAsync(GetRelevantCandlesHistoryWriterSettings().CurrentValue.Rabbit.CandlesSubscription.ShardName)
                .GetAwaiter()
                .GetResult();

            _candlesShardSettings =
                new CandlesShardRemoteSettings {Name = remoteSettings.Name, Pattern = remoteSettings.Pattern};
        }

        [UsedImplicitly]
        public void ConfigureContainer(ContainerBuilder builder)
        {
            var marketType = _mtSettingsManager.CurrentValue.CandlesHistoryWriter != null
                ? MarketType.Spot
                : MarketType.Mt;
            
            var candlesHistoryWriter = _mtSettingsManager.CurrentValue.CandlesHistoryWriter != null
                ? _mtSettingsManager.Nested(x => x.CandlesHistoryWriter)
                : _mtSettingsManager.Nested(x => x.MtCandlesHistoryWriter);
            
            builder.RegisterModule(new JobModule(
                marketType,
                candlesHistoryWriter.CurrentValue,
                _mtSettingsManager.CurrentValue.Assets,
                _mtSettingsManager.CurrentValue.RedisSettings,
                _mtSettingsManager.CurrentValue.MonitoringServiceClient,
                candlesHistoryWriter.Nested(x => x.Db),
                _candlesShardSettings,
                Log));
            
            builder.RegisterModule(new CqrsModule(_mtSettingsManager.CurrentValue.MtCandlesHistoryWriter.Cqrs, Log));
        }

        [UsedImplicitly]
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, IHostApplicationLifetime appLifetime)
        {
            try
            {
                ApplicationContainer = app.ApplicationServices.GetAutofacRoot();
                
                if (env.IsDevelopment())
                {
                    app.UseDeveloperExceptionPage();
                }
                
                app.UseLykkeMiddleware(nameof(Startup), ex => ErrorResponse.Create("Technical problem"));

                app.UseRouting();
                app.UseEndpoints(endpoints => {
                    endpoints.MapControllers();
                });
                app.UseSwagger(c =>
                {
                    c.PreSerializeFilters.Add((swagger, httpReq) => 
                        swagger.Servers =
                            new List<OpenApiServer>
                            {
                                new OpenApiServer
                                {
                                    Url = $"{httpReq.Scheme}://{httpReq.Host.Value}"
                                }
                            });
                });
                app.UseSwaggerUI(x =>
                {
                    x.SwaggerEndpoint("/swagger/v1/swagger.json", "v1");
                });
                app.UseStaticFiles();

                appLifetime.ApplicationStarted.Register(() => StartApplication(appLifetime).GetAwaiter().GetResult());
                appLifetime.ApplicationStopping.Register(() => StopApplication().GetAwaiter().GetResult());
                appLifetime.ApplicationStopped.Register(() => CleanUp().GetAwaiter().GetResult());
            }
            catch (Exception ex)
            {
                Log?.WriteFatalErrorAsync(nameof(Startup), nameof(Configure), "", ex).Wait();
                throw;
            }
        }

        private async Task StartApplication(IHostApplicationLifetime appLifetime)
        {
            try
            {
                await ApplicationContainer.Resolve<IStartupManager>().StartAsync();

                Program.AppHost.WriteLogs(Environment, Log);

                await Log.WriteMonitorAsync(nameof(Startup), nameof(StartApplication), "", "Started");

            }
            catch (Exception ex)
            {
                await Log.WriteFatalErrorAsync(nameof(Startup), nameof(StartApplication), "", ex);
                appLifetime.StopApplication();
            }
        }

        private async Task StopApplication()
        {
            try
            {
                await ApplicationContainer.Resolve<IShutdownManager>().ShutdownAsync();
            }
            catch (Exception ex)
            {
                if (Log != null)
                {
                    await Log.WriteFatalErrorAsync(nameof(Startup), nameof(StopApplication), "", ex);
                }
                throw;
            }
        }

        private async Task CleanUp()
        {
            try
            {
                if (Log != null)
                {
                    await Log.WriteMonitorAsync("", "", "Terminating");
                }

                ApplicationContainer.Dispose();
            }
            catch (Exception ex)
            {
                if (Log != null)
                {
                    await Log.WriteFatalErrorAsync(nameof(Startup), nameof(CleanUp), "", ex);
                }
                throw;
            }
        }

        private ILog CreateLog(IConfiguration configuration,
            IReloadingManager<CandlesHistoryWriterSettings> settings)
        {
            const string tableName = "CandlesHistoryWriterServiceLog";
            var aggregateLogger = new AggregateLogger();
            var settingsValue = settings.CurrentValue;

            if (settings.CurrentValue.UseSerilog)
            {
                aggregateLogger.AddLog(new SerilogLogger(typeof(Startup).Assembly, configuration));
            }
            else if (settingsValue.Db.StorageMode == StorageMode.SqlServer)
            {
                aggregateLogger.AddLog(new LogToSql(new SqlLogRepository(tableName,
                    settingsValue.Db.LogsConnectionString)));
            }
            else if (settingsValue.Db.StorageMode == StorageMode.Azure)
            {
                throw new InvalidOperationException("Azure storage mode is not supported");
            }

            return aggregateLogger;
        }
    }
}
