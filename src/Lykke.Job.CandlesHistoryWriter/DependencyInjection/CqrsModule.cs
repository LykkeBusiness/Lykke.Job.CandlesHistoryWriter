// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Autofac;
using BookKeeper.Client.Workflow.Events;
using Common.Log;
using CorporateActions.Broker.Contracts.Workflow;
using Lykke.Cqrs;
using Lykke.Cqrs.Configuration;
using Lykke.Cqrs.Configuration.BoundedContext;
using Lykke.Cqrs.Configuration.Routing;
using Lykke.Cqrs.Middleware.Logging;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;
using Lykke.Job.CandlesHistoryWriter.Services.Workflow;
using Lykke.Job.CandlesHistoryWriter.Workflow;
using Lykke.Messaging.Serialization;
using Lykke.Snow.Common.Correlation.Cqrs;
using Lykke.Snow.Common.Startup;
using Lykke.Snow.Cqrs;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Lykke.Job.CandlesHistoryWriter.DependencyInjection
{
    public class CqrsModule : Module
    {
        private const string DefaultRoute = "self";
        private const string EventsRoute = "events";
        private const string CommandsRoute = "commands";
        private readonly CqrsSettings _settings;
        private readonly ILog _log;
        private readonly long _defaultRetryDelayMs;

        public CqrsModule(CqrsSettings settings, ILog log)
        {
            _settings = settings;
            _log = log;
            _defaultRetryDelayMs = (long) _settings.RetryDelay.TotalMilliseconds;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterInstance(_settings.ContextNames).AsSelf().SingleInstance();
            builder.Register(context => new AutofacDependencyResolver(context)).As<IDependencyResolver>()
                .SingleInstance();
            builder.RegisterInstance(new CqrsContextNamesSettings()).AsSelf().SingleInstance();

            builder.RegisterType<EodStartedProjection>().AsSelf();
            builder.RegisterType<RFactorCommandsHandler>().AsSelf();

            builder.RegisterType<CqrsCorrelationManager>()
                .AsSelf();

            builder.Register(CreateEngine)
                .As<ICqrsEngine>()
                .SingleInstance();
        }

        private RabbitMqCqrsEngine CreateEngine(IComponentContext ctx)
        {
            var rabbitMqConventionEndpointResolver = new RabbitMqConventionEndpointResolver(
                "RabbitMq",
                SerializationFormat.MessagePack,
                environment: _settings.EnvironmentName);
            
            var log = new LykkeLoggerAdapter<CqrsModule>(ctx.Resolve<ILogger<CqrsModule>>());
            
            var registrations = new List<IRegistration>
            {
                Register.DefaultEndpointResolver(rabbitMqConventionEndpointResolver),
                Register.CommandInterceptors(new DefaultCommandLoggingInterceptor(log)),
                Register.EventInterceptors(new DefaultEventLoggingInterceptor(log)),
                RegisterContext(),
            };
            
            var rabbitMqSettings = new ConnectionFactory
            {
                Uri = new Uri(_settings.ConnectionString, UriKind.Absolute)
            };

            var engine = new RabbitMqCqrsEngine(log,
                ctx.Resolve<IDependencyResolver>(),
                new DefaultEndpointProvider(),
                rabbitMqSettings.Endpoint.ToString(),
                rabbitMqSettings.UserName,
                rabbitMqSettings.Password,
                true,
                registrations.ToArray());

            var correlationManager = ctx.Resolve<CqrsCorrelationManager>();
            engine.SetWriteHeadersFunc(correlationManager.BuildCorrelationHeadersIfExists);
            engine.SetReadHeadersAction(correlationManager.FetchCorrelationIfExists);

            return engine;
        }

        private IRegistration RegisterContext()
        {
            var contextRegistration = Register.BoundedContext(_settings.ContextNames.CandlesHistoryWriter)
                .FailedCommandRetryDelay(_defaultRetryDelayMs).ProcessingOptions(CommandsRoute).MultiThreaded(8)
                .QueueCapacity(1024);
            
            RegisterEodProjection(contextRegistration);
            RegisterRFactorCommandsHandler(contextRegistration);

            return contextRegistration;
        }

        private void RegisterEodProjection(
            ProcessingOptionsDescriptor<IBoundedContextRegistration> contextRegistration)
        {
            contextRegistration.ListeningEvents(
                    typeof(EodProcessStartedEvent))
                .From(_settings.ContextNames.BookKeeper)
                .On(EventsRoute)
                .WithProjection(
                    typeof(EodStartedProjection), _settings.ContextNames.BookKeeper);
		}
        
        private void RegisterRFactorCommandsHandler(ProcessingOptionsDescriptor<IBoundedContextRegistration> contextRegistration)
        {
            contextRegistration
                .ListeningCommands(
                    typeof(UpdateHistoricalCandlesCommand)
                )
                .On(CommandsRoute)
                .WithCommandsHandler<RFactorCommandsHandler>()
                .PublishingEvents(
                    typeof(HistoricalCandlesUpdatedEvent)
                )
                .With(EventsRoute);
        }
    }
}
