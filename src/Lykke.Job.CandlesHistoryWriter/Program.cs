﻿// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Threading.Tasks;
using Autofac.Extensions.DependencyInjection;
using JetBrains.Annotations;
using Lykke.SettingsReader.ConfigurationProvider;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace Lykke.Job.CandlesHistoryWriter
{
    [UsedImplicitly]
    internal class Program
    {
        internal static IHost AppHost { get; private set; }

        public static string EnvInfo => Environment.GetEnvironmentVariable("ENV_INFO");

        // ReSharper disable once UnusedParameter.Local
        private static async Task Main(string[] args)
        {
            Console.WriteLine($"Lykke.Job.CandlesHistoryWriter version {Microsoft.Extensions.PlatformAbstractions.PlatformServices.Default.Application.ApplicationVersion}");
#if DEBUG
            Console.WriteLine("Is DEBUG");
#else
            Console.WriteLine("Is RELEASE");
#endif
            Console.WriteLine($"ENV_INFO: {EnvInfo}");

            AppDomain.CurrentDomain.UnhandledException += (s, e) =>
            {
                Console.WriteLine($"Unhandled exception: {e?.ExceptionObject}");

                if (e?.IsTerminating == true)
                {
                    Console.WriteLine("Terminating...");
                }
            };

            try
            {
                var configuration = new ConfigurationBuilder()
                    .AddHttpSourceConfiguration()
                    .AddEnvironmentVariables()
                    .Build();

                AppHost = Host.CreateDefaultBuilder()
                    .UseServiceProviderFactory(new AutofacServiceProviderFactory())
                    .ConfigureWebHostDefaults(webBuilder =>
                    {
                        webBuilder.ConfigureKestrel(serverOptions =>
                            {
                                // Set properties and call methods on options
                            })
                            .UseUrls("http://*:5000")
                            .UseConfiguration(configuration)
                            .UseContentRoot(Directory.GetCurrentDirectory())
                            .UseStartup<Startup>();
                    })
                    .Build();

                await AppHost.RunAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Fatal error:");
                Console.WriteLine(ex);

                // Lets devops to see startup error in console between restarts in the Kubernetes
                var delay = TimeSpan.FromMinutes(1);

                Console.WriteLine();
                Console.WriteLine($"Process will be terminated in {delay}. Press any key to terminate immediately.");

                await Task.WhenAny(
                        Task.Delay(delay),
                        Task.Run(() =>
                        {
                            Console.ReadKey(true);
                        }));
            }

            Console.WriteLine("Terminated");
        }
    }
}
