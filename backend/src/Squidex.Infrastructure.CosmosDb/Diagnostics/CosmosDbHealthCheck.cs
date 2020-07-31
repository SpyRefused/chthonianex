// ==========================================================================
//  Squidex Headless CMS
// ==========================================================================
//  Copyright (c) Squidex UG (haftungsbeschraenkt)
//  All rights reserved. Licensed under the MIT license.
// ==========================================================================

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Squidex.Infrastructure.Diagnostics
{
    public class CosmosDbHealthCheck : IHealthCheck
    {
        private static readonly ConcurrentDictionary<string, CosmosClient> Connections = new ConcurrentDictionary<string, CosmosClient>();
        private readonly string connectionString;

        public CosmosDbHealthCheck(string connectionString)
        {
            this.connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                if (!Connections.TryGetValue(connectionString, out var cosmosDbClient))
                {
                    cosmosDbClient = new CosmosClient(connectionString);

                    if (!Connections.TryAdd(connectionString, cosmosDbClient))
                    {
                        cosmosDbClient.Dispose();
                        cosmosDbClient = Connections[connectionString];
                    }
                }

                await cosmosDbClient.ReadAccountAsync();
                return HealthCheckResult.Healthy();
            }
            catch (Exception ex)
            {
                return new HealthCheckResult(context.Registration.FailureStatus, exception: ex);
            }
        }
    }
}