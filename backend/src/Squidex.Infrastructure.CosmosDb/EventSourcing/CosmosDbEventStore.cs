// ==========================================================================
//  Squidex Headless CMS
// ==========================================================================
//  Copyright (c) Squidex UG (haftungsbeschraenkt)
//  All rights reserved. Licensed under the MIT license.
// ==========================================================================

using System;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace Squidex.Infrastructure.EventSourcing
{
    public sealed partial class CosmosDbEventStore : DisposableObjectBase, IEventStore, IInitializable
    {
        private readonly CosmosClient cosmosClient;
        private readonly Uri collectionUri;
        private readonly Uri databaseUri;

        public JsonSerializerSettings SerializerSettings { get; }

        public string DatabaseId { get; }

        public string MasterKey { get; }

        public Uri ServiceUri => cosmosClient.Endpoint;

        public CosmosDbEventStore(CosmosClient cosmosClient, string masterKey, string database, JsonSerializerSettings serializerSettings)
        {
            Guard.NotNull(cosmosClient, nameof(cosmosClient));
            Guard.NotNull(serializerSettings, nameof(serializerSettings));
            Guard.NotNullOrEmpty(masterKey, nameof(masterKey));
            Guard.NotNullOrEmpty(database, nameof(database));

            this.cosmosClient = cosmosClient;

            databaseUri = cosmosClient.CreateDatabaseAsync(database).Result.Database.Client.Endpoint;
            DatabaseId = database;

            cosmosClient.
            collectionUri = UriFactory.CreateDocumentCollectionUri(database, Constants.Collection);

            MasterKey = masterKey;

            SerializerSettings = serializerSettings;
        }

        protected override void DisposeObject(bool disposing)
        {
            if (disposing)
            {
                cosmosClient.Dispose();
            }
        }

        public async Task InitializeAsync(CancellationToken ct = default)
        {
            await cosmosClient.CreateDatabaseIfNotExistsAsync(new Database { Id = DatabaseId });

            await cosmosClient.CreateDocumentCollectionIfNotExistsAsync(databaseUri,
                new DocumentCollection
                {
                    PartitionKey = new PartitionKeyDefinition
                    {
                        Paths = new Collection<string>
                        {
                            "/id"
                        }
                    },
                    Id = Constants.LeaseCollection
                });

            await cosmosClient.CreateDocumentCollectionIfNotExistsAsync(databaseUri,
                new DocumentCollection
                {
                    PartitionKey = new PartitionKeyDefinition
                    {
                        Paths = new Collection<string>
                        {
                            "/eventStream"
                        }
                    },
                    IndexingPolicy = new IndexingPolicy
                    {
                        IncludedPaths = new Collection<IncludedPath>
                        {
                            new IncludedPath
                            {
                                Path = "/*",
                                Indexes = new Collection<Index>
                                {
                                    Index.Range(DataType.Number),
                                    Index.Range(DataType.String)
                                }
                            }
                        }
                    },
                    UniqueKeyPolicy = new UniqueKeyPolicy
                    {
                        UniqueKeys = new Collection<UniqueKey>
                        {
                            new UniqueKey
                            {
                                Paths = new Collection<string>
                                {
                                    "/eventStream",
                                    "/eventStreamOffset"
                                }
                            }
                        }
                    },
                    Id = Constants.Collection
                },
                new RequestOptions
                {
                    PartitionKey = new PartitionKey("/eventStream")
                });
        }
    }
}
