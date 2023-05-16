using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Ingest;
using Kusto.Data.Net.Client;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using testAPI.Data;
using System.Data;
using System.Runtime.CompilerServices;
using testAPI.Models;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Microsoft.Extensions.Logging;

namespace testAPI.Data
{
    public class KustosClient
    {
        private KustoConnectionStringBuilder kcsb;

        public ICslQueryProvider queryClient;

        public IKustoQueuedIngestClient ingestClient;

        public KustoQueuedIngestionProperties kustoIngestionProperties;
        public KustosClient()
        {
            // 1. Create a connection string to a cluster/database with AAD user authentication
            string cluster = "https://learnkustos.westus.kusto.windows.net/";
            string databaseName = "test";
            string tableName = "WeatherForecast_test";
            var authority = "5a5ed79c-670c-4542-aca8-cec104c8c494"; // Or the AAD tenant GUID: "..."
            var applicationClientId = "1b7b73dc-e114-49f7-b8db-4add54b78bad";
            var applicationKey = "cX.8Q~eCT09My7goMJ~Z~mYkYwe853t_xpJM8cZ0";
            kcsb = new KustoConnectionStringBuilder(cluster, databaseName).WithAadApplicationKeyAuthentication(applicationClientId, applicationKey, authority);
            queryClient = Kusto.Data.Net.Client.KustoClientFactory.CreateCslQueryProvider(this.kcsb);
            ingestClient = Kusto.Ingest.KustoIngestFactory.CreateQueuedIngestClient(this.kcsb);

            kustoIngestionProperties = new KustoQueuedIngestionProperties(databaseName, tableName)
            {
                ReportLevel = IngestionReportLevel.FailuresAndSuccesses,
                ReportMethod = IngestionReportMethod.QueueAndTable,
                Format = DataSourceFormat.json,
                //data format is removed for time being
            };
        }

        public IDataReader getWeatherForecasts()
        {
            // 2. Query the data
            string query = "WeatherForecast_test | take 10";
            IDataReader reader = queryClient.ExecuteQuery(query);
            // 3. return the results
            return reader;
        }

        public string createNewEntry(WeatherForecast weather)
        {
            // Serialize the object to JSON
            string jsonString = JsonSerializer.Serialize(weather);

            // Convert the JSON string to a byte array
            byte[] byteArray = Encoding.UTF8.GetBytes(jsonString);

            MemoryStream stream = new MemoryStream(byteArray);
            IKustoIngestionResult result = ingestClient.IngestFromStream(stream, this.kustoIngestionProperties);
            // Get Ingestion Status.
            IngestionStatus? ingestionStatus = result?.GetIngestionStatusCollection().First();
            if (ingestionStatus != null) 
            { 
                while (ingestionStatus.Status == Status.Pending)
                {
                    // Wait for ingestion to complete.
                    System.Threading.Thread.Sleep(1000);
                    ingestionStatus = result?.GetIngestionStatusCollection().First();
                    if(ingestionStatus == null)
                    {
                        return "Error 500";
                    }
                }
            }
            return "Success 200";
        }
    }
}


