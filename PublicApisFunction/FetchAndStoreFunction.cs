using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using System.IO;
using Newtonsoft.Json.Linq;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using System.Linq;

public static class FetchAndStoreFunction
{
    private static readonly HttpClient httpClient = new HttpClient();
    private static string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

    [FunctionName("FetchAndStoreFunction")]
    public static async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer, ILogger log)
    {
        log.LogInformation($"Trigger function executed at: {DateTime.Now} timestamp");

        var response = await httpClient.GetAsync("https://api.publicapis.org/random?auth=null");
        var content = await response.Content.ReadAsStringAsync();
        var success = response.IsSuccessStatusCode;
        var timestamp = DateTime.UtcNow;

        var tableClient = new TableClient(storageConnectionString, "LogTable");
        await tableClient.CreateIfNotExistsAsync();

        var blobServiceClient = new BlobServiceClient(storageConnectionString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient("payloads");
        await blobContainerClient.CreateIfNotExistsAsync();

        var blobName = $"{timestamp:yyyyMMddHHmmssfff}.json";
        var blobClient = blobContainerClient.GetBlobClient(blobName);

        using (var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content)))
        {
            await blobClient.UploadAsync(stream, true);
        }

        var logEntity = new TableEntity
        {
            PartitionKey = "Log",
            RowKey = Guid.NewGuid().ToString(),
            ["Timestamp"] = timestamp,
            ["Success"] = success,
            ["BlobName"] = blobName
        };
        await tableClient.AddEntityAsync(logEntity);

        log.LogInformation($"Logged the data {(success ? "success" : "fail")} and stored the payload in the blob store");
    }
}

public static class LogListFunction
{
    private static string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

    [FunctionName("LogListFunction")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "logs/{from}/{to}")] HttpRequest req,
        string from,
        string to,
        ILogger log)
    {
        log.LogInformation("HTTP function triggered");

        DateTime fromDate, toDate;
        if (!DateTime.TryParse(from, out fromDate) || !DateTime.TryParse(to, out toDate))
        {
            return new BadRequestObjectResult("Enter valid from and to dates in the format of: yyyy-MM-dd.");
        }

        var tableClient = new TableClient(storageConnectionString, "LogTable");
        await tableClient.CreateIfNotExistsAsync();
        
        var query = tableClient.QueryAsync<TableEntity>(filter: $"Timestamp {DateTime.UtcNow.AddDays(-1)} and Timestamp {DateTime.UtcNow}");
        var logs = await query.ToListAsync();


        return new OkObjectResult(logs);
    }
}

public static class FetchPayloadFunction
{
    private static string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

    [FunctionName("FetchPayloadFunction")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "payload/{blobName}")] HttpRequest req,
        string blobName,
        ILogger log)
    {
        log.LogInformation("HTTP trigger fetch a payload.");

        var blobServiceClient = new BlobServiceClient(storageConnectionString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient("payloads");
        var blobClient = blobContainerClient.GetBlobClient(blobName);

        if (!await blobClient.ExistsAsync())
        {
            return new NotFoundResult();
        }

        var downloadInfo = await blobClient.DownloadAsync();
        return new FileStreamResult(downloadInfo.Value.Content, downloadInfo.Value.ContentType);
    }
}