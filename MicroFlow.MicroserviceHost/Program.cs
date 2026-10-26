using System;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using MicroFlow.Core;
using MicroFlow.MicroserviceHost;

// Check for simulator mode
if (args.Length > 0 && args[0] == "--run-simulator")
{
    await MicroservicesSimulator.Run();
    return;
}

// --- Standard Microservice Startup Logic ---

var builder = WebApplication.CreateBuilder(args);

if (args.Length == 0)
{
    throw new ArgumentException("Microservice startup configuration (Base64) is required as a command-line argument.");
}

var base64EncodedConfig = args[0];
var jsonConfig = Encoding.UTF8.GetString(Convert.FromBase64String(base64EncodedConfig));
var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
var startupConfig = JsonSerializer.Deserialize<StartupConfig>(jsonConfig, options);

if (startupConfig == null)
{
    throw new InvalidOperationException("Failed to deserialize microservice startup configuration.");
}

var microserviceInfo = startupConfig.ServiceInfo;
var instanceId = startupConfig.InstanceId;
var port = startupConfig.Port;

// Configure the web server to listen on the specified port
builder.WebHost.UseUrls($"http://localhost:{port}");

var app = builder.Build();

// Initialize RabbitMQ Service with full configuration
var rabbitMqService = new RabbitMqService(startupConfig);
rabbitMqService.Initialize();

// Ensure RabbitMQ connection is closed on application shutdown
app.Lifetime.ApplicationStopping.Register(() =>
{
    rabbitMqService.Dispose();
    Console.WriteLine($"[{instanceId}] RabbitMQ connection closed.");
});

app.MapGet("/", () => $"Hello from {instanceId}!");

Console.WriteLine($"'{instanceId}' is running and listening on http://localhost:{port}");

app.Run();