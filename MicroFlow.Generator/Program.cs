using System;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using MicroFlow.Core;

namespace MicroFlow.Generator;

public class Program
{
    private static readonly ConcurrentDictionary<string, ReplicaMetrics> _latestMetrics = new();
    private static readonly CancellationTokenSource _cts = new();
    private static MicroservicesHoster _hoster = null!;
    private static MicroserviceConfig _config = null!;
    
    private static bool _autoScalingEnabled = false;
    private static int _simulatorRate = 50; // Requests per second default

    private static readonly Dictionary<string, int> _serviceLatencies = new()
    {
        ["AuthService"] = -1,
        ["UserService"] = -1,
        ["NotificationService"] = -1
    };

    private static readonly Dictionary<string, bool> _serviceFailures = new()
    {
        ["AuthService"] = false,
        ["UserService"] = false,
        ["NotificationService"] = false
    };

    private static readonly Dictionary<string, string> _serviceCacheMode = new()
    {
        ["AuthService"] = "None",
        ["UserService"] = "None",
        ["NotificationService"] = "None"
    };

    public static async Task Main(string[] args)
    {
        var assemblyLocation = Assembly.GetExecutingAssembly().Location;
        var executionDirectory = Path.GetDirectoryName(assemblyLocation);

        if (string.IsNullOrEmpty(executionDirectory))
        {
            Console.WriteLine("Error: Could not determine execution directory.");
            return;
        }

        var configPath = Path.Combine(executionDirectory, "microservices.json");
        if (!File.Exists(configPath))
        {
            Console.WriteLine($"Error: Configuration file not found at '{configPath}'.");
            return;
        }

        // Generate dynamic ports for replicas
        var generatedConfig = await MicroservicesGenerator.GenerateMicroservicesAsync(configPath);
        if (generatedConfig == null)
        {
            Console.WriteLine("Error: Failed to generate microservice configuration.");
            return;
        }
        _config = generatedConfig;

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("==================================================");
        Console.WriteLine("          MICROFLOW SIMULATION INITIALIZER        ");
        Console.WriteLine("==================================================");
        Console.ResetColor();

        // 1. Start all Microservices Replicas
        _hoster = new MicroservicesHoster(_config);
        await _hoster.StartServicesAsync();

        Console.WriteLine("\nAll microservice replicas launched.");
        Console.WriteLine("Waiting 5 seconds for RabbitMQ initializations...");
        await Task.Delay(5000);

        // 2. Start the Metrics Consumer in background
        var metricsTask = Task.Run(() => StartMetricsConsumer(_cts.Token));

        // 3. Start the Simulator process in the background
        var hostDllPath = Path.Combine(executionDirectory, "MicroFlow.MicroserviceHost.dll");
        var simulatorTask = Task.Run(async () =>
        {
            await Task.Delay(1000); // Small buffer before starting load
            try
            {
                await _hoster.StartSimulatorAsync(hostDllPath);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Simulator Exception: {ex.Message}");
            }
        });

        // 4. Start the Autoscaler loop
        var autoscaleTask = Task.Run(() => StartAutoscalerLoop(_cts.Token));

        // 5. Start Kestrel Web Server
        var builder = WebApplication.CreateBuilder(args);
        builder.Services.AddCors(options =>
        {
            options.AddDefaultPolicy(policy =>
            {
                policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
            });
        });
        
        var app = builder.Build();
        
        app.UseDefaultFiles();
        app.UseStaticFiles();
        app.UseCors();

        // SSE Endpoint to stream metrics to the frontend
        app.MapGet("/api/metrics/stream", async (HttpContext context, CancellationToken clientToken) =>
        {
            context.Response.Headers.Append("Content-Type", "text/event-stream");
            context.Response.Headers.Append("Cache-Control", "no-cache");
            context.Response.Headers.Append("Connection", "keep-alive");

            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(clientToken, _cts.Token);
            var token = combinedCts.Token;

            while (!token.IsCancellationRequested)
            {
                // Sync current active replicas list to verify online states
                var activeList = _hoster.GetActiveReplicas();
                
                var data = new
                {
                    AutoScaling = _autoScalingEnabled,
                    SimulatorRate = _simulatorRate,
                    ServiceLatencies = _serviceLatencies,
                    ServiceFailures = _serviceFailures,
                    ServiceCacheMode = _serviceCacheMode,
                    Replicas = activeList.Select(rep => {
                        _latestMetrics.TryGetValue(rep.InstanceId, out var m);
                        return new {
                            InstanceId = rep.InstanceId,
                            ServiceName = rep.ServiceName,
                            Port = rep.Port,
                            Processed = m?.MessagesProcessed ?? 0,
                            Failed = m?.MessagesFailed ?? 0,
                            LatencyMs = m?.AverageProcessingTimeMs ?? 0.0,
                            MemoryMb = m?.MemoryUsageMb ?? 0.0,
                            LastSeen = m?.Timestamp ?? DateTime.MinValue
                        };
                    }).ToList(),
                    Simulator = _latestMetrics.TryGetValue("Simulator-1", out var sim) ? new {
                        Processed = sim.MessagesProcessed,
                        InFlight = sim.MessagesFailed,
                        RttMs = sim.AverageProcessingTimeMs,
                        MemoryMb = sim.MemoryUsageMb
                    } : null
                };

                var json = JsonSerializer.Serialize(data);
                var formatted = $"data: {json}\n\n";
                await context.Response.WriteAsync(formatted, token);
                await context.Response.Body.FlushAsync(token);

                await Task.Delay(1000, token);
            }
        });

        // Endpoint to dynamically scale services
        app.MapPost("/api/scale", async (HttpContext context) =>
        {
            using var document = await JsonDocument.ParseAsync(context.Request.Body);
            var root = document.RootElement;
            var serviceName = root.GetProperty("serviceName").GetString();
            var replicas = root.GetProperty("replicas").GetInt32();

            if (string.IsNullOrEmpty(serviceName) || replicas < 1 || replicas > 5)
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync("Invalid parameters. Replicas count must be between 1 and 5.");
                return;
            }

            Console.WriteLine($"[Web API] Received scale request for '{serviceName}' to {replicas} replicas.");
            _hoster.ScaleService(serviceName, replicas);
            await context.Response.WriteAsync("Success");
        });

        // Endpoint to adjust Simulator traffic load
        app.MapPost("/api/load", async (HttpContext context) =>
        {
            using var document = await JsonDocument.ParseAsync(context.Request.Body);
            var root = document.RootElement;
            var rate = root.GetProperty("rate").GetInt32();

            if (rate < 0 || rate > 250)
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync("Invalid rate. Must be between 0 and 250 requests/sec.");
                return;
            }

            _simulatorRate = rate;
            Console.WriteLine($"[Web API] Changing Simulator load rate to {rate} RPS.");
            PublishLoadRateToRabbitMQ(rate);
            await context.Response.WriteAsync("Success");
        });

        // Endpoint to toggle Auto-scaling
        app.MapPost("/api/autoscale", async (HttpContext context) =>
        {
            using var document = await JsonDocument.ParseAsync(context.Request.Body);
            var root = document.RootElement;
            var enabled = root.GetProperty("enabled").GetBoolean();

            _autoScalingEnabled = enabled;
            Console.WriteLine($"[Web API] Auto-scaling mode toggled to: {enabled}");
            await context.Response.WriteAsync("Success");
        });

        // Endpoint to set service latency / DB response delay
        app.MapPost("/api/service/latency", async (HttpContext context) =>
        {
            using var document = await JsonDocument.ParseAsync(context.Request.Body);
            var root = document.RootElement;
            var serviceName = root.GetProperty("serviceName").GetString();
            var latencyMs = root.GetProperty("latencyMs").GetInt32();

            if (string.IsNullOrEmpty(serviceName) || latencyMs < -1 || latencyMs > 2000)
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync("Invalid parameters.");
                return;
            }

            _serviceLatencies[serviceName] = latencyMs;
            _serviceFailures.TryGetValue(serviceName, out var isFailed);
            _serviceCacheMode.TryGetValue(serviceName, out var cacheMode);

            Console.WriteLine($"[Web API] Changing service latency for '{serviceName}' to {latencyMs}ms.");
            PublishServiceControl(new ServiceControlMessage
            {
                ServiceName = serviceName,
                LatencyMs = latencyMs,
                IsFailed = isFailed,
                CacheMode = cacheMode ?? "None"
            });
            await context.Response.WriteAsync("Success");
        });

        // Endpoint to toggle service failure state (Chaos Engineering)
        app.MapPost("/api/service/failure", async (HttpContext context) =>
        {
            using var document = await JsonDocument.ParseAsync(context.Request.Body);
            var root = document.RootElement;
            var serviceName = root.GetProperty("serviceName").GetString();
            var isFailed = root.GetProperty("isFailed").GetBoolean();

            if (string.IsNullOrEmpty(serviceName))
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync("Invalid parameters.");
                return;
            }

            _serviceFailures[serviceName] = isFailed;
            _serviceLatencies.TryGetValue(serviceName, out var latencyMs);
            _serviceCacheMode.TryGetValue(serviceName, out var cacheMode);

            Console.WriteLine($"[Web API] Setting failure state for '{serviceName}' to: {isFailed}.");
            PublishServiceControl(new ServiceControlMessage
            {
                ServiceName = serviceName,
                LatencyMs = latencyMs,
                IsFailed = isFailed,
                CacheMode = cacheMode ?? "None"
            });
            await context.Response.WriteAsync("Success");
        });

        app.MapPost("/api/service/cache", async (HttpContext context) =>
        {
            using var document = await JsonDocument.ParseAsync(context.Request.Body);
            var root = document.RootElement;
            var serviceName = root.GetProperty("serviceName").GetString();
            var cacheMode = root.GetProperty("cacheMode").GetString();

            if (string.IsNullOrEmpty(serviceName))
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsync("Invalid parameters.");
                return;
            }

            _serviceCacheMode[serviceName] = cacheMode ?? "None";
            _serviceLatencies.TryGetValue(serviceName, out var latencyMs);
            _serviceFailures.TryGetValue(serviceName, out var isFailed);

            Console.WriteLine($"[Web API] Setting cache mode for '{serviceName}' to: {cacheMode}.");
            PublishServiceControl(new ServiceControlMessage
            {
                ServiceName = serviceName,
                LatencyMs = latencyMs,
                IsFailed = isFailed,
                CacheMode = cacheMode ?? "None"
            });
            await context.Response.WriteAsync("Success");
        });

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("\n==================================================");
        Console.WriteLine("  WEB DASHBOARD RUNNING AT: http://localhost:5000 ");
        Console.WriteLine("==================================================");
        Console.ResetColor();

        // Run Kestrel server on port 5000
        app.Urls.Add("http://localhost:5000");
        await app.RunAsync(_cts.Token);

        // Cleanup on shutdown
        _cts.Cancel();
        _hoster.StopServices();
    }

    private static void StartMetricsConsumer(CancellationToken token)
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        IConnection? connection = null;
        IModel? channel = null;
        int retryCount = 0;
        int maxRetries = 10;

        while (retryCount < maxRetries && !token.IsCancellationRequested)
        {
            try
            {
                connection = factory.CreateConnection();
                channel = connection.CreateModel();
                break;
            }
            catch (Exception ex)
            {
                retryCount++;
                Console.WriteLine($"[Metrics Consumer] RabbitMQ connection attempt {retryCount} failed: {ex.Message}. Retrying in 2 seconds...");
                Thread.Sleep(2000);
            }
        }

        if (connection == null || channel == null)
        {
            Console.WriteLine("[Metrics Consumer Error] Failed to connect to RabbitMQ.");
            return;
        }

        using (connection)
        using (channel)
        {
            try
            {
                channel.ExchangeDeclare(exchange: "metrics_events", type: "fanout", durable: true);
                var queueName = channel.QueueDeclare().QueueName;
                channel.QueueBind(queue: queueName, exchange: "metrics_events", routingKey: "");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var json = Encoding.UTF8.GetString(body);
                        var metrics = JsonSerializer.Deserialize<ReplicaMetrics>(json);

                        if (metrics != null)
                        {
                            _latestMetrics[metrics.InstanceId] = metrics;
                        }
                    }
                    catch
                    {
                        // Ignore invalid payloads
                    }
                };

                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

                while (!token.IsCancellationRequested)
                {
                    Thread.Sleep(200);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Metrics Consumer Error] Loop exception: {ex.Message}");
            }
        }
    }

    private static void PublishLoadRateToRabbitMQ(int rate)
    {
        try
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.ExchangeDeclare(exchange: "simulator_controls", type: "fanout", durable: true);
            var payload = JsonSerializer.Serialize(new { rate = rate });
            var body = Encoding.UTF8.GetBytes(payload);

            channel.BasicPublish(exchange: "simulator_controls",
                                 routingKey: "",
                                 basicProperties: null,
                                 body: body);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[RabbitMQ Error] Failed to publish load change command: {ex.Message}");
        }
    }

    private static void PublishServiceControl(ServiceControlMessage control)
    {
        try
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.ExchangeDeclare(exchange: "service_controls", type: "fanout", durable: true);
            var payload = JsonSerializer.Serialize(control);
            var body = Encoding.UTF8.GetBytes(payload);

            channel.BasicPublish(exchange: "service_controls",
                                 routingKey: "",
                                 basicProperties: null,
                                 body: body);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[RabbitMQ Error] Failed to publish service control command: {ex.Message}");
        }
    }

    private static async Task StartAutoscalerLoop(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(2500, token); // Check scaling every 2.5 seconds

                if (!_autoScalingEnabled) continue;

                // Query simulator metrics
                if (_latestMetrics.TryGetValue("Simulator-1", out var simMetrics))
                {
                    var rtt = simMetrics.AverageProcessingTimeMs;
                    var inFlight = simMetrics.MessagesFailed; // Items currently in-flight

                    // Evaluate scale criteria
                    // If RTT is high (> 250ms) or we have a backlog of requests in-flight (> 25 items), we need more workers
                    if (rtt > 220 || inFlight > 20)
                    {
                        // Identify which service is slowest
                        var slowestService = GetSlowestService();
                        if (!string.IsNullOrEmpty(slowestService))
                        {
                            var serviceInfo = _config.Microservices.FirstOrDefault(s => s.Name.Equals(slowestService, StringComparison.OrdinalIgnoreCase));
                            if (serviceInfo != null && serviceInfo.Replicas < 5)
                            {
                                var nextReplicas = serviceInfo.Replicas + 1;
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine($"[Autoscaler] E2E RTT ({rtt:F1}ms) or In-Flight ({inFlight}) is high. Scaling UP '{slowestService}' to {nextReplicas} replicas.");
                                Console.ResetColor();
                                _hoster.ScaleService(slowestService, nextReplicas);
                            }
                        }
                    }
                    // If RTT is low (< 160ms) and we have very few in-flight requests (< 5), and some service has > 1 replica, scale down
                    else if (rtt < 160 && inFlight < 5)
                    {
                        // Scale down the service with the most replicas (above 1)
                        var mostScaledService = _config.Microservices
                            .Where(s => s.Replicas > 1 && !s.Name.Equals("Simulator", StringComparison.OrdinalIgnoreCase))
                            .OrderByDescending(s => s.Replicas)
                            .FirstOrDefault();

                        if (mostScaledService != null)
                        {
                            var nextReplicas = mostScaledService.Replicas - 1;
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"[Autoscaler] E2E RTT ({rtt:F1}ms) is stable. Scaling DOWN '{mostScaledService.Name}' to {nextReplicas} replicas.");
                            Console.ResetColor();
                            _hoster.ScaleService(mostScaledService.Name, nextReplicas);
                        }
                    }
                }
            }
            catch (TaskCanceledException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"[Autoscaler Error] Exception: {ex.Message}");
            }
        }
    }

    private static string GetSlowestService()
    {
        // Simple heuristic: compare average processing time across Auth and User services
        double authLatency = 0;
        double userLatency = 0;

        foreach (var m in _latestMetrics.Values)
        {
            if (m.ServiceName.Equals("AuthService", StringComparison.OrdinalIgnoreCase))
            {
                authLatency = Math.Max(authLatency, m.AverageProcessingTimeMs);
            }
            else if (m.ServiceName.Equals("UserService", StringComparison.OrdinalIgnoreCase))
            {
                userLatency = Math.Max(userLatency, m.AverageProcessingTimeMs);
            }
        }

        // Return the service with the highest processing latency
        if (authLatency > userLatency) return "AuthService";
        if (userLatency > 0) return "UserService";
        
        return "AuthService"; // Default fallback
    }
}