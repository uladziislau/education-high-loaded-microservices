using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using MicroFlow.Core;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MicroFlow.MicroserviceHost
{
    public class MicroservicesSimulator
    {
        private static readonly ConcurrentDictionary<string, DateTime> _inFlightRequests = new();
        private static readonly CancellationTokenSource _cts = new();
        
        private static long _sentCount = 0;
        private static long _completedCount = 0;
        private static double _averageRttMs = 0.0;
        private static readonly object _statsLock = new();
        private static Timer? _metricsTimer;

        private static volatile int _intervalMs = 20; // 50 RPS (1000 / 50 = 20ms)
        private static volatile bool _isPaused = false;

        public static async Task Run()
        {
            Console.WriteLine("==================================================");
            Console.WriteLine("   STARTING HIGH-LOAD MICROSERVICES SIMULATOR    ");
            Console.WriteLine("==================================================");

            var factory = new ConnectionFactory() { HostName = "localhost" };
            IConnection? connection = null;
            IModel? publishChannel = null;
            IModel? consumeChannel = null;
            int retryCount = 0;
            int maxRetries = 10;

            while (retryCount < maxRetries)
            {
                try
                {
                    connection = factory.CreateConnection();
                    publishChannel = connection.CreateModel();
                    consumeChannel = connection.CreateModel();
                    break;
                }
                catch (Exception ex)
                {
                    retryCount++;
                    Console.WriteLine($"[Simulator] RabbitMQ connection attempt {retryCount} failed: {ex.Message}. Retrying in 2 seconds...");
                    await Task.Delay(2000);
                }
            }

            if (connection == null || publishChannel == null || consumeChannel == null)
            {
                Console.WriteLine("[Simulator Error] Could not establish RabbitMQ connections.");
                return;
            }

            using (connection)
            using (publishChannel)
            using (consumeChannel)
            {
                // Set up metrics reporting timer
                StartMetricsReporting(publishChannel);

                // Subscribe to controls queue (simulator_controls)
                consumeChannel.ExchangeDeclare(exchange: "simulator_controls", type: "fanout", durable: true);
                var controlsQueueName = consumeChannel.QueueDeclare().QueueName;
                consumeChannel.QueueBind(queue: controlsQueueName, exchange: "simulator_controls", routingKey: "");
                
                var controlsConsumer = new EventingBasicConsumer(consumeChannel);
                controlsConsumer.Received += (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var json = Encoding.UTF8.GetString(body);
                        using var doc = JsonDocument.Parse(json);
                        if (doc.RootElement.TryGetProperty("rate", out var rateProp))
                        {
                            int rate = rateProp.GetInt32();
                            if (rate <= 0)
                            {
                                _isPaused = true;
                                Console.WriteLine("[Simulator] Traffic generation paused.");
                            }
                            else
                            {
                                _isPaused = false;
                                _intervalMs = 1000 / rate;
                                Console.WriteLine($"[Simulator] Traffic rate changed to {rate} RPS (interval {_intervalMs}ms).");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Simulator Error] Failed to parse control message: {ex.Message}");
                    }
                };
                consumeChannel.BasicConsume(queue: controlsQueueName, autoAck: true, consumer: controlsConsumer);

                // Subscribe to completion events (user_events)
                var responseQueueName = consumeChannel.QueueDeclare().QueueName;
                consumeChannel.QueueBind(queue: responseQueueName, exchange: "user_events", routingKey: "user_events");

                var consumer = new EventingBasicConsumer(consumeChannel);
                consumer.Received += (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var json = Encoding.UTF8.GetString(body);
                        var envelope = JsonSerializer.Deserialize<MessageEnvelope>(json);

                        if (envelope != null && envelope.MessageType == "UserProfileCreated")
                        {
                            var correlationId = envelope.CorrelationId;
                            if (_inFlightRequests.TryRemove(correlationId, out var startTime))
                            {
                                var latency = (DateTime.UtcNow - startTime).TotalMilliseconds;
                                lock (_statsLock)
                                {
                                    _completedCount++;
                                    if (_averageRttMs == 0.0)
                                    {
                                        _averageRttMs = latency;
                                    }
                                    else
                                    {
                                        _averageRttMs = (0.1 * latency) + (0.9 * _averageRttMs);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Simulator Error] Failed to process completion event: {ex.Message}");
                    }
                };

                consumeChannel.BasicConsume(queue: responseQueueName, autoAck: true, consumer: consumer);
                Console.WriteLine("[Simulator] Listening for completion events to measure E2E RTT...");

                // Register cancellation handler
                Console.CancelKeyPress += (sender, eventArgs) =>
                {
                    eventArgs.Cancel = true;
                    _cts.Cancel();
                };

                while (!_cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        if (_isPaused)
                        {
                            await Task.Delay(500, _cts.Token);
                            continue;
                        }

                        var correlationId = Guid.NewGuid().ToString();
                        var envelope = MessageEnvelope.Create("RegisterUser", correlationId, "RegisterUser");
                        var json = JsonSerializer.Serialize(envelope);
                        var body = Encoding.UTF8.GetBytes(json);

                        _inFlightRequests.TryAdd(correlationId, DateTime.UtcNow);
                        
                        publishChannel.BasicPublish(exchange: "auth_commands",
                                             routingKey: "auth_commands",
                                             basicProperties: null,
                                             body: body);

                        Interlocked.Increment(ref _sentCount);

                        // Capture dynamic interval
                        int currentInterval = _intervalMs;
                        await Task.Delay(currentInterval > 0 ? currentInterval : 1, _cts.Token);
                    }
                    catch (TaskCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Simulator Error] Failed to publish message: {ex.Message}");
                        await Task.Delay(1000, _cts.Token);
                    }
                }

                _metricsTimer?.Dispose();
                Console.WriteLine("[Simulator] Load generation stopped.");
            }
        }

        private static void StartMetricsReporting(IModel channel)
        {
            _metricsTimer = new Timer((state) =>
            {
                try
                {
                    long completed;
                    double avgLatency;
                    long inFlight = _inFlightRequests.Count;

                    lock (_statsLock)
                    {
                        completed = _completedCount;
                        avgLatency = _averageRttMs;
                    }

                    var metrics = new ReplicaMetrics
                    {
                        InstanceId = "Simulator-1",
                        ServiceName = "Simulator",
                        Port = 0,
                        MessagesProcessed = completed, // Completed requests
                        MessagesFailed = inFlight,      // Currently in-flight
                        AverageProcessingTimeMs = avgLatency,
                        MemoryUsageMb = Process.GetCurrentProcess().WorkingSet64 / (1024.0 * 1024.0),
                        Timestamp = DateTime.UtcNow
                    };

                    var json = JsonSerializer.Serialize(metrics);
                    var body = Encoding.UTF8.GetBytes(json);

                    channel.BasicPublish(exchange: "metrics_events",
                                         routingKey: "",
                                         basicProperties: null,
                                         body: body);
                }
                catch
                {
                    // Ignore publisher errors
                }
            }, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        }
    }
}