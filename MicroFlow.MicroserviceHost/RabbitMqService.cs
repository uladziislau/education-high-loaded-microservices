using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using MicroFlow.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;

namespace MicroFlow.MicroserviceHost
{
    public class RabbitMqService : IDisposable
    {
        private IConnection? _connection;
        private IModel? _channel;
        private readonly StartupConfig _startupConfig;
        private readonly MicroserviceInfo _serviceInfo;
        private readonly Dictionary<string, RabbitMqQueueConfig> _rabbitMqConfig;
        private readonly string _instanceId;
        private readonly int _port;

        private int _customLatencyMs = -1;
        private bool _isFailed = false;
        private string _cacheMode = "None";
        private readonly IMemoryCache _localCache = new MemoryCache(new MemoryCacheOptions());
        private static readonly HttpClient _httpClient = new HttpClient();

        private long _processedCount = 0;
        private long _failedCount = 0;
        private double _averageProcessingTimeMs = 0.0;
        private readonly object _metricsLock = new();
        private Timer? _metricsTimer;
        private readonly Random _random = new();

        public RabbitMqService(StartupConfig startupConfig)
        {
            _startupConfig = startupConfig;
            _serviceInfo = startupConfig.ServiceInfo;
            _rabbitMqConfig = startupConfig.RabbitMQConfig;
            _instanceId = startupConfig.InstanceId;
            _port = startupConfig.Port;
        }

        public void Initialize()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            int retryCount = 0;
            int maxRetries = 10;
            while (retryCount < maxRetries)
            {
                try
                {
                    _connection = factory.CreateConnection();
                    _channel = _connection.CreateModel();

                    // Set Prefetch Count to 10 to balance load among replicas
                    _channel.BasicQos(prefetchSize: 0, prefetchCount: 10, global: false);

                    Console.WriteLine($"[{_instanceId}] Connected to RabbitMQ. Prefetch QoS set to 10.");

                    SetupExchangesAndQueues();
                    SetupServiceControlsListener();
                    StartMetricsReporting();
                    return; // Connection successful
                }
                catch (Exception ex)
                {
                    retryCount++;
                    Console.WriteLine($"[{_instanceId}] Connection attempt {retryCount} failed: {ex.Message}. Retrying in 2 seconds...");
                    Thread.Sleep(2000);
                }
            }
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[{_instanceId}] Could not connect to RabbitMQ after {maxRetries} attempts.");
            Console.ResetColor();
        }

        private void SetupExchangesAndQueues()
        {
            if (_channel == null) return;

            // Declare all exchanges defined in the config
            foreach (var queueConfig in _rabbitMqConfig)
            {
                var exchangeName = queueConfig.Key;
                var exchangeType = queueConfig.Value.Type;
                _channel.ExchangeDeclare(exchange: exchangeName, type: exchangeType, durable: true);
            }

            // Bind incoming queues
            foreach (var exchangeName in _serviceInfo.IncomingQueues)
            {
                var exchangeType = _rabbitMqConfig.GetValueOrDefault(exchangeName)?.Type ?? "direct";
                var routingKey = exchangeType == "fanout" ? "" : exchangeName;

                string queueNameToConsume;

                if (exchangeType == "fanout")
                {
                    // Replicas of the SAME service must consume from the SAME queue
                    // so RabbitMQ load-balances fanout events among replicas.
                    queueNameToConsume = $"{exchangeName}_{_serviceInfo.Name}";
                    _channel.QueueDeclare(queue: queueNameToConsume, durable: true, exclusive: false, autoDelete: false, arguments: null);
                }
                else
                {
                    // For Direct exchanges
                    queueNameToConsume = exchangeName;
                    _channel.QueueDeclare(queue: queueNameToConsume, durable: true, exclusive: false, autoDelete: false, arguments: null);
                }

                _channel.QueueBind(queue: queueNameToConsume, exchange: exchangeName, routingKey: routingKey);

                var consumer = new EventingBasicConsumer(_channel);
                consumer.Received += (model, ea) =>
                {
                    // Process message asynchronously in a thread pool thread
                    Task.Run(async () =>
                    {
                        var stopwatch = Stopwatch.StartNew();
                        bool success = false;
                        string correlationId = string.Empty;
                        try
                        {
                            var body = ea.Body.ToArray();
                            var json = Encoding.UTF8.GetString(body);
                            var envelope = JsonSerializer.Deserialize<MessageEnvelope>(json);

                            if (envelope != null)
                            {
                                correlationId = envelope.CorrelationId;
                                await HandleReceivedMessageAsync(envelope);
                                success = true;
                            }
                            
                            _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[{_instanceId}] Error processing message: {ex.Message}");
                            // Requeue message if processing failed
                            _channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                        }
                        finally
                        {
                            stopwatch.Stop();
                            RecordMetrics(success, stopwatch.Elapsed.TotalMilliseconds);
                        }
                    });
                };

                _channel.BasicConsume(queue: queueNameToConsume, autoAck: false, consumer: consumer);
                Console.WriteLine($"[{_instanceId}] Bound and listening on queue '{queueNameToConsume}'");
            }
        }

        private void SetupServiceControlsListener()
        {
            if (_channel == null) return;

            try
            {
                _channel.ExchangeDeclare(exchange: "service_controls", type: "fanout", durable: true);
                var controlQueue = _channel.QueueDeclare().QueueName;
                _channel.QueueBind(queue: controlQueue, exchange: "service_controls", routingKey: "");

                var consumer = new EventingBasicConsumer(_channel);
                consumer.Received += (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var json = Encoding.UTF8.GetString(body);
                        var control = JsonSerializer.Deserialize<ServiceControlMessage>(json);

                        if (control != null && (control.ServiceName == _serviceInfo.Name || control.ServiceName == "*"))
                        {
                            _customLatencyMs = control.LatencyMs;
                            _isFailed = control.IsFailed;
                            _cacheMode = control.CacheMode;
                            Console.WriteLine($"[{_instanceId}] Control update received: Latency={_customLatencyMs}ms, Failed={_isFailed}, CacheMode={_cacheMode}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{_instanceId}] Error in service control listener: {ex.Message}");
                    }
                };

                _channel.BasicConsume(queue: controlQueue, autoAck: true, consumer: consumer);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_instanceId}] Failed to set up service controls listener: {ex.Message}");
            }
        }

        private async Task HandleReceivedMessageAsync(MessageEnvelope envelope)
        {
            if (_isFailed)
            {
                // Simulate service crash/hang
                await Task.Delay(2000);
                throw new Exception($"Simulated Service Outage: {_serviceInfo.Name} is currently unavailable.");
            }

            var message = envelope.Payload;

            if (_serviceInfo.Name == "UserService")
            {
                string key = envelope.CorrelationId;
                string value = string.Empty;

                if (_cacheMode == "Local")
                {
                    if (_localCache.TryGetValue(key, out string? cachedVal) && cachedVal != null)
                    {
                        value = cachedVal;
                    }
                }
                else if (_cacheMode == "Redis" && _startupConfig.CachePort > 0)
                {
                    try
                    {
                        // Simulate network hop latency to external Redis (5ms)
                        await Task.Delay(5);

                        var cacheResponse = await _httpClient.GetAsync($"http://localhost:{_startupConfig.CachePort}/cache/{key}");
                        if (cacheResponse.IsSuccessStatusCode)
                        {
                            value = await cacheResponse.Content.ReadAsStringAsync();
                        }
                    }
                    catch { /* Cache unavailable */ }
                }

                if (string.IsNullOrEmpty(value) && _startupConfig.DatabasePort > 0)
                {
                    // Cache Miss or Cache Disabled - go to DB
                    try
                    {
                        var dbResponse = await _httpClient.GetAsync($"http://localhost:{_startupConfig.DatabasePort}/db/{key}");
                        if (dbResponse.IsSuccessStatusCode)
                        {
                            value = await dbResponse.Content.ReadAsStringAsync();
                            
                            // Save to cache
                            if (_cacheMode == "Local")
                            {
                                _localCache.Set(key, value, TimeSpan.FromSeconds(10));
                            }
                            else if (_cacheMode == "Redis" && _startupConfig.CachePort > 0)
                            {
                                // Simulate network hop serialization & transmission delay (5ms)
                                await Task.Delay(5);

                                var content = new StringContent(JsonSerializer.Serialize(new { value }), Encoding.UTF8, "application/json");
                                await _httpClient.PostAsync($"http://localhost:{_startupConfig.CachePort}/cache/{key}", content);
                            }
                        }
                    }
                    catch { /* DB unavailable */ }
                }

                if (string.IsNullOrEmpty(value))
                {
                    // Fallback to synthetic delay if infrastructure is completely missing
                    var processingDelay = _customLatencyMs >= 0 ? _customLatencyMs : _random.Next(50, 150);
                    if (processingDelay > 0) await Task.Delay(processingDelay);
                }
            }
            else
            {
                // Simulate high-load processing work for other services
                var processingDelay = _customLatencyMs >= 0 ? _customLatencyMs : _random.Next(50, 150);
                if (processingDelay > 0)
                {
                    await Task.Delay(processingDelay);
                }
            }

            // AuthService logic: When a command is received, it publishes an event.
            if (_serviceInfo.Name == "AuthService" && envelope.MessageType == "RegisterUser")
            {
                var responseEvent = "UserRegistered";
                PublishMessage(responseEvent, envelope.CorrelationId, "system_events");
                return;
            }

            // Logic for other services reacting to the "UserRegistered" event.
            if (envelope.MessageType == "UserRegistered")
            {
                switch (_serviceInfo.Name)
                {
                    case "UserService":
                        var newUserEvent = "UserProfileCreated";
                        PublishMessage(newUserEvent, envelope.CorrelationId, "user_events");
                        break;

                    case "NotificationService":
                        // Simulate sending email
                        break;
                }
            }
        }

        public void PublishMessage(string payload, string correlationId, string exchangeName)
        {
            if (_channel == null) return;

            var envelope = MessageEnvelope.Create(payload, correlationId, payload);
            var json = JsonSerializer.Serialize(envelope);
            var body = Encoding.UTF8.GetBytes(json);

            var exchangeType = _rabbitMqConfig.GetValueOrDefault(exchangeName)?.Type ?? "direct";
            var routingKey = exchangeType == "fanout" ? "" : exchangeName;

            _channel.BasicPublish(exchange: exchangeName,
                                 routingKey: routingKey,
                                 basicProperties: null,
                                 body: body);
        }

        private void StartMetricsReporting()
        {
            _metricsTimer = new Timer(PublishMetrics, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        }

        private void PublishMetrics(object? state)
        {
            if (_channel == null || !_channel.IsOpen) return;

            try
            {
                long processed;
                long failed;
                double avgTime;
                
                lock (_metricsLock)
                {
                    processed = _processedCount;
                    failed = _failedCount;
                    avgTime = _averageProcessingTimeMs;
                }

                var metrics = new ReplicaMetrics
                {
                    InstanceId = _instanceId,
                    ServiceName = _serviceInfo.Name,
                    Port = _port,
                    MessagesProcessed = processed,
                    MessagesFailed = failed,
                    AverageProcessingTimeMs = avgTime,
                    MemoryUsageMb = Process.GetCurrentProcess().WorkingSet64 / (1024.0 * 1024.0),
                    Timestamp = DateTime.UtcNow
                };

                var json = JsonSerializer.Serialize(metrics);
                var body = Encoding.UTF8.GetBytes(json);

                _channel.BasicPublish(exchange: "metrics_events",
                                     routingKey: "",
                                     basicProperties: null,
                                     body: body);
            }
            catch
            {
                // Silence metrics publication errors to avoid crashing replica
            }
        }

        private void RecordMetrics(bool success, double elapsedMs)
        {
            lock (_metricsLock)
            {
                if (success)
                {
                    _processedCount++;
                    if (_averageProcessingTimeMs == 0.0)
                    {
                        _averageProcessingTimeMs = elapsedMs;
                    }
                    else
                    {
                        _averageProcessingTimeMs = (0.15 * elapsedMs) + (0.85 * _averageProcessingTimeMs);
                    }
                }
                else
                {
                    _failedCount++;
                }
            }
        }

        public void Dispose()
        {
            _metricsTimer?.Dispose();
            _channel?.Close();
            _connection?.Close();
            GC.SuppressFinalize(this);
        }
    }
}