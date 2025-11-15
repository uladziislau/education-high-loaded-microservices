using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using UDIE.EHLM.Core;
using System;
using System.Collections.Generic;
using System.Text;

namespace UDIE.EHLM.MicroserviceHost
{
    public class RabbitMqService : IDisposable
    {
        private IConnection? _connection;
        private IModel? _channel;
        private readonly MicroserviceInfo _serviceInfo;
        private readonly Dictionary<string, RabbitMqQueueConfig> _rabbitMqConfig;

        public RabbitMqService(MicroserviceInfo serviceInfo, Dictionary<string, RabbitMqQueueConfig> rabbitMqConfig)
        {
            _serviceInfo = serviceInfo;
            _rabbitMqConfig = rabbitMqConfig;
        }

        public void Initialize()
        {
            try
            {
                var factory = new ConnectionFactory() { HostName = "localhost" };
                _connection = factory.CreateConnection();
                _channel = _connection.CreateModel();

                Console.WriteLine($"[{_serviceInfo.Name}] Successfully connected to RabbitMQ.");

                SetupExchangesAndQueues();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[{_serviceInfo.Name}] Could not initialize RabbitMQ: {ex.Message}");
                Console.ResetColor();
            }
        }

        private void SetupExchangesAndQueues()
        {
            if (_channel == null) return;

            Console.WriteLine($"[{_serviceInfo.Name}] Setting up exchanges and queues...");

            // Declare all exchanges defined in the config
            foreach (var queueConfig in _rabbitMqConfig)
            {
                var exchangeName = queueConfig.Key;
                var exchangeType = queueConfig.Value.Type;
                _channel.ExchangeDeclare(exchange: exchangeName, type: exchangeType, durable: true);
                Console.WriteLine($"[{_serviceInfo.Name}] Declared exchange '{exchangeName}' of type '{exchangeType}'.");
            }

            // For each incoming "queue" (which is really an exchange in our design) for this service...
            foreach (var exchangeName in _serviceInfo.IncomingQueues)
            {
                var exchangeType = _rabbitMqConfig.GetValueOrDefault(exchangeName)?.Type ?? "direct";
                var routingKey = exchangeType == "fanout" ? "" : exchangeName; // Fanout ignores routing key

                string queueNameToConsume;

                if (exchangeType == "fanout")
                {
                    // For FANOUT, each service needs its OWN queue to get a copy of all messages.
                    // We'll create a transient, exclusive, auto-deleting queue for this consumer.
                    queueNameToConsume = $"{exchangeName}_{_serviceInfo.Name}_{Guid.NewGuid()}"; // Unique queue name
                    _channel.QueueDeclare(queue: queueNameToConsume, durable: false, exclusive: true, autoDelete: true, arguments: null);
                }
                else
                {
                    // For DIRECT, services consume from a shared, well-known queue.
                    queueNameToConsume = exchangeName;
                    _channel.QueueDeclare(queue: queueNameToConsume, durable: true, exclusive: false, autoDelete: false, arguments: null);
                }

                // Bind the queue to the exchange
                _channel.QueueBind(queue: queueNameToConsume, exchange: exchangeName, routingKey: routingKey);
                Console.WriteLine($"[{_serviceInfo.Name}] Bound queue '{queueNameToConsume}' to exchange '{exchangeName}'.");

                // Start listening for messages on the declared queue
                var consumer = new EventingBasicConsumer(_channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    Console.ForegroundColor = ConsoleColor.Green;
                    // Use queueNameToConsume in the log message for clarity
                    Console.WriteLine($"[{_serviceInfo.Name}] Received on '{queueNameToConsume}' via '{ea.Exchange}': '{message}'");
                    Console.ResetColor();

                    HandleReceivedMessage(message);

                    _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };

                _channel.BasicConsume(queue: queueNameToConsume, autoAck: false, consumer: consumer);
                Console.WriteLine($"[{_serviceInfo.Name}] Listening for messages on '{queueNameToConsume}'.");
            }
        }

        private void HandleReceivedMessage(string message)
        {
            // AuthService logic: When a command is received, it publishes an event.
            if (_serviceInfo.Name == "AuthService" && message == "RegisterUser")
            {
                var responseEvent = "UserRegistered";
                PublishMessage(responseEvent, "system_events");
                return; // End processing for this service
            }

            // Logic for other services reacting to the "UserRegistered" event.
            if (message == "UserRegistered")
            {
                switch (_serviceInfo.Name)
                {
                    case "UserService":
                        var newUserEvent = "UserProfileCreated";
                        // UserService publishes its own event after processing.
                        PublishMessage(newUserEvent, "user_events");
                        break;

                    case "NotificationService":
                        // NotificationService just logs a more descriptive action.
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"[{_serviceInfo.Name}] Simulating: sending welcome email for new user.");
                        Console.ResetColor();
                        break;
                }
            }
        }

        public void PublishMessage(string message, string exchangeName)
        {
            if (_channel == null) return;

            var body = Encoding.UTF8.GetBytes(message);

            // Publish to the specified exchange. The routing key depends on the exchange type.
            // For fanout, it's ignored. For direct, it should be the queue name.
            var exchangeType = _rabbitMqConfig.GetValueOrDefault(exchangeName)?.Type ?? "direct";
            var routingKey = exchangeType == "fanout" ? "" : exchangeName;

            _channel.BasicPublish(exchange: exchangeName,
                                 routingKey: routingKey,
                                 basicProperties: null,
                                 body: body);

            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"[{_serviceInfo.Name}] Published to exchange '{exchangeName}': '{message}'");
            Console.ResetColor();
        }

        public void Dispose()
        {
            _channel?.Close();
            _connection?.Close();
            GC.SuppressFinalize(this);
        }
    }
}