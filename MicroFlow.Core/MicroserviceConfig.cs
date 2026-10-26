namespace MicroFlow.Core;

using System.Text.Json.Serialization;

public class MicroserviceConfig
{
    public List<MicroserviceInfo> Microservices { get; set; } = new();

    [JsonPropertyName("RabbitMQQueues")]
    public Dictionary<string, RabbitMqQueueConfig> RabbitMQQueues { get; set; } = new();
}