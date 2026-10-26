using System.Text.Json.Serialization;

namespace MicroFlow.Core
{
    public class RabbitMqQueueConfig
    {
        [JsonPropertyName("type")]
        public string Type { get; set; } = "direct";
    }
}