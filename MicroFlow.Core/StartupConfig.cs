namespace MicroFlow.Core
{
    public class StartupConfig
    {
        public MicroserviceInfo ServiceInfo { get; set; } = new();
        public Dictionary<string, RabbitMqQueueConfig> RabbitMQConfig { get; set; } = new();
        public string InstanceId { get; set; } = string.Empty;
        public int Port { get; set; }
        public int DatabasePort { get; set; }
        public int CachePort { get; set; }
    }
}