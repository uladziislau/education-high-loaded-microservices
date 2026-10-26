using System;

namespace MicroFlow.Core
{
    public class ReplicaMetrics
    {
        public string InstanceId { get; set; } = string.Empty;
        public string ServiceName { get; set; } = string.Empty;
        public int Port { get; set; }
        public long MessagesProcessed { get; set; }
        public long MessagesFailed { get; set; }
        public double AverageProcessingTimeMs { get; set; }
        public double MemoryUsageMb { get; set; }
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    }
}
