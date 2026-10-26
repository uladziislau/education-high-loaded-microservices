using System;

namespace MicroFlow.Core
{
    public class MessageEnvelope
    {
        public string MessageId { get; set; } = Guid.NewGuid().ToString();
        public string CorrelationId { get; set; } = string.Empty;
        public string MessageType { get; set; } = string.Empty;
        public DateTime TimestampUtc { get; set; } = DateTime.UtcNow;
        public string Payload { get; set; } = string.Empty;

        public static MessageEnvelope Create(string payload, string correlationId, string messageType)
        {
            return new MessageEnvelope
            {
                CorrelationId = correlationId,
                MessageType = messageType,
                Payload = payload
            };
        }
    }

    public class ServiceControlMessage
    {
        public string ServiceName { get; set; } = string.Empty;
        public int LatencyMs { get; set; } = -1;
        public bool IsFailed { get; set; }
        public string CacheMode { get; set; } = "None";
    }
}
