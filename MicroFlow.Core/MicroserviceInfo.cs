namespace MicroFlow.Core;

public class MicroserviceInfo
{
    public string Name { get; set; } = string.Empty;
    public int Port { get; set; } // Keeps backward compatibility, can be the first port or default
    public int Replicas { get; set; } = 1;
    public List<int> Ports { get; set; } = new();

    public List<string> Events { get; set; } = new();
    public List<string> Commands { get; set; } = new();
    public List<string> Queries { get; set; } = new();
    public List<string> Notifications { get; set; } = new();

    public List<string> OutgoingQueues { get; set; } = new();
    public List<string> IncomingQueues { get; set; } = new();
}