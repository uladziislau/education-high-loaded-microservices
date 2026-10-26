using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using MicroFlow.Core;

namespace MicroFlow.Generator;

public static class MicroservicesGenerator
{
    public static async Task<MicroserviceConfig?> GenerateMicroservicesAsync(string configFile)
    {
        var json = await File.ReadAllTextAsync(configFile);
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var microserviceConfig = JsonSerializer.Deserialize<MicroserviceConfig>(json, options);
        if (microserviceConfig?.Microservices == null)
        {
            return null;
        }

        foreach (var microserviceInfo in microserviceConfig.Microservices)
        {
            microserviceInfo.Ports.Clear();
            var replicasCount = microserviceInfo.Replicas > 0 ? microserviceInfo.Replicas : 1;
            for (int i = 0; i < replicasCount; i++)
            {
                var port = GetAvailablePort();
                microserviceInfo.Ports.Add(port);
            }
            if (microserviceInfo.Ports.Count > 0)
            {
                microserviceInfo.Port = microserviceInfo.Ports[0];
            }
        }

        return microserviceConfig;
    }

    private static int GetAvailablePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
