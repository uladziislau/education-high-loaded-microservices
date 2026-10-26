using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Collections.Generic;
using MicroFlow.Core;

namespace MicroFlow.Generator;

public class ActiveReplica
{
    public string ServiceName { get; set; } = string.Empty;
    public string InstanceId { get; set; } = string.Empty;
    public int Port { get; set; }
    public Process Process { get; set; } = null!;
    public StreamWriter LogWriter { get; set; } = null!;
}

public class MicroservicesHoster
{
    private readonly MicroserviceConfig _config;
    private readonly List<ActiveReplica> _activeReplicas = new();
    private readonly object _scaleLock = new();

    public int DatabasePort { get; private set; }
    public int CachePort { get; private set; }
    private Process? _databaseProcess;
    private Process? _cacheProcess;

    public MicroservicesHoster(MicroserviceConfig config)
    {
        _config = config;
    }

    public List<ActiveReplica> GetActiveReplicas()
    {
        lock (_scaleLock)
        {
            return new List<ActiveReplica>(_activeReplicas);
        }
    }

    public Task StartServicesAsync()
    {
        StartInfrastructureNode("MicroFlow.Database", out int dbPort, out _databaseProcess);
        DatabasePort = dbPort;

        StartInfrastructureNode("MicroFlow.Cache", out int cachePort, out _cacheProcess);
        CachePort = cachePort;

        if (_config.Microservices == null) return Task.CompletedTask;

        foreach (var serviceInfo in _config.Microservices)
        {
            // Sync ports allocation
            serviceInfo.Ports.Clear();
            var replicasCount = serviceInfo.Replicas > 0 ? serviceInfo.Replicas : 1;
            
            ScaleService(serviceInfo.Name, replicasCount);
        }

        return Task.CompletedTask;
    }

    private void StartInfrastructureNode(string projectName, out int port, out Process process)
    {
        port = GetAvailablePort();
        var assemblyLocation = Assembly.GetExecutingAssembly().Location;
        var executionDirectory = Path.GetDirectoryName(assemblyLocation) ?? AppContext.BaseDirectory;
        var dllPath = Path.Combine(executionDirectory, $"{projectName}.dll");

        process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = $"\"{dllPath}\" --urls=http://localhost:{port}",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                WorkingDirectory = executionDirectory
            }
        };

        process.OutputDataReceived += (sender, args) =>
        {
            if (args.Data != null) Console.WriteLine($"[{projectName}]: {args.Data}");
        };
        process.ErrorDataReceived += (sender, args) =>
        {
            if (args.Data != null) Console.WriteLine($"[{projectName} ERROR]: {args.Data}");
        };

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        
        Console.WriteLine($"[Infrastructure] Started '{projectName}' on port {port} (PID: {process.Id})");
    }

    public void ScaleService(string serviceName, int targetCount)
    {
        lock (_scaleLock)
        {
            var serviceInfo = _config.Microservices.FirstOrDefault(s => s.Name.Equals(serviceName, StringComparison.OrdinalIgnoreCase));
            if (serviceInfo == null) return;

            var currentReplicas = _activeReplicas.Where(r => r.ServiceName.Equals(serviceName, StringComparison.OrdinalIgnoreCase)).ToList();
            var currentCount = currentReplicas.Count;

            if (currentCount == targetCount) return;

            var assemblyLocation = Assembly.GetExecutingAssembly().Location;
            var executionDirectory = Path.GetDirectoryName(assemblyLocation) ?? AppContext.BaseDirectory;
            var hostDllPath = Path.Combine(executionDirectory, "MicroFlow.MicroserviceHost.dll");
            var logsDirectory = Path.Combine(executionDirectory, "logs");

            if (!Directory.Exists(logsDirectory))
            {
                Directory.CreateDirectory(logsDirectory);
            }

            if (targetCount > currentCount)
            {
                // Scale Up
                int toAdd = targetCount - currentCount;
                for (int i = 0; i < toAdd; i++)
                {
                    // Find smallest index not currently active
                    int nextIndex = 1;
                    while (_activeReplicas.Any(r => r.ServiceName.Equals(serviceName, StringComparison.OrdinalIgnoreCase) && r.InstanceId.EndsWith($"-{nextIndex}")))
                    {
                        nextIndex++;
                    }

                    var port = GetAvailablePort();
                    var instanceId = $"{serviceName}-{nextIndex}";

                    var startupConfig = new StartupConfig
                    {
                        ServiceInfo = serviceInfo,
                        RabbitMQConfig = _config.RabbitMQQueues,
                        InstanceId = instanceId,
                        Port = port,
                        DatabasePort = DatabasePort,
                        CachePort = CachePort
                    };

                    var configJson = JsonSerializer.Serialize(startupConfig);
                    var base64Config = Convert.ToBase64String(Encoding.UTF8.GetBytes(configJson));

                    var logFilePath = Path.Combine(logsDirectory, $"{instanceId}.log");
                    var logWriter = new StreamWriter(logFilePath, append: false) { AutoFlush = true };
                    logWriter.WriteLine($"=== Process started at {DateTime.Now} ===");

                    var process = new Process
                    {
                        StartInfo = new ProcessStartInfo
                        {
                            FileName = "dotnet",
                            Arguments = $"\"{hostDllPath}\" {base64Config}",
                            UseShellExecute = false,
                            RedirectStandardOutput = true,
                            RedirectStandardError = true,
                            CreateNoWindow = true,
                            WorkingDirectory = executionDirectory
                        }
                    };

                    process.OutputDataReceived += (sender, args) =>
                    {
                        if (args.Data != null) logWriter.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {args.Data}");
                    };
                    process.ErrorDataReceived += (sender, args) =>
                    {
                        if (args.Data != null) logWriter.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] ERROR: {args.Data}");
                    };

                    process.Start();
                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();

                    _activeReplicas.Add(new ActiveReplica
                    {
                        ServiceName = serviceInfo.Name,
                        InstanceId = instanceId,
                        Port = port,
                        Process = process,
                        LogWriter = logWriter
                    });

                    Console.WriteLine($"[Scaler] Started '{instanceId}' on port {port} (PID: {process.Id}). Logs: logs/{instanceId}.log");
                }
            }
            else
            {
                // Scale Down
                int toRemove = currentCount - targetCount;
                var replicasToRemove = currentReplicas.OrderByDescending(r => r.InstanceId).Take(toRemove).ToList();

                foreach (var rep in replicasToRemove)
                {
                    try
                    {
                        if (!rep.Process.HasExited)
                        {
                            rep.Process.Kill(true);
                        }
                        rep.LogWriter.WriteLine($"=== Process stopped at {DateTime.Now} ===");
                        rep.LogWriter.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Scaler Error] Failed to stop {rep.InstanceId}: {ex.Message}");
                    }
                    _activeReplicas.Remove(rep);
                    Console.WriteLine($"[Scaler] Stopped '{rep.InstanceId}' and freed port {rep.Port}.");
                }
            }

            // Sync config state
            serviceInfo.Replicas = targetCount;
            serviceInfo.Ports = _activeReplicas.Where(r => r.ServiceName.Equals(serviceName, StringComparison.OrdinalIgnoreCase)).Select(r => r.Port).ToList();
            if (serviceInfo.Ports.Count > 0)
            {
                serviceInfo.Port = serviceInfo.Ports[0];
            }
        }
    }

    public async Task StartSimulatorAsync(string hostDllPath)
    {
        var assemblyLocation = Assembly.GetExecutingAssembly().Location;
        var executionDirectory = Path.GetDirectoryName(assemblyLocation);

        var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = $"\"{hostDllPath}\" --run-simulator",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                WorkingDirectory = executionDirectory
            }
        };

        process.OutputDataReceived += (sender, args) =>
        {
            if (args.Data != null)
            {
                Console.WriteLine($"[SIMULATOR]: {args.Data}");
            }
        };
        process.ErrorDataReceived += (sender, args) =>
        {
            if (args.Data != null)
            {
                Console.WriteLine($"[SIMULATOR ERROR]: {args.Data}");
            }
        };

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        await process.WaitForExitAsync();
    }

    public void StopServices()
    {
        lock (_scaleLock)
        {
            Console.WriteLine("\nShutting down all microservices...");
            foreach (var rep in _activeReplicas)
            {
                try
                {
                    if (!rep.Process.HasExited)
                    {
                        rep.Process.Kill(true);
                        Console.WriteLine($"Stopped process '{rep.InstanceId}' (PID {rep.Process.Id}).");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to stop process {rep.InstanceId}: {ex.Message}");
                }

                try
                {
                    rep.LogWriter.WriteLine($"=== Process stopped at {DateTime.Now} ===");
                    rep.LogWriter.Dispose();
                }
                catch { }
            }
            _activeReplicas.Clear();

            try
            {
                if (_databaseProcess != null && !_databaseProcess.HasExited)
                {
                    _databaseProcess.Kill(true);
                    Console.WriteLine("Stopped Database process.");
                }
                if (_cacheProcess != null && !_cacheProcess.HasExited)
                {
                    _cacheProcess.Kill(true);
                    Console.WriteLine("Stopped Cache process.");
                }
            }
            catch { }
        }
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