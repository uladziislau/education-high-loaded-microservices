using Microsoft.Extensions.Caching.Memory;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddMemoryCache();
var app = builder.Build();

app.MapGet("/cache/{key}", (string key, IMemoryCache cache) =>
{
    if (cache.TryGetValue(key, out string? value))
    {
        return Results.Ok(new { key, value });
    }
    return Results.NotFound();
});

app.MapPost("/cache/{key}", (string key, JsonElement body, IMemoryCache cache) =>
{
    var value = body.GetProperty("value").GetString() ?? "";
    
    // Set cache options: 10 seconds Absolute Expiration (TTL)
    var cacheEntryOptions = new MemoryCacheEntryOptions()
        .SetAbsoluteExpiration(TimeSpan.FromSeconds(10));
        
    cache.Set(key, value, cacheEntryOptions);
    
    return Results.Ok(new { key, value });
});

Console.WriteLine("Starting MicroFlow.Cache...");
app.Run();
