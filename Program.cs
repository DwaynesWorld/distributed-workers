using dotnet_etcd;
using Etcdserverpb;
using Google.Protobuf;
using Microsoft.Extensions.Options;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;
using V3Lockpb;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((_, lc) => lc.WriteTo.Console(theme: AnsiConsoleTheme.Code));

builder.Services.AddLogging();
builder.Services.AddOptions();
builder.Services.Configure<ConnectionStrings>(builder.Configuration.GetSection("ConnectionStrings"));
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddTransient<Worker>();

for (int i = 0; i < 8; i++)
{
    builder.Services.AddSingleton<IHostedService>(p => p.GetRequiredService<Worker>());
}

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();
app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();
app.Run();

public class Worker : BackgroundService
{
    private readonly Guid _workerId = Guid.NewGuid();

    private readonly ILogger<Worker> _logger;
    private readonly EtcdClient _client;

    public Worker(ILogger<Worker> logger, IOptions<ConnectionStrings> connectionStrings)
    {
        _logger = logger;
        _client = new(connectionStrings.Value.Etcd);
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await Task.Yield();

        _logger.LogInformation("[{workerId}] Worker started...", _workerId);

        var (lease, heartbeat) = await RegisterWorker(cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await DoWork(lease, cancellationToken);

                // Delay to share work with other workers
                await Task.Delay(2000, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogCritical("[{workerId}] An unhandled exception occurred: {message}", _workerId, ex.Message);

                // Simulate restart, would not exist in real service
                heartbeat.Cancel();
                await Task.Delay(20000, cancellationToken);
                (lease, heartbeat) = await RegisterWorker(cancellationToken);
            }
        }
    }

    // Register workers w/TTL to protect against catastrophic failures where locks are not released
    private async Task<(long, CancellationTokenSource)> RegisterWorker(CancellationToken cancellationToken)
    {
        var request = new LeaseGrantRequest() { ID = _workerId.GetHashCode(), TTL = 15 };
        var lease = await _client.LeaseGrantAsync(request, cancellationToken: cancellationToken);

        _logger.LogInformation("[{workerId}] Worker registered with lease '{leaseId}'", _workerId, lease.ID);

        // Start keep alive, normally CancellationTokenSource would not be needed
        var heartbeat = new CancellationTokenSource();
        _ = Task.Run(async () => await _client.LeaseKeepAlive(lease.ID, heartbeat.Token), heartbeat.Token);

        return (lease.ID, heartbeat);
    }

    private async Task DoWork(long lease, CancellationToken cancellationToken)
    {
        LockResponse? @lock = null;

        try
        {
            var request = new LockRequest() { Name = ByteString.CopyFromUtf8("work-lock"), Lease = lease };
            @lock = await _client.LockAsync(request, cancellationToken: cancellationToken);

            _logger.LogInformation("[{workerId}] Worker acquired lock '{lock}'", _workerId, @lock.Key.ToStringUtf8());

            await DoMutuallyExclusiveWork(cancellationToken);
        }
        finally
        {
            if (@lock is not null)
            {
                // Do not forward token, this should execute even in the event of a token cancellation.
                _ = await _client.UnlockAsync(key: @lock.Key.ToStringUtf8(), cancellationToken: default);
                _logger.LogInformation("[{workerId}] Worker released lock '{lock}'", _workerId, @lock.Key.ToStringUtf8());
            }
        }
    }

    private async Task DoMutuallyExclusiveWork(CancellationToken cancellationToken)
    {
        _logger.LogInformation("[{workerId}] Worker started exclusive work", _workerId);
        await Task.Delay(2000, cancellationToken);

        if (Random.Shared.Next(4) == 0)
            throw new Exception("Simulated failure");

        _logger.LogInformation("[{workerId}] Worker completed exclusive work", _workerId);
    }
}


public class ConnectionStrings
{
    public string Etcd { get; set; } = default!;
}