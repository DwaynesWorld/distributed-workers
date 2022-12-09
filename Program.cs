using dotnet_etcd;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;
using V3Lockpb;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((_, lc) => lc.WriteTo.Console(theme: AnsiConsoleTheme.Code));

builder.Services.AddLogging();
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddTransient<Worker>();

for (int i = 0; i < 3; i++)
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
    private readonly ILogger<Worker> _logger;
    private readonly Guid _workerId = Guid.NewGuid();
    private readonly EtcdClient _client = new("http://localhost:23791,http://localhost:23792,http://localhost:23793");

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await Task.Yield();

        _logger.LogInformation("[{workerId}] Worker started...", _workerId);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await DoWork(cancellationToken);

                // Delay to share work with other workers
                await Task.Delay(2000, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogCritical("[{workerId}] An unhandled exception occurred: {message}", _workerId, ex.Message);

                // Simulate restart
                await Task.Delay(30000, cancellationToken);
            }
        }
    }

    private async Task DoWork(CancellationToken cancellationToken)
    {
        LockResponse? @lock = null;

        try
        {
            @lock = await _client.LockAsync(
                name: "work-lock",
                headers: null,
                deadline: null,
                cancellationToken: cancellationToken);
            _logger.LogInformation("[{workerId}] Worker acquired lock '{lock}'", _workerId, @lock.Key.ToStringUtf8());

            await DoMutuallyExclusiveWork(cancellationToken);
        }
        finally
        {
            if (@lock is not null)
            {
                _ = await _client.UnlockAsync(
                    key: @lock.Key.ToStringUtf8(),
                    headers: null,
                    deadline: null,
                    // Do not forward token, this should execute even in the event
                    // of a token cancellation.
                    cancellationToken: default);
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