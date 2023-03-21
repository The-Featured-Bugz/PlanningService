namespace PlanningService;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;

    private ConnectionFactory factory = new ConnectionFactory();
    private IConnection connection;
    private IModel channel;

    private string csvPath = string.Empty;
    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;


        string connectionString = configuration["RabbitMQConnectionString"] ?? string.Empty;

        csvPath = configuration["csvpath"] ?? string.Empty;

        factory = new ConnectionFactory() { HostName = connectionString };
        connection = factory.CreateConnection();
        channel = connection.CreateModel();
        logger.LogInformation("WW");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {


        channel.QueueDeclare(queue: "hello",
                 durable: false,
                 exclusive: false,
                 autoDelete: false,
                 arguments: null);


        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                _logger.LogInformation("Getting Json: " + message);

                string csvFormat = "";
                bool addCharFlag = false;
                for (int i = 0; i < message.Length; i++)
                {
                    if (message[i] == ':' && message[i - 1] == '"')
                    {
                        addCharFlag = true;
                        i++;
                    }
                    if (message[i] == ',')
                    {
                        addCharFlag = false;
                        csvFormat += ',';
                    }
                    if (addCharFlag && message[i] != '"' && message[i] != '}' && message[i] != '{')
                    {
                        csvFormat += message[i];
                    }
                }

                _logger.LogInformation("Sending: " + csvFormat);
                File.AppendAllText(csvPath, csvFormat + Environment.NewLine);

            }
            catch (System.Exception ex)
            {

                _logger.LogError(ex.Message);
            }
        };


        channel.BasicConsume(queue: "hello",
                 autoAck: true,
                 consumer: consumer);


        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Nyt data: " + stoppingToken.ToString());

            _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            await Task.Delay(10000, stoppingToken);
        }
    }
}
