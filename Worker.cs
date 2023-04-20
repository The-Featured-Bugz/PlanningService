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
    private string csvPathService = string.Empty;
    private string csvPathRepair = string.Empty;
    private string[] routeWays = new string[0];
    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;

        string gg = configuration["routeWays"] ?? string.Empty;
        routeWays = gg.Length > 0 ? gg.Split(",") : new string[0];
        string connectionString = configuration["RabbitMQConnectionString"] ?? string.Empty;

        csvPath = configuration["csvpath"] ?? string.Empty;

        csvPathService = configuration["csvPathService"] ?? string.Empty;
        csvPathRepair = configuration["csvPathRepair"] ?? string.Empty;

        factory = new ConnectionFactory() { HostName = connectionString };
        connection = factory.CreateConnection();
        channel = connection.CreateModel();
        logger.LogInformation("WW " + routeWays[0]);













    }


    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {

        
           var queueName = channel.QueueDeclare().QueueName;
            foreach (var bindingKey in routeWays)
            {
                channel.QueueBind(queue: queueName,
                                exchange: "topic_logs",
                                routingKey: bindingKey);
            }

             _logger.LogInformation(routeWays.Length.ToString());

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var routingKey = ea.RoutingKey;
                _logger.LogInformation($" [x] Received '{routingKey}':'{message}'");
            };
            channel.BasicConsume(queue: queueName,
                                autoAck: true,
                                consumer: consumer);       
        




        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Nyt data: ");

            _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            await Task.Delay(10000, stoppingToken);
        }
    }


































    /*
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
                            //Hejsa
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
        } */
}
