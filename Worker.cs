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


    //Selve lokations mappen, hvor csv filerne skal ligge
    private string _csvLocation = string.Empty;

    //Navne på kørere message kan bliver sendt til
    private string[] _routeWays = new string[0];



    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;

        //Hvilken køer der laves.
        string routeWaysArray = configuration["routeWaysArray"] ?? string.Empty;
        //Sætter kørerne i en array der kan køres af et for loop
        _routeWays = routeWaysArray.Length > 0 ? routeWaysArray.Split(",") : new string[0];
        string connectionString = configuration["RabbitMQConnectionString"] ?? string.Empty;

        _csvLocation = configuration["csvLocation"] ?? string.Empty;

        factory = new ConnectionFactory() { HostName = connectionString };
        connection = factory.CreateConnection();
        channel = connection.CreateModel();



        _logger.LogInformation($"csvLocation: {_csvLocation}");

        //Logger en besked for at fortælle, hvad kører der er lavet.
        string rWays = string.Empty;
        for (int i = 0; i < _routeWays.Length; i++)
        {
            rWays += _routeWays[i] + ", ";
        }
         _logger.LogInformation($"RouteWays: {rWays}");

    }


    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
{
    // Deklarer et kønavn og få navnet fra RabbitMQ-serveren
    var queueName = channel.QueueDeclare().QueueName;

    // Gennemløber listen af routing keys og binder køen til exchange
    foreach (var bindingKey in _routeWays)
    {
        channel.QueueBind(queue: queueName,
                          exchange: "topic_logs",
                          routingKey: bindingKey);
    }

    // Logger antallet af routing keys
    _logger.LogInformation(_routeWays.Length.ToString());

    // Opretter en forbruger, som lytter til beskeder på køen
    var consumer = new EventingBasicConsumer(channel);

    // Når forbrugeren modtager en besked, vil denne handling blive udført
    consumer.Received += (model, ea) =>
    {
        // Konverterer beskedens krop fra bytes til en UTF-8-streng
        var body = ea.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);

        // Gemmer routing key'en fra beskeden
        var routingKey = ea.RoutingKey;

        // Logger beskeden og dens routing key
        _logger.LogInformation($"[x] Modtaget '{routingKey}':'{message}' {Directory.GetCurrentDirectory() + _csvLocation +"\\"+ routingKey + ".csv"}");

        // Parser beskeden til CSV-format og gemmer den i en CSV-fil baseret på routing key'en
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

        // Logger den parsede besked
        _logger.LogInformation("Sender: " + csvFormat);

        // Gemmer den parsede besked i en CSV-fil baseret på routing key'en
        File.AppendAllText(Directory.GetCurrentDirectory() + _csvLocation + "\\"+ routingKey + ".csv", csvFormat + Environment.NewLine);
    };

    // Begynder at forbruge beskeder fra køen
    channel.BasicConsume(queue: queueName,
                         autoAck: true,
                         consumer: consumer);       

    // Kører en uendelig løkke, der logger en besked og venter 10 sekunder inden den kører igen
    while (!stoppingToken.IsCancellationRequested)
    {
        _logger.LogInformation("Nyt data: ");
        _logger.LogInformation("Arbejder ved: {time}", DateTimeOffset.Now);
        await Task.Delay(10000, stoppingToken);
    }
}


}
