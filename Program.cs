using PlanningService;
using NLog;
using NLog.Web;

var logger = NLog.LogManager.Setup().LoadConfigurationFromAppSettings().GetCurrentClassLogger();
logger.Debug("init main");
try
{
    IHost host = Host.CreateDefaultBuilder(args)

        .ConfigureServices(services =>
        {
        services.AddHostedService<Worker>();
        })
        .ConfigureLogging(logging =>{
            logging.ClearProviders();
        }).UseNLog()
    .Build();

    logger.Info("HejHej");
    host.Run();   
}
catch (System.Exception ex)
{
    
    logger.Error(ex,"Stopped program because of exception");
    throw;
}finally{
    NLog.LogManager.Shutdown();
}
