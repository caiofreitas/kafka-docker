using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace EnvioKafka
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console(theme: AnsiConsoleTheme.Literate)
                .CreateLogger();
            logger.Information("Testando o envio de mensagens com Kafka");



            string bootstrapServers = "localhost:9091"; 
            string nomeTopic = "topic-email";

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    for (int i = 0; i < 10; i++)
                    {

                        var result = await producer.ProduceAsync(
                            nomeTopic,
                            new () { Value = i.ToString() });

                        logger.Information(
                            $"Mensagem: {i} | " +
                            $"Status: { result.Status.ToString()}");
                    }
                }

                logger.Information("Concluído o envio de mensagens");
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
    }
}