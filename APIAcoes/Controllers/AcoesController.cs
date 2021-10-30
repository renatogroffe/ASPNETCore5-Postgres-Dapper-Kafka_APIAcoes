using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Confluent.Kafka;
using Dapper;
using Npgsql;
using APIAcoes.Data;
using APIAcoes.Models;
using APIAcoes.Extensions;

namespace APIAcoes.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class AcoesController : ControllerBase
    {
        private readonly ILogger<AcoesController> _logger;
        private readonly IConfiguration _configuration;

        public AcoesController(ILogger<AcoesController> logger,
            [FromServices]IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        [HttpPost]
        [ProducesResponseType(typeof(Resultado), (int)HttpStatusCode.OK)]
        [ProducesResponseType((int)HttpStatusCode.BadRequest)]
        public async Task<Resultado> Post(Acao acao)
        {
            CotacaoAcao cotacaoAcao = new ()
            {
                Codigo = acao.Codigo,
                Valor = acao.Valor,
                CodCorretora = _configuration["Corretora:Codigo"],
                NomeCorretora = _configuration["Corretora:Nome"]
            };
            var conteudoAcao = JsonSerializer.Serialize(cotacaoAcao);
            _logger.LogInformation($"Dados: {conteudoAcao}");

            string topic = _configuration["ApacheKafka:Topic"];

            using (var producer = KafkaExtensions.CreateProducer(_configuration))
            {
                var result = await producer.ProduceAsync(
                    topic,
                    new Message<Null, string>
                    { Value = conteudoAcao });

                _logger.LogInformation(
                    $"Apache Kafka - Envio para o tópico {topic} concluído | " +
                    $"{conteudoAcao} | Status: { result.Status.ToString()}");
            }

            return new ()
            {
                Mensagem = "Informações de ação enviadas com sucesso!"
            };
        }

        [HttpGet]
        [ProducesResponseType(typeof(IEnumerable<HistoricoAcao>), (int)HttpStatusCode.OK)]
        public IEnumerable<HistoricoAcao> ListAll()
        {
            using var connection = new NpgsqlConnection(
                _configuration.GetConnectionString("BaseAcoes"));
            connection.Open();
            var dados = connection.Query<HistoricoAcao>(
                "SELECT * FROM \"Acoes\" ORDER BY \"Id\" DESC");
            connection.Close();

            _logger.LogInformation($"No. registros encontrados = {dados.Count()}");
            
            return dados;
        }
    }
}