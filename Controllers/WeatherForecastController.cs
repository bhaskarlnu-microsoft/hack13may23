using Microsoft.AspNetCore.Mvc;
using testAPI.Models;
using testAPI.Data;

namespace testAPI.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private readonly ILogger<WeatherForecastController> _logger;
        private KustosClient kustoClient;

        public WeatherForecastController(ILogger<WeatherForecastController> logger, KustosClient kustoClient)
        {
            _logger = logger;
            this.kustoClient = kustoClient;
        }

        [HttpGet(Name = "GetWeatherForecast")]
        public List<WeatherForecast> Get()
        {
            List<WeatherForecast> list= new List<WeatherForecast> ();
            var reader = kustoClient.getWeatherForecasts();
            _logger.LogInformation("Get Request");
            while (reader.Read())
            {
                list.Add(new WeatherForecast
                {
                    Date = reader.GetDateTime(0),
                    TemperatureC = reader.GetInt32(1),
                    Summary = reader.GetString(3)
                });
            }
            return list;
        }
        [HttpPost]
        public string Post(WeatherForecastRequest request)
        {
            WeatherForecast weather = new WeatherForecast
            {
                Date = request.Date ?? DateTime.Now,
                TemperatureC = request.TemperatureC ?? 0,
                Summary = request.Summary ?? "No Summary"
            };
            _logger.LogInformation("Post Request");
            string response = kustoClient.createNewEntry(weather);
            return response; 
        }

    }
}