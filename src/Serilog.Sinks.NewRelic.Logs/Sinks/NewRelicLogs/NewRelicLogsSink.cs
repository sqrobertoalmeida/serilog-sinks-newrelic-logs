using Newtonsoft.Json;
using Polly;
using Polly.Extensions.Http;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace Serilog.Sinks.NewRelic.Logs
{
    internal class NewRelicLogsSink : PeriodicBatchingSink
    {
        private static readonly HttpClient _client = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(5)
        };

        private readonly IAsyncPolicy<HttpResponseMessage> _retryPolicy;

        public const int DefaultBatchSizeLimit = 1000;

        public static readonly TimeSpan DefaultPeriod = TimeSpan.FromSeconds(2);

        public string EndpointUrl { get; }

        public string ApplicationName { get; }

        public string LicenseKey { get; }

        public string InsertKey { get; }

        private IFormatProvider FormatProvider { get; }

        public NewRelicLogsSink(
            string endpointUrl, 
            string applicationName, 
            string licenseKey, 
            string insertKey, 
            int batchSizeLimit, 
            TimeSpan period, 
            IFormatProvider formatProvider = null)
            : base(batchSizeLimit, period)
        {
            this.EndpointUrl = endpointUrl;
            this.ApplicationName = applicationName;
            this.LicenseKey = licenseKey;
            this.InsertKey = insertKey;
            this.FormatProvider = formatProvider;

            ConfigureHttpClient();

            _retryPolicy = HttpPolicyExtensions
                .HandleTransientHttpError()
                .OrResult(msg => ((int)msg.StatusCode) >= 300)
                .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));
        }

        private void ConfigureHttpClient()
        {
            if (!string.IsNullOrWhiteSpace(LicenseKey))
            {
                SelfLog.WriteLine("X-License-Key is configured by NEW_RELIC_LICENSE_KEY environment variable.");
                _client.DefaultRequestHeaders.TryAddWithoutValidation("X-License-Key", LicenseKey);
            }
            else if (!string.IsNullOrWhiteSpace(InsertKey))
            {
                SelfLog.WriteLine("X-Insert-Key is configured by NEW_RELIC_INSERT_KEY environment variable.");
                _client.DefaultRequestHeaders.TryAddWithoutValidation("X-Insert-Key", InsertKey);
            }
        }

        protected override async Task EmitBatchAsync(IEnumerable<LogEvent> eventsEnumerable)
        {
            var payload = new NewRelicLogPayload(this.ApplicationName);
            var events = eventsEnumerable.ToList();

            foreach (var _event in events)
            {
                try
                {
                    var item = new NewRelicLogItem(_event, this.FormatProvider);

                    payload.Logs.Add(item);
                }
                catch (Exception ex)
                {
                    SelfLog.WriteLine("Log event could not be formatted and was dropped: {0} {1}", ex.Message, ex.StackTrace);
                }
            }

            var body = Serialize(new List<object> { payload }, events.Count);

            await this.SendToNewRelicLogsAsync(body).ConfigureAwait(false);
        }

        private async Task SendToNewRelicLogsAsync(string body)
        {
            try
            {
                await this.SendToNewRelicLogs(body);
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("Event batch could not be sent to NewRelic Logs and was dropped: {0} {1}", ex.Message, ex.StackTrace);
            }
        }

        private async Task SendToNewRelicLogs(string body)
        {
            ServicePointManager.SecurityProtocol |= SecurityProtocolType.Tls12;

            if (string.IsNullOrWhiteSpace(LicenseKey) && string.IsNullOrWhiteSpace(InsertKey))
                return;

            var byteStream = Encoding.UTF8.GetBytes(body);

            try
            {
                using (var memoryStream = new MemoryStream())
                {
                    using (var gzip = new GZipStream(memoryStream, CompressionMode.Compress, true))
                        gzip.Write(byteStream, 0, byteStream.Length);

                    memoryStream.Position = 0;

                    using (StreamContent content = new StreamContent(memoryStream))
                    {
                        content.Headers.ContentType = new MediaTypeHeaderValue("application/gzip");
                        content.Headers.ContentEncoding.Add("gzip");

                        var response = await _retryPolicy.ExecuteAsync(async () => await _client.PostAsync(this.EndpointUrl, content));

                        if (response == null || response.StatusCode != HttpStatusCode.Accepted)
                        {
                            SelfLog.WriteLine("Self-log: Response from NewRelic Logs is missing or negative: {0}", response?.StatusCode);
                        }
                    }
                }
            }
            catch (WebException ex)
            {
                SelfLog.WriteLine("Failed to parse response from NewRelic Logs: {0} {1}", ex.Message, ex.StackTrace);
            }
        }

        private static string Serialize(List<object> items, int count)
        {
            var serializer = new JsonSerializer();

            //Stipulate 500 bytes per log entry on average
            var json = new StringBuilder(count * 500);

            using (var stringWriter = new StringWriter(json))
            {
                using (var jsonWriter = new JsonTextWriter(stringWriter))
                {
                    serializer.Serialize(jsonWriter, items);
                }
            }

            return json.ToString();
        }
    }
}
