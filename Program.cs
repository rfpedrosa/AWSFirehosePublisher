using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Amazon.KinesisFirehose;
using Amazon.KinesisFirehose.Model;
using Amazon.Runtime;
using Microsoft.Extensions.Configuration;

namespace AWSFirehosePublisher
{
  class Program
  {
    private static IAmazonKinesisFirehose _firehoseClient;

    static async Task Main(string[] args)
    {
      Console.WriteLine("Loading AWS credentials from environment variables");

      var config = new ConfigurationBuilder()
        .Build();

      // to easily support docker, i'm reading aws credentials from environment variables
      // https://stackoverflow.com/questions/43053495/how-to-set-credentials-on-aws-sdk-on-net-core
      var options = config.GetAWSOptions();
      options.Credentials = new EnvironmentVariablesAWSCredentials();
      _firehoseClient = options.CreateServiceClient<IAmazonKinesisFirehose>();

      Console.WriteLine("AWS credentials successfully loaded");

      await PublishGenericEventsAsync(100);
    }

    private static async Task PublishGenericEventsAsync(int nrOfEvents)
    {
        for(int i = 0; i < nrOfEvents; i++) {
            await PutRecordAsync(Guid.NewGuid());
        }        
    }
    

    private static Task<PutRecordResponse> PutRecordAsync(Guid id)
    {
      var data = "{\"id\": \"" + id + "\"}";

      // convert string to stream
      var byteArray = Encoding.UTF8.GetBytes(data);

      var putRecordRequest = new PutRecordRequest
      {
        DeliveryStreamName = "generic_event_stream", // AWS console -> Data FIrehose -> "Firehose delivery streams" 
        Record = new Record
        {
          Data = new MemoryStream(byteArray)
        }
      };

      // Put record into the DeliveryStream
      Console.WriteLine($"PutRecordAsync: {data}");
      return _firehoseClient.PutRecordAsync(putRecordRequest);
    }
  }
}
