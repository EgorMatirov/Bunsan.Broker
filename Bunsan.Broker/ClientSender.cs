using System;
using System.IO;
using Bunsan.Broker.Rabbit;
using RabbitMQ.Client.Exceptions;
using Google.Protobuf;

namespace Bunsan.Broker
{
    public class ClientSender : ClientBase
    {
        public ClientSender(ConnectionParameters parameters) : base(parameters) { }

        public void Send(Constraints constraints, string id, Task task)
        {
            var rabbitTask = new RabbitTask
            {
                Identifier = id,
                Task = task,
                Constraints = constraints,
                ResultQueue = ResultQueue,
                StatusQueue = StatusQueue,
            };
            byte[] data;
            using (var stream = new MemoryStream())
            {
                rabbitTask.WriteTo(stream);
                data = new byte[stream.Length];
                stream.Seek(0, SeekOrigin.Begin);
                stream.Read(data, 0, data.Length);
            }
            if (constraints.Resource.Count != 1)
                throw new NotImplementedException("Supports only single resource constraint");
            for (; ; )
            {
                try
                {
                    var properties = Connection.Channel.CreateBasicProperties();
                    properties.ReplyTo = ErrorQueue;
                    properties.CorrelationId = id;
                    properties.DeliveryMode = 2;  // persistent
                    if (Connection.IsClosed) throw new InvalidOperationException("Already closed");
                    Connection.Channel.BasicPublish("",
                        constraints.Resource[0],
                        false,
                        properties,
                        data);
                    break;
                }
                catch (AlreadyClosedException)
                {
                    // retry
                }
            }
        }
    }
}