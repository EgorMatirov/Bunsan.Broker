using System;
using System.IO;
using System.Text;
using System.Threading;
using Bunsan.Broker.Rabbit;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Bunsan.Broker
{
    public class ClientListener : ClientBase
    {
        private delegate void MessageCallback(BasicDeliverEventArgs message);
        private delegate void ReconnectCallback();

        private class Listener : DefaultBasicConsumer
        {
            private readonly MessageCallback _messageCallback;
            private readonly ErrorCallback _errorCallback;
            private readonly ReconnectCallback _reconnectCallback;

            public Listener(IModel model,
                MessageCallback messageCallback,
                ErrorCallback errorCallback,
                ReconnectCallback reconnectCallback)
                : base(model)
            {
                _messageCallback = messageCallback;
                _errorCallback = errorCallback;
                _reconnectCallback = reconnectCallback;
            }

            private void call_message(object messageObject)
            {
                var message = (BasicDeliverEventArgs)messageObject;
                try
                {
                    _messageCallback(message);
                }
                catch (Exception e)
                {
                    try
                    {
                        _errorCallback(message.BasicProperties.CorrelationId, e.ToString());
                    }
                    catch (Exception) { /* ignore */ }
                }
            }

            private void call_reconnect(object ignored)
            {
                _reconnectCallback();
            }

            public override void HandleBasicDeliver(string consumerTag,
                ulong deliveryTag,
                bool redelivered,
                string exchange,
                string routingKey,
                IBasicProperties properties,
                byte[] body)
            {
                var message = new BasicDeliverEventArgs
                {
                    ConsumerTag = consumerTag,
                    DeliveryTag = deliveryTag,
                    Redelivered = redelivered,
                    Exchange = exchange,
                    RoutingKey = routingKey,
                    BasicProperties = properties,
                    Body = body
                };
                ThreadPool.QueueUserWorkItem(call_message, message);
            }

            public override void HandleModelShutdown(object model, ShutdownEventArgs reason)
            {
                ThreadPool.QueueUserWorkItem(call_reconnect);
            }
        }

        private class ListenerFactory
        {
            private readonly StatusCallback _statusCallback;
            private readonly ResultCallback _resultCallback;
            private readonly ErrorCallback _errorCallback;
            private readonly ReconnectCallback _reconnectCallback;

            public ListenerFactory(StatusCallback statusCallback,
                ResultCallback resultCallback,
                ErrorCallback errorCallback,
                ReconnectCallback reconnectCallback)
            {
                _statusCallback = statusCallback;
                _resultCallback = resultCallback;
                _errorCallback = errorCallback;
                _reconnectCallback = reconnectCallback;
            }

            public Listener MakeStatusListener(IModel channel)
            {
                return new Listener(channel, message =>
                {
                    // we don't care about error handling of Status messages
                    using (var stream = new MemoryStream(message.Body))
                    {
                        var status = RabbitStatus.Parser.ParseFrom(stream);
                        // note: message was already acked, exceptions can be ignored
                        _statusCallback(status.Identifier, status.Status);
                    }
                }, _errorCallback, _reconnectCallback);
            }

            public Listener MakeResultListener(IModel channel)
            {
                return new Listener(channel, message =>
                {
                    try
                    {
                        using (var stream = new MemoryStream(message.Body))
                        {
                            var result = RabbitResult.Parser.ParseFrom(stream);
                            _resultCallback(result.Identifier, result.Result);
                        }
                    }
                    catch (Exception)
                    {
                        // message is either invalid or can't be processed by result_callback
                        // exception will cause error_callback to be called
                        // we do not want to requeue message since it may cause infinite try-exception loop
                        channel.BasicNack(message.DeliveryTag, false, false);
                        throw;
                    }
                    // commit phase
                    channel.BasicAck(message.DeliveryTag, false);
                }, _errorCallback, _reconnectCallback);
            }

            public Listener MakeErrorListener(IModel channel)
            {
                return new Listener(channel, message =>
                {
                    _errorCallback(message.BasicProperties.CorrelationId,
                        Encoding.UTF8.GetString(message.Body));
                    // commit phase
                    channel.BasicAck(message.DeliveryTag, false);
                }, _errorCallback, _reconnectCallback);
            }
        }

        private ListenerFactory _listenerFactory;

        public ClientListener(ConnectionParameters parameters) : base(parameters) 
        {
            Connection.OnConnect += Rebind;
        }

        ~ClientListener()
        {
            Connection.OnConnect -= Rebind;
        }

        private void Rebind(IModel channel)
        {
            if (_listenerFactory == null) return;
            var statusListener = _listenerFactory.MakeStatusListener(channel);
            var resultListener = _listenerFactory.MakeResultListener(channel);
            var errorListener = _listenerFactory.MakeErrorListener(channel);
            channel.QueueDeclare(StatusQueue, false, false, true, null);
            channel.QueueDeclare(ResultQueue, true, false, false, null);
            channel.QueueDeclare(ErrorQueue, true, false, false, null);
            channel.BasicConsume(StatusQueue, true, statusListener);
            channel.BasicConsume(ResultQueue, false, resultListener);
            channel.BasicConsume(ErrorQueue, false, errorListener);
        }

        public void Listen(StatusCallback statusCallback, ResultCallback resultCallback, ErrorCallback errorCallback)
        {
            _listenerFactory = new ListenerFactory(statusCallback, resultCallback, errorCallback, Connection.Reconnect);
            // force rebind for existing connection
            Rebind(Connection.Channel);
        }
    }
}