using System;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Bunsan.Broker
{
    public class Connection : IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        protected bool Running = true;
        private IConnection _connection;
        protected IModel channel;

        protected object Lock => _connectionFactory;

        public IModel Channel
        {
            get
            {
                lock (Lock)
                {
                    reconnect();
                    return channel;
                }
            }
        }

        public bool IsRunning
        {
            get
            {
                lock (Lock)
                {
                    return Running;
                }
            }
        }

        public bool IsClosed => !IsRunning;

        public event Action<IModel> OnConnect = delegate { };
        
        public Connection(ConnectionParameters parameters)
        {
            _connectionFactory = new ConnectionFactory();
            if (parameters.Host != null) _connectionFactory.HostName = parameters.Host;
            if (parameters.Port != 0) _connectionFactory.Port = parameters.Port;
            if (parameters.VirtualHost != null) _connectionFactory.VirtualHost = parameters.VirtualHost;
            if (parameters.Credentials != null)
            {
                var credentials = parameters.Credentials;
                if (credentials.Username != null) _connectionFactory.UserName = credentials.Username;
                if (credentials.Password != null) _connectionFactory.Password = credentials.Password;
            }
        }

        public void Close()
        {
            lock (Lock)
            {
                Running = false;
                if (_connection != null && _connection.IsOpen)
                    _connection.Close();
            }
        }

        private void Connect()
        {
            _connection = _connectionFactory.CreateConnection();
            channel = _connection.CreateModel();
            OnConnect(channel);
        }

        private void reconnect()
        {
            if (!Running) return;
            while (_connection == null || !_connection.IsOpen)
            {
                try
                {
                    Connect();
                }
                catch (BrokerUnreachableException)
                {
                    Thread.Sleep(5000);
                }
            }
        }

        // Connect if necessary, i.e. not closed and not connected
        public void Reconnect()
        {
            lock (Lock)
            {
                reconnect();
            }
        }

        public void Dispose()
        {
            Close();
        }
    }
}