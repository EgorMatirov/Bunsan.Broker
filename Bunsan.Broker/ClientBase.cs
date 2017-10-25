using System;

namespace Bunsan.Broker
{
    public abstract class ClientBase : IDisposable
    {
        public string StatusQueue { get; set; }
        public string ResultQueue { get; set; }
        public string ErrorQueue { get; set; }
        protected readonly Connection Connection;

        public ClientBase(ConnectionParameters parameters)
        {
            Connection = new Connection(parameters);
            if (string.IsNullOrEmpty(parameters.Identifier)) throw new ArgumentException("Expected non-empty Identifier");
            StatusQueue = "client." + parameters.Identifier + ".status";
            ResultQueue = "client." + parameters.Identifier + ".result";
            ErrorQueue = "client." + parameters.Identifier + ".error";
        }

        public delegate void StatusCallback(string id, Status status);
        public delegate void ResultCallback(string id, Result result);
        public delegate void ErrorCallback(string id, string error);

        public void Close()
        {
            Connection.Close();
        }

        public void Dispose()
        {
            Close();
        }
    }
}