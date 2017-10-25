namespace Bunsan.Broker
{
    public class Configuration
    {
        public string WorkerName { get; set; }
        public string WorkerResourceName { get; set; }
        public ConnectionParameters ConnectionParameters { get; set; }
    }
}