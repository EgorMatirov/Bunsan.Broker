using Bacs.Problem;
using Bacs.Process;

namespace Bunsan.Broker
{
    public class SubmitData
    {
        public string Identifier { get; set; }
        public Buildable Solution { get; set; }
        public Problem Problem { get; set; }
        public bool PretestsOnly { get; set; }
    }
}