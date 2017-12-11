using System.Linq;
using Bacs.Problem;
using Bacs.Problem.Single;
using Bacs.Problem.Single.Process;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Bunsan.Broker
{
    public class SubmitClient
    {
        public enum ContinueConditionChange
        {
            Default,
            WhileOk,
            Always
        }
        
        private const string ExtensionSingleTypeUrl = "type.googleapis.com/bacs.problem.single.ProfileExtension";
        protected ClientSender clientSender;

        public SubmitClient(Configuration configuration)
        {
            Configuration = configuration;
        }

        protected Configuration Configuration { get; }
        protected ClientSender ClientSender => clientSender ?? (clientSender = new ClientSender(Configuration.ConnectionParameters));

        public void Submit(SubmitData submitData, bool shouldIncludeFiles = true, ContinueConditionChange continueConditionChange = ContinueConditionChange.Default)
        {
            var bunsanTask = CreateBunsanTask(submitData, shouldIncludeFiles, continueConditionChange).ToByteArray();
            ClientSender.Send(
                new Constraints {Resource = {Configuration.WorkerResourceName}},
                submitData.Identifier,
                new Task
                {
                    Data = ByteString.CopyFrom(bunsanTask),
                    Package = submitData.Problem.System.Package,
                    Worker = Configuration.WorkerName
                });
        }

        private const ulong OutputFilesSize = 10000;

        public static Bacs.Problem.Single.Task CreateBunsanTask(SubmitData submitData, bool shouldIncludeFiles, ContinueConditionChange continueConditionChange)
        {
            var problem = submitData.Problem;
            var solution = submitData.Solution;
            var system = new Bacs.Problem.System
            {
                Package = problem.System.Package,
                Revision = Revision.Parser.ParseFrom(problem.System.Revision.ToByteArray())
            };

            var testing = ProfileExtension.Parser.ParseFrom(problem.Profile.First().Extension.Value);

            if (continueConditionChange != ContinueConditionChange.Default)
            {
                var newContinueCondition = continueConditionChange == ContinueConditionChange.Always
                    ? TestSequence.Types.ContinueCondition.Always
                    : TestSequence.Types.ContinueCondition.WhileOk;
                foreach(var test in testing.TestGroup)
                {
                    test.Tests.ContinueCondition = newContinueCondition;
                }
            }

            if (submitData.PretestsOnly)
            {
                var notPretestGroups = testing.TestGroup.Where(x => x.Id != "pre");
                foreach (var notPretestGroup in notPretestGroups)
                {
                    testing.TestGroup.Remove(notPretestGroup);
                }
            }

            if (shouldIncludeFiles)
            {
                foreach (var testGroup in testing.TestGroup.Where(g => g.Id == "pre"))
                {
                    bool wasStdErr = false;
                    var process = testGroup.Process;

                    if (process.File != null)
                    {
                        var recieve = new Bacs.File.Range
                        {
                            Offset = 0,
                            Size = OutputFilesSize,
                            Whence = Bacs.File.Range.Types.Whence.Begin
                        };

                        foreach (var file in process.File)
                        {
                            if (file.Id != "stdin")
                            {
                                file.Init = "";
                            }

                            wasStdErr = (file.Id == "stderr");
                            file.Receive = recieve;
                        }

                        if (!wasStdErr)
                        {
                            var stdErrFile = new File
                            {
                                Id = "stderr",
                                Init = "",
                                //Path = null,
                                Receive = recieve,
                                Permission = { File.Types.Permissions.Read, File.Types.Permissions.Write }
                            };

                            process.File.Add(stdErrFile);
                        }

                        var stdHintFile = new File
                        {
                            Id = "hint",
                            Init = "out",
                            Receive = recieve,
                            //Permission = { File.Types.Permissions.READ, File.Types.Permissions.WRITE }
                        };

                        process.File.Add(stdHintFile);
                    }

                    CheckRedirectionForStderr(process);
                    testGroup.Process = process;
                }
            }

            CheckForInvalidRedirections(testing);

            var profile = new Profile
            {
                Extension =  new Any
                {
                    TypeUrl = ExtensionSingleTypeUrl,
                    Value = ByteString.CopyFrom(testing.ToByteArray())
                }
            };

            var task = new Bacs.Problem.Single.Task
            {
                System = system,
                Solution = solution,
                Profile = profile
            };
            return task;
        }

        private static void CheckRedirectionForStderr(Settings process)
        {
            var hasStdErr = process.Execution.Redirection.Any(x => x.FileId == "stderr");
            if (!hasStdErr)
            {
                var redirection = new Execution.Types.Redirection
                {
                    FileId = "stderr",
                    Stream = Execution.Types.Redirection.Types.Stream.Stderr
                };

                process.Execution.Redirection.Add(redirection);
            }
        }

        private static void CheckForInvalidRedirections(Bacs.Problem.Single.ProfileExtension testing)
        {
            foreach (var testGroup in testing.TestGroup)
            {
                var execution = testGroup.Process.Execution;

                var redirections = execution.Redirection
                    .Where(x => !string.IsNullOrEmpty(x.FileId))
                    .ToArray();

                execution.Redirection.Clear();
                execution.Redirection.Add(redirections);
            }
        }
    }
}