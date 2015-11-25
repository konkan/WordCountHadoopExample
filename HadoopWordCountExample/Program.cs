using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using HadoopLab;
using Microsoft.Hadoop.MapReduce;
using Microsoft.WindowsAzure.Storage;

namespace HadoopWordCountExample
{
    /// <summary>
    /// Simple console application that uses HdInsight(Hadoop) to implement a word counter.
    /// NOTICE: Do not update following Nuget packages as it will not work with Microsoft.Hadoop.MapReduce.
    /// - Microsoft.WindowsAzure.ConfigurationManager
    /// - WindowsAzure.Storage
    /// </summary>
    class Program
    {
        private static readonly Stopwatch Stopwatch = new Stopwatch();

        static void Main()
        {
            //Cluster info
            var clusterUri = new Uri("{{YOUR CLUSTER URI}}");
            var clusterUserName = "{{YOUR CLUSTER USER NAME}}";
            var clusterPassword = "{{YOUR CLUSTER PASSWORD}}";
            var hadoopUserName = "test/counter";    //This also decides path to job folder(input/output)
            var container = "user";  //Base directory = container

            //Storage info
            var storageConnectionString = "{{YOUR STORAGE CONNECTION STRING}}";
            var cloudStorageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var storageKey = "{{YOUR STORAGE PRIMARY KEY}}";
            var storageAccount = cloudStorageAccount.BlobStorageUri.PrimaryUri.Host;

            //Init storage with test data
            var storageInit = new AzureStorageInitiator(storageConnectionString);
            storageInit.InitStorageAsync()
                .GetAwaiter()
                .GetResult();

            //Connect to cluster
            var hadoop = Hadoop.Connect(clusterUri, clusterUserName, hadoopUserName, clusterPassword, storageAccount,
                storageKey, container, true);

            //Execute job
            Stopwatch.Start();
            var result = hadoop.MapReduceJob.ExecuteJob<CounterJob>();
            Stopwatch.Stop();

            //Print job result to console.
            var jobFailed = result.Info.ExitCode != 0;
            Console.WriteLine(Environment.NewLine);
            Console.WriteLine(jobFailed
                ? $"Failed to run job: {result.Info.StandardError}."
                : $"Completed job in {Stopwatch.Elapsed.TotalSeconds} seconds.");

            if (jobFailed == false)
                PrintResultToConsole(hadoop);

            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        /// <summary>
        /// Will print the result to console.
        /// </summary>
        /// <param name="hadoop"></param>
        private static void PrintResultToConsole(IHadoop hadoop)
        {
            var resultRegex = new Regex("(?<word>[a-zA-Z]+)[^0-9]+(?<count>[0-9]+)", RegexOptions.Compiled);
            var outputLines = hadoop.StorageSystem.ReadAllLines("output/part-00000");

            var wordCounts = (from l in outputLines
                             let match = resultRegex.Match(l)
                             where match.Success
                             select new { Word = match.Groups["word"].Value, Count = int.Parse(match.Groups["count"].Value) })
                             .ToList();

            Console.WriteLine("Top 10 used words in \"I have a dream\"");
            foreach (var wordCount in wordCounts.OrderByDescending(o => o.Count).Take(10))
            {
                Console.WriteLine($"{wordCount.Word} - {wordCount.Count}");
            }
            Console.WriteLine(Environment.NewLine);
        }

        /// <summary>
        /// Will map all words per line, that has 4 character or more.
        /// </summary>
        public class CounterMap : MapperBase
        {
            private readonly Regex _wordRegex = new Regex("(?<word>[a-zA-Z]{4,})", RegexOptions.Compiled);

            public override void Map(string inputLine, MapperContext context)
            {
                //Split up line and validate words with regular expression
                var wordMatches = inputLine
                    .Split(' ')
                    .Select(o => _wordRegex.Match(o));
                
                foreach (var wordMatch in wordMatches)
                {
                    //Skip invalid words.
                    if(wordMatch.Success == false)
                        continue;

                    //Add entry with word as key and "1"(counter) as value
                    context.EmitKeyValue(wordMatch.Groups["word"].Value, "1");
                }
            }
        }

        /// <summary>
        /// Will calculate the total count of words, on all lines.
        /// </summary>
        public class CounterReduce : ReducerCombinerBase
        {
            public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
            {
                var totalCount = values.Sum(o => int.Parse(o));

                context.EmitKeyValue(key, totalCount.ToString());
            }
        }      

        /// <summary>
        /// Job that will count all words in all files given as input.
        /// </summary>
        public class CounterJob : HadoopJob<CounterMap, CounterReduce>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                var config = new HadoopJobConfiguration
                {
                    InputPath = "input",
                    OutputFolder = "output",
                    DeleteOutputFolder = true
                };

                return config;
            }
        }
    }

}
