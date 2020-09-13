using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using FluentUri;

namespace MusicMapRecommender {

    internal static class Program {

        private static readonly HttpClient CLIENT = new HttpClient(new HttpClientHandler { MaxConnectionsPerServer = MAX_PARALLEL_DOWNLOADS }) { Timeout = TimeSpan.FromSeconds(15) };

        // private static readonly Uri        BASE_URI = new Uri("http://localhost/");
        private static readonly Uri BASE_URI = new Uri("https://www.music-map.com/");

        private const int MAX_PARALLEL_DOWNLOADS = 24;

        private static async Task<int> Main() {
            string? knownArtistListFilename = Environment.GetCommandLineArgs().Skip(1).FirstOrDefault();
            if (knownArtistListFilename == null) {
                Console.WriteLine("Please pass the filename of a newline-delimited text file containing a list of known artists to look up as the first argument.");
                return 1;
            }

            ISet<string> knownArtistNames = new HashSet<string>((await File.ReadAllLinesAsync(knownArtistListFilename, Encoding.UTF8)).Select(s => s.ToLowerInvariant()));

            var blockOptions = new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = MAX_PARALLEL_DOWNLOADS };

            TransformBlock<string, ArtistPage> fetchSource = new TransformBlock<string, ArtistPage>(async artistName => {
                Uri artistUri = FluentUriBuilder.From(BASE_URI.AbsoluteUri).Path(artistName.ToLowerInvariant()).ToUri();
                using HttpResponseMessage response = await CLIENT.GetAsync(artistUri);

                var artistPage = new ArtistPage { artistName = artistName };
                if (response.IsSuccessStatusCode) {
                    artistPage.sourceCode = await response.Content.ReadAsStringAsync();
                } else {
                    Console.WriteLine($"Error: Failed to download artist page for {artistName}: {response.StatusCode}. Please check the artist name on {BASE_URI}");
                }

                return artistPage;
            }, blockOptions);

            TransformManyBlock<ArtistPage, Relationship> extractRelationships = new TransformManyBlock<ArtistPage, Relationship>(page => {
                if (page.sourceCode == null) return null;

                MatchCollection artistNameMatches = Regex.Matches(page.sourceCode, @"<a href=""(?<artistSlug>.+?)"" class=S id=s\d+>(?<artistName>.+?)</a>");
                IEnumerable<string> relatedArtistNames = artistNameMatches.Skip(1).Select(match => match.Groups["artistName"].Value);

                Match relationshipStrengthMatches = Regex.Match(page.sourceCode, @"Aid\[0\]=new Array\(-1(?:,(?<strength>-?\d+(?:\.\d+)?))*\);");
                IEnumerable<double> relatedArtistStrengths = relationshipStrengthMatches.Groups["strength"].Captures.Select(capture => double.Parse(capture.Value));

                return relatedArtistNames.Zip(relatedArtistStrengths)
                    .Select(tuple => new Relationship { knownArtist = page.artistName, unknownArtist = tuple.First, strength = tuple.Second });
            }, blockOptions);

            IDictionary<string, double> strengthsByArtistName = new Dictionary<string, double>();
            ActionBlock<Relationship> incoming = new ActionBlock<Relationship>(relationship => {
                if (knownArtistNames.Contains(relationship.unknownArtist.ToLowerInvariant())) return;
                strengthsByArtistName.TryGetValue(relationship.unknownArtist, out double existingStrength);
                strengthsByArtistName[relationship.unknownArtist] = existingStrength + relationship.strength;
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
            var outgoing = new BufferBlock<IDictionary<string, double>>();

#pragma warning disable 4014
            incoming.Completion.ContinueWith(task => { outgoing.Post(strengthsByArtistName); });
#pragma warning restore 4014

            IPropagatorBlock<Relationship, IDictionary<string, double>> reduce = DataflowBlock.Encapsulate(incoming, outgoing);

            ActionBlock<IDictionary<string, double>> printReport = new ActionBlock<IDictionary<string, double>>(strengths => {
                IOrderedEnumerable<KeyValuePair<string, double>> sorted = strengths.OrderByDescending(pair => pair.Value);
                foreach ((string unknownArtistName, double strength) in sorted) {
                    Console.WriteLine($"{strength,5:N1}\t{unknownArtistName}");
                }
            });

            DataflowLinkOptions linkOptions = new DataflowLinkOptions { PropagateCompletion = true };
            fetchSource.LinkTo(extractRelationships, linkOptions);
            extractRelationships.LinkTo(reduce, linkOptions);
            reduce.LinkTo(printReport, linkOptions);

            foreach (string knownArtistName in knownArtistNames) {
                fetchSource.Post(knownArtistName);
            }

            fetchSource.Complete();
            await printReport.Completion;
            return 0;
        }

        private struct ArtistPage {

            public string  artistName;
            public string? sourceCode;

        }

        private struct Relationship {

            public string knownArtist;
            public string unknownArtist;
            public double strength;

        }

    }

}