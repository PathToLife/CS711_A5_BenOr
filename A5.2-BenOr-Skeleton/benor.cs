#define PRIORITY_QUEUE

using System;
using static System.Console;
using System.IO;
using System.Linq;
using System.Collections.Generic;
#if PRIORITY_QUEUE
using Priority_Queue;

#endif


namespace benor
{
    public class Message
    {
        public Message(int u, int r, int f, int t, int v)
        {
            (Utime, Round, From, To, Val) = (u, r, f, t, v);
        }

        public void Deconstruct(out int u, out int r, out int f, out int t, out int v)
        {
            (u, r, f, t, v) = (Utime, Round, From, To, Val);
        }

        public int Utime;
        public int Round;
        public int From;
        public int To;
        public int Val;

        public override string ToString()
        {
            return $"{Utime} {Round} {From} {To} {Val}";
        }
    }

    public class Node
    {
        public static int NumProcesses; // number of processes
        public static int NumProcessesHalf => (int) (NumProcesses / 2.0);
        public static int MaxFaults; // max faults
        public static int V0; //
        public static int LoopLimit; // loop limit
        public static bool VERBOSE = false;

        private int Proc; // process number
        private int Krash; // crash round
        private int _currRound; // round number
        private int _universalTime; // universal time
        private int _xTempDecision; // temp init / report / proposal
        private int _zDecision; // decision

        private List<Message> msgProcessLater; // for the future
        private int[] receivedValues; // received values

        Random rnd;

        public Node(int p, int k)
        {
            Proc = p;
            Krash = k < 0 ? 100 : k;
            _currRound = 0;
            _universalTime = 0;

            // X comes from first msg (init from Arcs)
            _zDecision = 2;
            msgProcessLater = new List<Message>();

            // init receivedValues
            receivedValues = new int[NumProcesses];
            Array.Fill(receivedValues, -1);

            rnd = new Random(7 * Proc);
        }

        private void Print()
        {
            var recs = string.Join("",
                receivedValues.Select(v => v >= 0 ? (char) ('0' + v) : '_'));
            if (VERBOSE) Write($"    {Proc} : ");
            WriteLine(
                $"{_universalTime} {_currRound} {Proc} {recs} {_xTempDecision} {_zDecision} {(_zDecision != 2 ? "!" : "")}");
        }

        private Message[] SendEmpty()
        {
            if (VERBOSE) WriteLine($"    {Proc} >");
            return new Message [0];
        }

        private Message[] GenerateToSendAndIncrRound(int v)
        {
            _currRound += 1;

            if (_currRound > LoopLimit || _currRound > Krash)
            {
                if (VERBOSE) WriteLine($"    {Proc} -");
                return new Message [0];
            }

            var send = Enumerable.Range(1, NumProcesses)
                .Select(t => new Message(_universalTime, _currRound, Proc, t, v)).ToArray();
            if (VERBOSE) WriteLine($"    {Proc} > {string.Join(", ", send.ToList())}");
            Array.Fill(receivedValues, -1);
            return send;
        }

        public Message[] Process(Message[] newMsgs)
        {
            //receive inbound messages
            //process
            //returns outbound messages

            _xTempDecision = 2; // temp init / report / proposal

            // Finish, no more messages to send;
            if (_currRound > LoopLimit || _currRound > Krash) return SendEmpty();

            // First round, store
            if (_currRound == 0)
            {
                _xTempDecision = newMsgs[0].Val;
                Print();
                return GenerateToSendAndIncrRound(_xTempDecision);
            }
            
            var processNow = new List<Message>();
            var newProcessLater = new List<Message>(); // for msgs that are not due to be processed
            
            // separate messages from newMsgs
            foreach (var msg in newMsgs)
            {
                if (msg.Utime > _universalTime) _universalTime = msg.Utime;
                if (msg.Round < _currRound) continue; // ignore late messages

                if (msg.Round == _currRound)
                {
                    processNow.Add(msg); // build msgs that are due to be processed
                }
                else
                {
                    newProcessLater.Add(msg); // build msgs that are not-due to be processed
                }
            }

            // separate messages from msgProcessLater
            foreach (var msg in msgProcessLater)
            {
                if (msg.Round < _currRound) continue; // ignore late messages
                
                if (msg.Round == _currRound)
                {
                    processNow.Add(msg); // build msgs that are due to be processed
                }
                else
                {
                    newProcessLater.Add(msg); // build msgs that are not-due to be processed
                }
            }
            
            // store messages that are not-due  to be processed
            msgProcessLater = newProcessLater;

            // process due messages
            foreach (var msg in processNow)
            {
                receivedValues[msg.From - 1] = msg.Val;
            }
            
            // check enough messages
            var neededMessagesCount = NumProcesses - MaxFaults;
            var shouldWaitForMoreMessages = receivedValues.Count(v => v != -1) < neededMessagesCount;
            if (shouldWaitForMoreMessages)
            {
                Print();
                return SendEmpty();
            }

            var count0 = receivedValues.Count(v => v == 0);
            var count1 = receivedValues.Count(v => v == 1);

            if (_currRound % 2 == 1) // odd round
            {
                if (count0 > NumProcessesHalf)
                {
                    _xTempDecision = 0;
                }
                else if (count1 > NumProcessesHalf)
                {
                    _xTempDecision = 1;
                }

                Print();
                return GenerateToSendAndIncrRound(_xTempDecision);
            }
            else // even round
            {
                if (count0 >= MaxFaults + 1)
                {
                    _zDecision = 0;
                }

                if (count1 >= MaxFaults + 1)
                {
                    _zDecision = 1;
                }

                if (count0 >= 1)
                {
                    _xTempDecision = 0;
                }
                else if (count1 >= 1) _xTempDecision = 1;
                else _xTempDecision = rnd.Next(0, 2);

                Print();
                return GenerateToSendAndIncrRound(_xTempDecision);
            }
        }
    }

    public class Arcs
    {
        private static IEnumerable<string> ReadAllLines(TextReader inp)
        {
            string line;
            do
            {
                line = inp.ReadLine();
                if (string.IsNullOrEmpty(line)) break;
                yield return line;
            } while (!string.IsNullOrEmpty(line));
        }

        // read and store configuration
        // initialise nodes
        private static void Config(TextReader inp)
        {
            var lines = ReadAllLines(inp)
                .Select(line =>
                {
                    var com = line.IndexOf("//", StringComparison.Ordinal);
                    return com < 0 ? line : line.Substring(0, com);
                })
                .Select(line => line.Trim())
                .Where(line => line != "")
                .Select(line => line.Split((char[]) null, StringSplitOptions.RemoveEmptyEntries))
                .ToArray();

            var nfline = lines[0].Select(item => int.Parse(item)).ToArray();

            Node.NumProcesses = nfline[0];
            Node.MaxFaults = nfline[1];
            Node.V0 = nfline[2];
            Node.LoopLimit = nfline[3];

            var nlines = lines.Skip(1).Take(Node.NumProcesses)
                .Select(line => line.Select(item => int.Parse(item)).ToArray())
                .ToArray();

            var dlines = lines.Skip(nlines.Count() + 1)
                .Select(line => line.Select(item => int.Parse(item)).ToArray())
                .ToArray();

            init = new int[1 + Node.NumProcesses];
            _nodesList = new Node[1 + Node.NumProcesses];

            foreach (var line in nlines)
            {
                var Proc = line[0]; // process #
                var Val = line[1]; // init v
                var Krash = line[2]; // krash at

                init[Proc] = Val;
                _nodesList[Proc] = new Node(Proc, Krash); // !!!
            }
            //nodes.Dump ();

            _delaysDictionary = new Dictionary<(int, int), int[]>();

            foreach (var line in dlines)
            {
                _delaysDictionary[(line[0], line[1])] = line[2..];
                var max = line[2..].Max();
                if (MaxDelay < max) MaxDelay = max;
            }

            for (var n = 1; n <= Node.NumProcesses; n++)
            {
                if (!_delaysDictionary.ContainsKey((n, n))) _delaysDictionary[(n, n)] = new[] {1};
            }

            //delays.Dump ();
        }

        private static int Uclock;
        private static int MaxDelay;
        private static int MessageCount;
        private static int[] init;
        private static Node[] _nodesList;
        private static Dictionary<(int, int), int[]> _delaysDictionary;

        static int Delay(int fromNode, int toNode, int roundNumber)
        {
            var dels =
                _delaysDictionary.ContainsKey((fromNode, toNode)) ? _delaysDictionary[(fromNode, toNode)] :
                _delaysDictionary.ContainsKey((fromNode, 0)) ? _delaysDictionary[(fromNode, 0)] :
                _delaysDictionary.ContainsKey((0, toNode)) ? _delaysDictionary[(0, toNode)] :
                _delaysDictionary[(0, 0)];

            return roundNumber < dels.Length ? dels[roundNumber - 1] : dels[dels.Length - 1];
        }

        // priority queue for organising messages
#if PRIORITY_QUEUE
        static SimplePriorityQueue<Message, int> Msgs;
#else
    static List<Message> MsgsList;
#endif

        public static void RunSimulation()
        {
            // Linqpad stuff
            // Directory.SetCurrentDirectory (Path.GetDirectoryName (Util.CurrentQueryPath));
            // Config (new StreamReader ("abc.txt"));
            Config(Console.In);

#if PRIORITY_QUEUE
            Msgs = new SimplePriorityQueue<Message, int>();
            for (var p = 1; p <= Node.NumProcesses; p++)
            {
                var m0 = new Message(0, 0, 0, p, init[p]);
                Msgs.Enqueue(m0, Uclock);
            }
#else
        MsgsList = new List<Message> ();
        for (var p = 1; p <= Node.NN; p ++) {
            var m0 = new Message (0, 0, 0, p, init[p]);
            MsgsList.Add (m0);
        }
#endif
            //WriteLine ($"=== init: {string.Join(", ", Msgs)}");

            Uclock = 0;
            MaxDelay = 1;
            MessageCount = 0;

            // sends and receives messages on behalf of nodes,
            // adding required delays, and tracing

#if PRIORITY_QUEUE
            while (Msgs.Count > 0)
            {
                // automatically sorted
                Uclock = Msgs.First.Utime;
                var msgsout = new List<Message>();
                while (Msgs.Count > 0 && Msgs.First.Utime == Uclock)
                {
                    msgsout.Add(Msgs.Dequeue());
                }

#else
        while (MsgsList.Count > 0) {
            MsgsList.Sort ((x, y) => x.Utime - y.Utime);
            Uclock = MsgsList[0].Utime;
            var msgsout = new List<Message> ();
            while (MsgsList.Count > 0 && MsgsList[0].Utime == Uclock) {
                msgsout.Add (MsgsList[0]);
                MsgsList.RemoveAt (0);
            }
#endif

                //WriteLine ($"=== arcs out: {string.Join(", ", msgsout)}");
                var msgsout_grouped = msgsout.GroupBy(m => m.To).OrderBy(mg => mg.Key);

                var msgsin = msgsout_grouped
                    .SelectMany(mgroup =>
                    {
                        var mgroupout = _nodesList[mgroup.Key]
                            .Process(mgroup.ToArray());
                        return mgroupout;
                    })
                    .ToArray();

                //WriteLine ($"=== arcs in: {string.Join(", ", msgsin.ToList ())}");
                if (msgsin.Count() > 0 && msgsin[0].To == 0)
                {
                    WriteLine();
                    WriteLine($" {Uclock} {Uclock / MaxDelay} {MessageCount}");
                    break;
                }

                for (var i = 0; i < msgsin.Count(); i++)
                {
                    var m = msgsin[i];
                    m.Utime += Delay(m.From, m.To, m.Round);

#if PRIORITY_QUEUE
                    Msgs.Enqueue(m, m.Utime);
#else
                MsgsList .Add (m);
#endif
                }

                MessageCount += msgsin.Count();
            }
        }

        public static void Main()
        {
            try
            {
                RunSimulation();
            }
            catch (Exception ex)
            {
                WriteLine($"*** {ex.Message}");
            }
        }
    }
}