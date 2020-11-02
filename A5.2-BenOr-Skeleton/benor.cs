#define PRIORITY_QUEUE

namespace benor {
using System;
using static System.Console;
using System.IO;
using System.Linq;
using System.Collections.Generic;

#if PRIORITY_QUEUE
using Priority_Queue;
#endif

public class Message {
    public Message (int u, int r, int f, int t, int v) {
        (Utime, Round, From, To, Val) = (u, r, f, t, v);
    }
    
    public void Deconstruct (out int u, out int r, out int f, out int t, out int v) {
        (u, r, f, t, v) = (Utime, Round, From, To, Val);
    }
    
    public int Utime; public int Round; public int From; public int To; public int Val;
    
    public override string ToString () {
        return $"{Utime} {Round} {From} {To} {Val}";
    }
}

public class Node {
    public static int NN;  // number of processes
    public static int N2 => (int) (NN/2.0);
    public static int FF;  // max faults
    public static int V0; //
    public static int Limit;  // loop limit
    public static bool VERBOSE = false;

    public Node (int p, int k) { 
        Proc = p;  
        Krash = k <0 ? 100: k;  
        Round = 0; 
        Utime = 0;  
        // X comes from first msg (init from Arcs)
        Z = 2;   
        stash = new List<Message> ();
        received = new int[NN];
        Array.Fill (received, -1);
        rnd = new Random (7 * Proc);
    }
    
    int Proc;  // process number
    int Krash;  // crash round
    int Round;  // round number
    int Utime;  // universal time
    int X;  // temp init / report / proposal
    int Z;  // decision
    
    List<Message> stash; // for the future
    int[] received;  // received values
    
    Random rnd;
    
    void Print () {
        var recs = string 
            .Join ("", 
                received .Select (v => v>=0? (char) ('0'+v): '_'));
        if (VERBOSE) Write ($"    {Proc} : ");
        WriteLine ($"{Utime} {Round} {Proc} {recs} {X} {Z} {(Z!=2? "!": "")}");
    }
    
    Message[] SendEmpty () {
        if (VERBOSE) WriteLine ($"    {Proc} >");
        return new Message [0];
    }
    
    Message[] SendAll (int v) {
        Round += 1;
        
        if (Round > Limit  || Round > Krash) {
            if (VERBOSE) WriteLine ($"    {Proc} -");
            return new Message [0];
        }
        
        var send = Enumerable.Range (1, NN) .Select (t => new Message (Utime, Round, Proc, t, v)) .ToArray ();
        if (VERBOSE) WriteLine ($"    {Proc} > {string.Join(", ", send.ToList())}");
        Array.Fill (received, -1);
        return send;
    }
    
    public Message[] Process (Message [] ms) {
        //receive inbound messages
        //process
        //returns outbound messages
        
        if (Round > Limit || Round > Krash) return new Message [0];

        // UDOO
        
        return new Message [0];
    }
}

public class Arcs {
    static IEnumerable<string> ReadAllLines (TextReader inp) {
        var line = "";
        while ((line = inp.ReadLine ()) != null) {
            yield return line;
        }
    }
    
    // read and store configuration
    // initialise nodes
    static void Config (TextReader inp) {
        var lines = ReadAllLines (inp)
            .Select (line => {
                var com = line.IndexOf ("//");
                return com < 0? line : line.Substring (0, com);
                })
            .Select (line => line.Trim ())
            .Where (line => line != "")
            .Select (line => line .Split ((char[])null, StringSplitOptions.RemoveEmptyEntries))
            .ToArray (); 
            
        var nfline = lines[0] .Select (item => int.Parse (item)) .ToArray ();
        
        Node.NN = nfline[0];
        Node.FF = nfline[1];
        Node.V0 = nfline[2];
        Node.Limit = nfline[3];
                
        var nlines = lines .Skip (1) .Take (Node.NN) 
            .Select (line => line .Select (item => int.Parse (item)) .ToArray ())
            .ToArray();
        
        var dlines = lines.Skip (nlines.Count () + 1)
            .Select (line => line .Select (item => int.Parse (item)) .ToArray ())
            .ToArray();
        
        init = new int[1+Node.NN];
        nodes = new Node[1+Node.NN];
        
        foreach (var line in nlines) {
            var Proc = line[0];  // process #
            var Val = line[1];  // init v
            var Krash = line[2];  // krash at

            init[Proc] = Val;
            nodes[Proc] = new Node (Proc, Krash);  // !!!
        }
        //nodes.Dump ();
        
        delays = new Dictionary <(int, int), int[]> ();

        foreach (var line in dlines) {
            delays[(line[0], line[1])] = line[2..];
            var max = line[2..] .Max ();
            if (MaxDelay < max) MaxDelay = max;
        }
        for (var n = 1; n <= Node.NN; n ++) {
            if (! delays.ContainsKey ((n, n))) delays[(n, n)] = new[] {1};
        }
        //delays.Dump ();
        
    }
        
    static int Uclock;
    static int MaxDelay;
    static int MessageCount;
    
    static int[] init;
    static Node[] nodes;
    static Dictionary <(int, int), int[]> delays;

    static int Delay (int n1, int n2, int r) {
        var dels = 
            delays.ContainsKey ((n1, n2))? delays[(n1, n2)]: 
            delays.ContainsKey ((n1, 0))? delays[(n1, 0)]: 
            delays.ContainsKey ((0, n2))? delays[(0, n2)]: 
            delays[(0, 0)];
        
        return r < dels.Length? dels[r-1]: dels[dels.Length-1];
    }

    // priority queue for organising messages
#if PRIORITY_QUEUE
    static SimplePriorityQueue<Message, int> Msgs;
#else
    static List<Message> MsgsList;
#endif

    public static void Main2 () { 
        // Linqpad stuff
        // Directory.SetCurrentDirectory (Path.GetDirectoryName (Util.CurrentQueryPath));
        // Config (new StreamReader ("abc.txt"));
        Config (Console.In);
        
#if PRIORITY_QUEUE
        Msgs = new SimplePriorityQueue<Message, int> ();
        for (var p = 1; p <= Node.NN; p ++) {
            var m0 = new Message (0, 0, 0, p, init[p]);
            Msgs.Enqueue (m0, Uclock);
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
        while (Msgs.Count > 0) {
            // automatically sorted
            Uclock = Msgs.First.Utime;
            var msgsout = new List<Message> ();
            while (Msgs.Count > 0 && Msgs.First.Utime == Uclock) {
                msgsout.Add (Msgs.Dequeue());
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
            var msgsout_grouped = msgsout.GroupBy (m => m.To).OrderBy (mg => mg.Key);
            
            var msgsin = msgsout_grouped 
                .SelectMany (mgroup => {
                    var mgroupout = nodes[mgroup.Key] 
                        .Process (mgroup.ToArray ());
                    return mgroupout;
                    })
                .ToArray ();
                                
            //WriteLine ($"=== arcs in: {string.Join(", ", msgsin.ToList ())}");
            if (msgsin.Count() > 0 && msgsin[0].To == 0) {
                WriteLine ();
                WriteLine ($" {Uclock} {Uclock/MaxDelay} {MessageCount}");
                break;
            }
            
            for (var i = 0; i < msgsin.Count(); i++) {
                var m = msgsin[i];
                m.Utime += Delay (m.From, m.To, m.Round);

#if PRIORITY_QUEUE
                Msgs .Enqueue (m, m.Utime);
#else
                MsgsList .Add (m);
#endif
            }
            
            MessageCount += msgsin.Count();                
        }
         
        // stop        
    }    
    
    public static void Main1 () {   
    }    
    
    public static void Main () { 
        try {
            Main2 ();
            //Main1 ();
        } catch (Exception ex) {
            WriteLine ($"*** {ex.Message}");
        }
    }
}
}
