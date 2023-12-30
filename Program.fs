open System
open Akka.FSharp
open System.Threading
open System.Collections.Generic
open Chord

[<EntryPoint>]
let main argv =
    // Create an Akka.NET actor system for the Chord ring simulation
    let system = System.create "chord-ring" (Configuration.load())

    let numNodes, numRequests, m = int argv.[0], int argv.[1], 20

    // Calculate the total space of values based on m value
    // we have taken m value to be 20
    let totalSpace = int (Math.Pow(2.0, float m))

    // Create a hop counter actor to collect statistics
    let hopCounterActor = spawn system "hopCounter" (hopCounter numNodes)

    // Generate unique random node IDs for the Chord nodes
    // this maintains a hash set to check previous Node IDs.
    // if an ID is present in the HashSet, a new random
    // node ID is created until the ID is unique
    let hashSet = HashSet()
    let nodeIDs = Array.init numNodes (fun _ ->
        let rec gen() =
            let r = (Random()).Next(totalSpace)
            if hashSet.Add(r) then r
            else gen()
        gen()
    )

    // Create Chord node actors, initializing the Chord ring and sending queries
    let nodeActors = Array.init numNodes (fun i ->
        let actor = spawn system (string nodeIDs.[i]) (chordAlgorithmSimulator nodeIDs.[i] m numRequests hopCounterActor totalSpace)
        if i = 0 then actor <! CreateNode else actor <! JoinRing(nodeIDs.[0])
        actor
    )

    // Allow time for initialization and then start query operations
    Thread.Sleep(300)
    for nodeActor in nodeActors do
        nodeActor <! StartQuery
        Thread.Sleep(500)

    // Wait for the Akka.NET system to terminate
    system.WhenTerminated.Wait()

    0