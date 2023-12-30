module Chord
open System
#r "nuget: Akka.FSharp, 1.4.27"
open Akka.FSharp

// Messages for Chord algorithm simulation
type NodeMessage =
    | GetPreviousNodeResponse of int
    | StartQuery
    | SearchFingetNext of int * int * int 
    | CreateNode
    | SearchPreviousNode
    | JoinRing of int
    | SearchNextNode of int
    | FoundKeyResponse of int
    | QueryRequest
    | StabilizeNetwork
    | NotifyNextNode of int
    | GetNextNodeResponse of int
    | FindNextKey of int * int * int
    | FixFingerTable
    | UpdateFingerTable of int * int   

// Create an actor path for a Chord node
let getActorPath s =
    let actorPath = @"akka://chord-ring/user/" + string s
    actorPath

let inBetween totalSpace left value right includeRight =
    // Check if value is in between left and right within a Chord ring, considering the totalSpace.
    // If includeRight is true, it includes the right boundary.
    
    // Correct the right and value positions if they wrap around the Chord ring.
    let correctedRight = if right < left then right + totalSpace else right
    let correctedValue = if (value < left && left > right) then value + totalSpace else value

    (left = right) || (correctedValue > left && (correctedValue < correctedRight || (includeRight && correctedValue = correctedRight)))

type HopCounterMessage =
    | NodeConvergedMessage of nodeID: int * hopCount: int * numRequests: int // Message to indicate that a Chord node has completed a query.

let processMessage (numNodes: int) (hopsTotal: int ref) (requestsTotal: int ref) (convergedNodesTotal: int ref) (messageBox: Actor<_>) (message) =
    match message with
    | NodeConvergedMessage (nodeID, hopCount, numRequest) ->
        // Update the total hop count, total requests, and count of converged nodes.
        hopsTotal := !hopsTotal + hopCount
        requestsTotal := !requestsTotal + numRequest
        convergedNodesTotal := !convergedNodesTotal + 1

        // If all nodes have converged, calculate and print the average number of hops,
        // then terminate the system.
        if (!convergedNodesTotal = numNodes) then
            printfn "Average number of hops: %f" (float !hopsTotal / float !requestsTotal)
            messageBox.Context.System.Terminate() |> ignore

// Continuously receives and processes messages which calculates total hop count and avg number of hops
let rec loop numNodes (totalHops: int ref) (totalRequests: int ref) (totalConvergedNodes: int ref) (messageBox: Actor<_>) = actor {
    let! message = messageBox.Receive()
    processMessage numNodes totalHops totalRequests totalConvergedNodes messageBox message
    return! loop numNodes totalHops totalRequests totalConvergedNodes messageBox
}

let hopCounter numNodes (messageBox: Actor<_>) =
    // Create a hop counter actor to collect and process convergence messages.
    let totalHops = ref 0
    let totalRequests = ref 0
    let totalConvergedNodes = ref 0
    actor {
        return! loop numNodes totalHops totalRequests totalConvergedNodes messageBox
    }

// Create a Chord finger table for a node with a given nodeID.
let createFingerTable m nodeID =
    let totalSpace = int (Math.Pow(2.0, float m))
    Array.init m (fun i -> (nodeID + int (Math.Pow(2.0, float i))) % totalSpace)

// Simulate Chord algorithm on a Chord node with given nodeID.
let chordAlgorithmSimulator (nodeID: int) m maxNumRequests hopCounterRef totalSpace (messageBox: Actor<_>) =

    let mutable predecessorID = -1
    let mutable next = 0
    let mutable totalHops = 0
    let mutable numRequests = 0

    // Create and initialize the Chord finger table for the node.
    let mutable nodeFingerTable = createFingerTable m nodeID

    let rec loop () = actor {
        let! message = messageBox.Receive ()
        let sender = messageBox.Sender ()
        match message with
        | StartQuery ->
            // Initiates a query operation if the maximum number of requests has not been reached.
            if(numRequests < maxNumRequests) then
                messageBox.Self <! QueryRequest
                // Schedule the next StartQuery message after a delay.
                messageBox.Context.System.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.), messageBox.Self, StartQuery)
            else
                // If the maximum number of requests is reached, send the nodes status to the hop counter.
                hopCounterRef <! NodeConvergedMessage (nodeID, totalHops, numRequests)

        | SearchNextNode (id) ->
            // Handle a message to find the next node in the Chord ring with the given id.
            if(inBetween totalSpace nodeID id nodeFingerTable.[0] true) then
                let newNodePath = getActorPath id
                let newNodeRef = messageBox.Context.ActorSelection newNodePath
                newNodeRef <! GetNextNodeResponse (nodeFingerTable.[0])
            else
                // If id is not within the range of the first entry, search the finger table.
                let mutable i = m - 1
                while(i >= 0) do
                    if(inBetween totalSpace nodeID nodeFingerTable.[i] id false) then
                        // Iterate through the finger table entries to find the closest preceding node.
                        let nearestPrecedingNodeID = nodeFingerTable.[i]
                        let nearestPrecedingNodePath = getActorPath nearestPrecedingNodeID
                        let nearestPrecedingNodeRef = messageBox.Context.ActorSelection nearestPrecedingNodePath
                        nearestPrecedingNodeRef <! SearchNextNode (id)
                        i <- -1
                    i <- i - 1

        | QueryRequest ->
            // Generate a random key within the Chord network space.
            let key = (System.Random()).Next(totalSpace)
            // Initiate the key lookup process for the generated key.
            messageBox.Self <! FindNextKey (nodeID, key, 0)

        | JoinRing (nDash) ->
            predecessorID <- -1
            // Construct the actor path for nDash and send a message to search for the next node.
            let nDashPath = getActorPath nDash
            let nDashRef = messageBox.Context.ActorSelection nDashPath
            nDashRef <! SearchNextNode (nodeID)

        | GetNextNodeResponse (succesorID) ->
            // Update the entries in the nodes finger table with the successor ID.
            for i = 0 to m - 1 do
                nodeFingerTable.[i] <- succesorID
            
            // Schedule periodic stabilization and finger table maintenance tasks.
            messageBox.Context.System.Scheduler.ScheduleTellRepeatedly (
                TimeSpan.FromMilliseconds(0.0),
                TimeSpan.FromMilliseconds(500.0),
                messageBox.Self,
                StabilizeNetwork
            )
            messageBox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    messageBox.Self,
                    FixFingerTable
                )

        // Handle the message to create a new Chord node.
        | CreateNode ->
            predecessorID <- -1

            // Initialize the entries in the nodes finger table with its own node ID.
            for i = 0 to m - 1 do
                nodeFingerTable.[i] <- nodeID
            
            // Schedule periodic tasks for network stabilization and finger table maintenance.
            messageBox.Context.System.Scheduler.ScheduleTellRepeatedly (
                TimeSpan.FromMilliseconds(0.0),
                TimeSpan.FromMilliseconds(500.0),
                messageBox.Self,
                StabilizeNetwork
            )
            messageBox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    messageBox.Self,
                    FixFingerTable
                )

        | FindNextKey (firstNodeId, id, numHops) ->
            if(id = nodeID) then
                // If id matches the current nodes ID, send a response to the first node with the hop count.
                let firstNodePath = getActorPath firstNodeId
                let firstNodeRef = messageBox.Context.ActorSelection firstNodePath
                firstNodeRef <! FoundKeyResponse (numHops)
            elif(inBetween totalSpace nodeID id nodeFingerTable.[0] true) then
                // If id falls within the range of the first entry in the finger table, send a response to the origin node.
                let firstNodePath = getActorPath firstNodeId
                let firstNodeRef = messageBox.Context.ActorSelection firstNodePath
                firstNodeRef <! FoundKeyResponse (numHops)
            else
                // If id is not yet found, search through the finger table.
                let mutable i = m - 1
                while(i >= 0) do
                    // Iterate through the finger table entries to find the closest preceding node for id.
                    if(inBetween totalSpace nodeID nodeFingerTable.[i] id false) then
                        let nearestPrecedingNodeID = nodeFingerTable.[i]
                        let nearestPrecedingNodePath = getActorPath nearestPrecedingNodeID
                        let nearestPrecedingNodeRef = messageBox.Context.ActorSelection nearestPrecedingNodePath
                        nearestPrecedingNodeRef <! FindNextKey (firstNodeId, id, numHops + 1)
                        i <- -1
                    i <- i - 1

        | StabilizeNetwork ->
            // Retrieve the ID of the successor node from the first entry in the finger table.
            let successorID = nodeFingerTable.[0]
            let succesorPath = getActorPath successorID
            let succesorRef = messageBox.Context.ActorSelection succesorPath
            succesorRef <! SearchPreviousNode

        | GetPreviousNodeResponse (x) ->
            if((x <> -1) && (inBetween totalSpace nodeID x nodeFingerTable.[0] false)) then
                // If the received predecessor ID is valid, update the first entry in the finger table.
                nodeFingerTable.[0] <- x
            
            // Retrieve the ID of the successor node from the first entry in the finger table.
            let successorID = nodeFingerTable.[0]
            let succesorPath = getActorPath successorID
            let successorRef = messageBox.Context.ActorSelection succesorPath
            successorRef <! NotifyNextNode (nodeID)

        | SearchPreviousNode ->
            // Send a response message containing the current predecessor ID to the sender.
            sender <! GetPreviousNodeResponse (predecessorID)

        | SearchFingetNext (firstNodeId, next, id) ->
            if(inBetween totalSpace nodeID id nodeFingerTable.[0] true) then
                // If id falls within the range of the first entry in the finger table, send an update to the origin node.
                let firstNodePath = getActorPath firstNodeId
                let firstNodeRef = messageBox.Context.ActorSelection firstNodePath
                firstNodeRef <! UpdateFingerTable (next, nodeFingerTable.[0])
            else
                // If not, search through the finger table to find the closest preceding node.
                let mutable i = m - 1
                while(i >= 0) do
                    if(inBetween totalSpace nodeID nodeFingerTable.[i] id false) then
                        let nearestPrecedingNodeID = nodeFingerTable.[i]
                        let nearestPrecedingNodePath = getActorPath nearestPrecedingNodeID
                        let nearestPrecedingNodeRef = messageBox.Context.ActorSelection nearestPrecedingNodePath
                        nearestPrecedingNodeRef <! SearchFingetNext (firstNodeId, next, id)
                        i <- -1
                    i <- i - 1

        | NotifyNextNode (nDash) ->
            if((predecessorID = -1) || (inBetween totalSpace predecessorID nDash nodeID false)) then
                // If the predecessor is not set or is in the correct range, update the predecessor ID.
                predecessorID <- nDash

        | FoundKeyResponse (hopCount) ->
            if(numRequests < maxNumRequests) then
                // If the maximum number of requests is not reached, update the total hops and request count.
                totalHops <- totalHops + hopCount
                numRequests <- numRequests + 1

        | FixFingerTable ->
            // Handle the task to periodically fix entries in the finger table.
            next <- next + 1
            if(next >= m) then
                next <- 0
            
            // Calculate the value for the new finger entry.
            let fingerValue = nodeID + int (Math.Pow(2.0, float (next)))

            // Initiate a search for the next node for the new finger entry.
            messageBox.Self <! SearchFingetNext (nodeID, next, fingerValue)

        | UpdateFingerTable (next, fingerSuccessor) ->
            // Update the specified entry in the finger table with the provided successor ID.
            nodeFingerTable.[next] <- fingerSuccessor

        return! loop ()
    }
    loop ()