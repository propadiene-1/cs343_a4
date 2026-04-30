package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type RaftNode int

type VoteArguments struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteReply struct {
	Term       int
	ResultVote bool
}

type AppendEntryArgument struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries []LogEntry //empty if heartbeat message
	LeaderCommit int //commit index
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

type LogEntry struct {
 Index int
 Term int
}

//Leader election variabled
var selfID int
var serverNodes []ServerConnection
var currentTerm int
var votedFor int
var lastLogTerm int
var raftLog []LogEntry 
var votes int

//Log replication state variables
var commitIndex int //index of last known log entry committed
var lastApplied int //index of highest log entry applied
var lastAppliedIndex int

//Leader only
var nextIndex []int
var matchIndex []int

// Node state
var role string
var mu sync.Mutex
var isLeader bool

//failure simulation
var isAlive bool

// resetElection is used to signal the election-timer goroutine that a valid
// heartbeat (or granted vote) was received, so the timer should restart.
var resetElection = make(chan struct{}, 1)

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Reply false if term < currentTerm
// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
// Hint 2: Only focus on the details related to leader election and majority votes
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	if isAlive == false{
		return nil
	}
	mu.Lock()
	defer mu.Unlock()

	if arguments.Term < currentTerm { //node is ahead of candidate
		reply.Term = currentTerm
		reply.ResultVote = false
		return nil
	}

	if currentTerm < arguments.Term { //node is behind
		currentTerm = arguments.Term //update term
		votedFor = -1
		role = "follower"
		isLeader = false
	}
	//candidate log is up to date if last term is higher, or same term as leader with log length >= raft log
	nodeLastIndex := len(raftLog) -1
	nodeLastTerm := 0
	if nodeLastIndex >= 0 {
		nodeLastTerm = raftLog[nodeLastIndex].Term
	}

	logOK := arguments.LastLogTerm > nodeLastTerm || (arguments.LastLogTerm == nodeLastTerm && arguments.LastLogIndex >= nodeLastIndex)

	if (votedFor == -1 || votedFor == arguments.CandidateID) && logOK { //second case of figure 2
		votedFor = arguments.CandidateID
		reply.ResultVote = true
		//reset election timer so we don't immediately start
		select {
		case resetElection <- struct{}{}:
		default:
		}
		// if (arguments.LastLogTerm > lastLogTerm) {	//check up-to-date
		// 	votedFor = arguments.CandidateID //cast vote
		// 	reply.ResultVote = true
		// } else if (arguments.LastLogTerm == lastLogTerm && arguments.LastLogIndex >= len(raftLog)-1){
		// 	votedFor = arguments.CandidateID //cast vote
		// 	reply.ResultVote = true
		// } else {
		// 	reply.ResultVote = false
		// }
	} else {
		reply.ResultVote = false
	}
	reply.Term = currentTerm
	return nil
}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	if isAlive == false{
		return nil
	}
	mu.Lock()
	defer mu.Unlock()

	if arguments.Term < currentTerm { //node is ahead of candidate
		reply.Term = currentTerm
		reply.Success = false
		return nil
	}

	if arguments.Term > currentTerm { //sender is valid leader for this term
		currentTerm = arguments.Term
		votedFor = -1
	}
	role = "follower" //current node is follower
	isLeader = false

	reply.Term = currentTerm

	//check if log contains an entry at PrevLogIndex
	if arguments.PrevLogIndex >=0 {
		if arguments.PrevLogIndex >= len(raftLog) {
			reply.Success = false //don't have index yet
			
			select{
			case resetElection <- struct {} {}:
			default:
			}
			return nil
		}
		if raftLog[arguments.PrevLogIndex].Term != arguments.PrevLogTerm {
			//conflicting entry (incorrect info - must delete everything after and get correct entries)
			raftLog = raftLog[:arguments.PrevLogIndex]
			reply.Success = false
			select{
			case resetElection <- struct {} {}:
			default:
			}
			return nil
		}
	}

	//append new entries not already in the log
	insertAt := arguments.PrevLogIndex + 1
	for i, entry := range arguments.Entries {
		logPos := insertAt + i
		if logPos < len(raftLog) {
			if raftLog[logPos].Term != entry.Term {
				//conflicting entry 
				raftLog = raftLog[:logPos]
				raftLog = append(raftLog, entry)
			}
		} else {
			raftLog = append(raftLog, entry)
		}
	}

	//update commitIndex if leader's is ahead
	if arguments.LeaderCommit > commitIndex {
		lastNewIndex := len(raftLog) - 1
		if arguments.LeaderCommit < lastNewIndex {
			commitIndex = arguments.LeaderCommit
		}else {
			commitIndex = lastNewIndex
		}
	}
	reply.Success = true

	//reset election timer
	select {
	case resetElection <- struct{}{}:
	default:
	}

	if len(arguments.Entries) > 0{
		fmt.Printf("\n[%s] Appended %d entries from leader %d (term %d), log now length %d\n",
			time.Now().Format("15:04:05.000000"), len(arguments.Entries),
			arguments.LeaderID, arguments.Term, len(raftLog))
	}else{ //log entry empty - send heartbeat msg
	//fmt.Printf("\n[%s] Heartbeat received from leader %d (term %d)\n", time.Now().Format("15:04:05.000000"), arguments.LeaderID, arguments.Term)
	}

	return nil
}

// 	if (currentTerm < arguments.Term){//node is behind
// 		currentTerm = arguments.Term //update term
// 		votedFor = -1
// 		isLeader = false
// 	}

// 	return nil
// }

//failure simulation
func failNode(t int) {
	isAlive = false
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func () {
		<- timer.C
		isAlive = true
	}()
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func LeaderElection() {
	if isAlive == false{
		return
	}
	mu.Lock()
	role = "candidate"
	currentTerm += 1 //update term
	term := currentTerm
	votedFor = selfID

	myLastIndex := len(raftLog) -1
	myLastTerm := 0
	if myLastIndex >= 0{
		myLastTerm = raftLog[myLastIndex].Term
	}
	mu.Unlock()

	var voteMu sync.Mutex
	localVotes := 1 //self-vote automatically

	voteArgs := VoteArguments{
		Term:         term,
		CandidateID:  selfID,
		LastLogIndex: myLastIndex,
		LastLogTerm:  myLastTerm,
	}

	//Start election msg w/ term
	fmt.Printf("\n[%s] Node %d starting election for term %d\n",
		time.Now().Format("15:04:05.000000"), selfID, term)
 

	//call RequestVote from all nodes
	var wg sync.WaitGroup //asynchronous messaging
	//fmt.Printf("\n[%s] Started new election\n", time.Now().Format("15:04:05.000000"))
	for _, conn := range serverNodes { //conn = ServerConnection for this ID (in serverConnections map)
		fmt.Printf("\n[%s] Requesting vote from node %d\n", time.Now().Format("15:04:05.000000"), conn.serverID)
		wg.Add(1)
		go func(c ServerConnection) { //async: notify each node in a new goroutine
			var reply VoteReply
			defer wg.Done()
			err := c.rpcConnection.Call("RaftNode.RequestVote", &voteArgs, &reply)
			if err != nil {
				log.Println("Error with RequestVote:", err)
				return
			}
			mu.Lock()
			if reply.Term > currentTerm { //if candidate is behind, abort
				currentTerm = reply.Term
				votedFor = -1
				role = "follower"
				isLeader = false
				mu.Unlock()
				return
			}
			mu.Unlock()

			if reply.ResultVote {
				voteMu.Lock() //protect vote count access
				localVotes += 1
				voteMu.Unlock()
			}
			fmt.Printf("\n[%s] Vote reply from node %d: %+v", time.Now().Format("15:04:05.000000"), c.serverID, reply)
		}(conn)
	}
	wg.Wait()

	mu.Lock()
	majority := (len(serverNodes)+1)/2 + 1
	wonElection := localVotes >= majority && currentTerm == term && role != "leader"
	if wonElection {
		role = "leader"
		isLeader=true

		nextIndex = make([]int,len(serverNodes))
		matchIndex = make([]int, len(serverNodes))
		for i:= range serverNodes{
			nextIndex[i] = len(raftLog)
			matchIndex[i] = -1
		}
	}
	mu.Unlock()

	if wonElection {
		fmt.Printf("\n[%s] Node %d elected leader for term %d (votes: %d/%d)\n",
			time.Now().Format("15:04:05.000000"), selfID, term, localVotes, len(serverNodes)+1)
		go Heartbeat()
	} else {
		fmt.Printf("\n[%s] Node %d lost election for term %d (votes: %d/%d)\n",
			time.Now().Format("15:04:05.000000"), selfID, term, localVotes, len(serverNodes)+1)
	}
}

// 	//TODO: somehow start heartbeats, define leader role
// }

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat() {
	for {
		if isAlive == false {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		mu.Lock()
		if role != "leader" {
			mu.Unlock()
			return
		}
		term := currentTerm //capture current term
		leaderCommit := commitIndex
		logSnapshot := make([]LogEntry, len(raftLog))
		copy(logSnapshot, raftLog)
		nexti := make([]int, len(nextIndex))
		copy(nexti, nextIndex)
		mu.Unlock()

		for i, conn:= range serverNodes{
			go func(c ServerConnection, i int){
				mu.Lock()
				if role != "leader" {
					mu.Unlock()
					return
				}
				prevIndex := nexti[i] - 1
				prevTerm := 0
				if prevIndex >= 0 && prevIndex < len(logSnapshot){
					prevTerm = logSnapshot[prevIndex].Term
			}
			var entries []LogEntry
			if nexti[i] < len(logSnapshot){
				entries = logSnapshot[nexti[i]:]
			}
			mu.Unlock()

			args := AppendEntryArgument{
				Term:         term,
				LeaderID:     selfID,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries: entries,
				LeaderCommit: leaderCommit,
		}
		
		//fmt.Printf("\n[Leader %d] Sending %d entries to node %d (prevIndex=%d prevTerm=%d)\n", selfID, len(entries), c.serverID, prevIndex, prevTerm)
		var reply AppendEntryReply
		err:= c.rpcConnection.Call("RaftNode.AppendEntry", &args, &reply)

		if err!=nil{
			fmt.Printf("\nAppendEntry to node %d failed: %v\n", c.serverID, err)
			return
		}

		mu.Lock()
		defer mu.Unlock()

		if reply.Term > currentTerm{
			currentTerm = reply.Term
			votedFor = -1
			role = "follower"
			fmt.Printf("\n[%s] Node %d stepping down (saw term %d)\n", time.Now().Format("15:04:05.000000"), selfID, reply.Term)
			return
				}
		if reply.Success {
			if len(args.Entries) > 0 {
				newMatch := args.PrevLogIndex + len(args.Entries)
				if newMatch > matchIndex[i]{
					matchIndex[i] = newMatch
					nextIndex[i] = newMatch+1
				}
				for n:= len(raftLog) - 1; n>commitIndex; n--{
					if raftLog[n].Term!= currentTerm{
						continue
					}
					count := 1
					for _,mi:= range matchIndex{
						if mi>=n{
							count++
						}
					}
					if count >= (len(serverNodes)+1)/2+1{
						commitIndex=n
						fmt.Printf("\n[%s] Leader committed log up to index %d\n",
									time.Now().Format("15:04:05.000000"), commitIndex)
						//fmt.Printf("\n[Leader %d] matchIndex=%v nextIndex=%v\n", selfID, matchIndex, nextIndex)
						break
					}
				}
			}
		}else{
			if nextIndex[i] >0{
				nextIndex[i]--
			}
		}
	}(conn,i)
		}
		//Heartbeat interval: 100 ms
		time.Sleep(100 * time.Millisecond)
	}
}

// This function is designed to emulate a client reaching out to the server. Note that many of the realistic details are removed, for simplicity
func ClientAddToLog() {
 // In a realistic scenario, the client will find the leader node and communicate with it
 // In this implementation, we are pretending that the client reached out to the server somehow
 // But any new log entries will not be created unless the server / node is a leader
 // isLeader here is a boolean to indicate whether the node is a leader or not
	for {
		if isAlive == false{
			continue
		}
		time.Sleep(1 * time.Second)
		//fmt.Printf("\n[Node %d] ClientAddToLog ticked. isLeader=%v\n", selfID, isLeader)
		if isLeader {
			// lastAppliedIndex here is an int variable that is needed by a node to store the value of the last index it used in the log
			mu.Lock()
			entry := LogEntry{lastAppliedIndex, currentTerm}
			log.Println("Client communication created the new log entry at index " + strconv.Itoa(entry.Index))
			raftLog = append(raftLog, entry)
			lastAppliedIndex++
			//fmt.Printf("\n[Node %d] Log is now length %d\n", selfID, len(raftLog))
			mu.Unlock()
		}
	}
 // HINT 1: using the AppendEntry RPC might happen here
 // HINT 2: force the thread to sleep for a good amount of time (less than that of the leader election timer) and then repeat the actions above.
 // You may use an endless loop here or recursively call the function
 // HINT 3: you don’t need to add to the logic of creating new log entries, just handle the replication
}

func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Read the values sent in the command line

	// Get this sever's ID (same as its index for simplicity)
	myID, err := strconv.Atoi(arguments[1])
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	myPort := "localhost"

	// Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			myPort = text
			index++
			continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with readin the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Following lines are to register the RPCs of this object of type RaftNode
	api := new(RaftNode)
	err = rpc.Register(api)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(myPort, nil)
	log.Printf("serving rpc on port" + myPort)

	// This is a workaround to slow things down until all servers are up and running
	// Idea: wait for user input to indicate that all servers are ready for connections
	// Pros: Guaranteed that all other servers are already alive
	// Cons: Non-realistic work around

	// reader := bufio.NewReader(os.Stdin)
	// fmt.Print("Type anything when ready to connect >> ")
	// text, _ := reader.ReadString('\n')
	// fmt.Println(text)

	// Idea 2: keep trying to connect to other servers even if failure is encountered
	// For fault tolerance, each node will continuously try to connect to other nodes
	// This loop will stop when all servers are connected
	// Pro: Realistic setup
	// Con: If one server is not set up correctly, the rest of the system will halt

	for index, element := range lines {
		// Attemp to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers list
		serverNodes = append(serverNodes, ServerConnection{index, element, client})
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft

	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done!
	// Hint 4: wg.Wait() might be helpful here

	selfID = myID
	votedFor = -1
	role = "follower"
	isAlive = true
	commitIndex = -1

	go ClientAddToLog()

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			if scanner.Text() == "fail"{
				fmt.Printf("\n[Node %d] Simulating failure for 5 seconds\n", selfID)
				failNode(5)
			}
		}
	}()

	// Election timeout loop.
	// Each iteration waits for a randomised timeout in [150, 300) ms.
	// If no heartbeat (or granted-vote signal) arrives within that window,
	// the node starts a new election.
	go func() {
		for {
			// Random election timeout between 150 ms and 300 ms.
			timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond

			select {
			case <-resetElection:
				// Received a valid heartbeat or granted a vote — restart timer.
				continue
			case <-time.After(timeout):
				// Timer expired — check whether we are already the leader before
				// starting an election
				mu.Lock()
				r := role
				mu.Unlock()
				if r == "follower" || r == "candidate" {
					go LeaderElection()
				}
			}
		}
	}()

	//go LeaderElection() //basic call for testing. will change.
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
