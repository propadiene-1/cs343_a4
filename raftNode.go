package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
	"math/rand"
)

type RaftNode int

type VoteArguments struct {
	Term        int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

type VoteReply struct {
	Term       int
	ResultVote bool
}

type AppendEntryArgument struct {
	Term     int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
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

var selfID int
var serverNodes []ServerConnection
var currentTerm int
var votedFor int
var lastLogTerm int
var raftLog []int
var votes int 
var role string
var electionTimer *time.Timer

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	if (arguments.Term < currentTerm){ //case 1: candidate term < currentTerm
		reply.Term = currentTerm 
		reply.ResultVote = false //reply false
		return nil
	}
	if (currentTerm < arguments.Term){//node is behind the candidate
		currentTerm = arguments.Term //update term
		votedFor = -1
		role = "follower"
	}
	if (votedFor == -1 || votedFor == arguments.CandidateID){ //case 2: votedFor = null or votedFor = candidateID
		if (arguments.LastLogTerm > lastLogTerm) {	//check up-to-date
			votedFor = arguments.CandidateID //cast vote
			reply.ResultVote = true
		} else if (arguments.LastLogTerm == lastLogTerm && arguments.LastLogIndex >= len(raftLog)-1){
			votedFor = arguments.CandidateID //cast vote
			reply.ResultVote = true
		} else {
			reply.ResultVote = false
		}
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
	//add to raftLog
	//update lastIndex and prevIndex
	if (arguments.Term < currentTerm) { //this node is ahead
		reply.Term = currentTerm
		reply.Success = false //heartbeat fails
		return nil
	}
	if (currentTerm < arguments.Term) { //this node is behind
		currentTerm = arguments.Term
		votedFor = -1
	}
	role = "follower"
	reply.Term = currentTerm
	reply.Success = true
	resetElectionTimer()
	return nil
}

// Helper to reset the election timer for current node
func resetElectionTimer(){
	timerVal := time.Duration(200+rand.Intn(100)) * time.Millisecond //random timer value
	if electionTimer == nil {
		electionTimer = time.NewTimer(timerVal) //initialize new timer
		go func() {
			for{ //infinite while loop for timer
				<- electionTimer.C
				if role != "leader" {
					fmt.Printf("\n[%s] Node %d timed out, starting election\n", time.Now().Format("15:04:05.000000"), selfID)
					LeaderElection() //election start when timer runs out
					resetElectionTimer()
				}
			}
		}()
	} else { //timer already exists
		electionTimer.Stop()
		electionTimer.Reset(timerVal)
	}
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func LeaderElection() {
	role = "candidate"
	currentTerm +=1 //update term
	var voteMu sync.Mutex
	votes = 1 //self-vote automatically
	voteArgs := VoteArguments{
		Term: currentTerm, 
		CandidateID: selfID, 
		LastLogIndex: len(raftLog)-1, 
		LastLogTerm: lastLogTerm,
	}
	//call RequestVote from all nodes
	var wg sync.WaitGroup //asynchronous messaging
	fmt.Printf("\n[%s] Started new election\n", time.Now().Format("15:04:05.000000"))
	for _, conn := range serverNodes{ //conn = ServerConnection for this ID (in serverConnections map)
		fmt.Printf("\n[%s] Requesting vote from node %d\n", time.Now().Format("15:04:05.000000"), conn.serverID)
		wg.Add(1)
		go func(c ServerConnection){ //async: notify each node in a new goroutine
			var reply VoteReply
			defer wg.Done()
			err := c.rpcConnection.Call("RaftNode.RequestVote", &voteArgs, &reply)
			if err != nil {
				log.Println("Error with RequestVote:", err)
				return
			}
			if (reply.Term > currentTerm) { //if candidate is behind, abort
				currentTerm = reply.Term
				votedFor = -1
				role = "follower"
				return
			}
			if reply.ResultVote{
				voteMu.Lock() //protect vote count access
				votes +=1
				voteMu.Unlock()
			}
			fmt.Printf("\n[%s] Received reply: %+v", time.Now().Format("15:04:05.000000"), reply)
		}(conn)
	}
	wg.Wait()
	if (votes > (len(serverNodes)+1)/2){ //majority count
		fmt.Printf("\nNode %d successfully elected self.", selfID)
		role = "leader"
		go Heartbeat()
	}
}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat() {
	for role == "leader"{
		//call AppendEntry from all nodes (a.k.a. send heartbeat)
		for _, conn := range serverNodes {
			go func(c ServerConnection){  //async: each heartbeat in a new goroutine
				var reply AppendEntryReply
				appendArgs := AppendEntryArgument{
					Term: currentTerm,
					LeaderID: selfID,
				}
				err := c.rpcConnection.Call("RaftNode.AppendEntry", &appendArgs, &reply)
				if err != nil {
					log.Println("Heartbeat failed to node ", c.serverID)
					return
				}
				if reply.Success == false { //if a follower is ahead, resign
					currentTerm = reply.Term
					votedFor = -1
					role = "follower"
					resetElectionTimer()
				}
			}(conn)
		}
		time.Sleep(50*time.Millisecond)
	}
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

	resetElectionTimer() //starts heartbeat process, calls LeaderElection inside
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
