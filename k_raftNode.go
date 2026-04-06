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

// Node state
var isLeader bool
var mu sync.Mutex

// resetElection is used to signal the election-timer goroutine that a valid
// heartbeat (or granted vote) was received, so the timer should restart.
var resetElection = make(chan struct{}, 1)

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Reply false if term < currentTerm
// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
// Hint 2: Only focus on the details related to leader election and majority votes
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
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
		isLeader = false
	}

	logOK := arguments.LastLogTerm > lastLogTerm || (arguments.LastLogTerm == lastLogTerm && arguments.LastLogIndex >= len(raftLog)-1)

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
	isLeader = false //current node is follower

	reply.Term = currentTerm
	reply.Success = true

	//reset election timer
	select {
	case resetElection <- struct{}{}:
	default:
	}
	fmt.Printf("\n[%s] Heartbeat received from leader %d (term %d)\n",
		time.Now().Format("15:04:05.000000"), arguments.LeaderID, arguments.Term)
	return nil
}

// 	if (currentTerm < arguments.Term){//node is behind
// 		currentTerm = arguments.Term //update term
// 		votedFor = -1
// 		isLeader = false
// 	}

// 	return nil
// }

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func LeaderElection() {
	mu.Lock()
	currentTerm += 1 //update term
	term := currentTerm
	votedFor = selfID
	mu.Unlock()

	var voteMu sync.Mutex
	votes = 1 //self-vote automatically

	voteArgs := VoteArguments{
		Term:         currentTerm,
		CandidateID:  selfID,
		LastLogIndex: len(raftLog) - 1,
		LastLogTerm:  lastLogTerm,
	}

	//call RequestVote from all nodes
	var wg sync.WaitGroup //asynchronous messaging
	fmt.Printf("\n[%s] Started new election\n", time.Now().Format("15:04:05.000000"))
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
				isLeader = false
				mu.Unlock()
				return
			}
			mu.Unlock()

			if reply.ResultVote {
				voteMu.Lock() //protect vote count access
				votes += 1
				voteMu.Unlock()
			}
			fmt.Printf("\n[%s] Received reply: %+v", time.Now().Format("15:04:05.000000"), reply)
		}(conn)
	}
	wg.Wait()
	mu.Lock()
	majority := (len(serverNodes)+1)/2 + 1
	wonElection := votes >= majority && currentTerm == term && !isLeader
	if wonElection {
		isLeader = true
	}
	mu.Unlock()
	if wonElection {
		fmt.Printf("\n[%s] Node %d elected leader for term %d (votes: %d/%d)\n",
			time.Now().Format("15:04:05.000000"), selfID, term, votes, len(serverNodes)+1)
		go Heartbeat()
	} else {
		fmt.Printf("\n[%s] Node %d lost election for term %d (votes: %d/%d)\n",
			time.Now().Format("15:04:05.000000"), selfID, term, votes, len(serverNodes)+1)
	}
}

// if (votes > (len(serverNodes)+1)/2){ //majority count
// 	fmt.Printf("\nNode %d successfully elected self.", selfID)
// 	//TODO: somehow start heartbeats, define leader role
// }

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat() {
	for {
		mu.Lock()
		if !isLeader {
			mu.Unlock()
			return
		}
		term := currentTerm
		mu.Unlock()
		args := AppendEntryArgument{
			Term:         term,
			LeaderID:     selfID,
			PrevLogIndex: len(raftLog) - 1,
			PrevLogTerm:  lastLogTerm,
		}

		for _, conn := range serverNodes {
			go func(c ServerConnection) {
				var reply AppendEntryReply
				err := c.rpcConnection.Call("RaftNode.AppendEntry", &args, &reply)

				if err != nil {
					log.Printf("Heartbeat to node %d failed: %v\n", c.serverID, err)
					return
				}

				mu.Lock()
				defer mu.Unlock()
				if reply.Term > currentTerm {
					currentTerm = reply.Term
					votedFor = -1
					isLeader = false
					fmt.Printf("\n[%s] Node %d stepping down (saw term %d)\n",
						time.Now().Format("15:04:05.000000"), selfID, reply.Term)
				}
			}(conn)
		}
		//Heartbeat interval: 100 ms
		time.Sleep(100 * time.Millisecond)
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
	isLeader = false

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
				leader := isLeader
				mu.Unlock()
				if !leader {
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
