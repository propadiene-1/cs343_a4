package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	netRPC "net/rpc"
	"os"
	"time"
)

// Object type to manage connections to the cluster
type SystemConnection struct {
	connection    *netRPC.Client
	addressString string
}

var connectionPoint SystemConnection

type ClientArguments struct {
	VariableName string  // ex. "x", "name"
	CommandType  string  // "R" for read, "W" for write
	Data         *string // could also be integer if you are working with numbers
	// Note: if you want to allow bopth types, there is a way, but you will need to look it up :)
}

type ClientReply struct {
	DataValue string // could also be integer if you are working with numbers
	// If the ClientArguments had the CommandType "R", you should find the returned value here
	WrongLeader   bool   // Finding this set to true means that client needs to connect to the correct leader IP:port address provided in the reply
	LeaderAddress string // the IP:port of the true leader
	// This is only used when the client is attempting to connet to the leader in the cluster
}

// Function that calls some RPC at the leader's raft node to perform a write operation
// Note that I wrote this function to work with strings
// You can modify this if you want
func WriteValue(name string, val *string) error {

	// Note the following realistic setup:
	// The client is supposed to send their commands to the leader node only
	// However, the client has no idea at any given moment in time who is the leader
	// So, the client randomly chooses a raft node to connect to (done in main)
	// If the call results in a FoundLeader = False, the client needs to connect to the right leader at that time and resend the request

	theArgs := ClientArguments{
		VariableName: name,
		CommandType:  "W",
		Data:         val,
	}
	theReply := ClientReply{}

	// Make the RPC with appropriate arguments
	// TODO: You should add the corresponding RPC func to your RaftNode implementation
	err := connectionPoint.connection.Call("RaftNode.ClientAddToLog", theArgs, &theReply)
	if err != nil {
		log.Fatal("RPC error:", err)
	}

	fmt.Println("yay!")
	// First check if node was indeed the current leader
	if theReply.WrongLeader {
		fmt.Println("OPS, got wrong leader")
		fmt.Println(theReply.LeaderAddress)
		// If not, renew connection object
		// Connect to the indicated leader node
		leaderConnection, err := netRPC.DialHTTP("tcp", theReply.LeaderAddress)
		if err != nil {
			log.Fatal("Problem with dialing the new leader:", err)
		}
		// Save new connection information
		connectionPoint = SystemConnection{leaderConnection, theReply.LeaderAddress}

		// Try the write command again
		// Make the RPC with appropriate arguments
		theReply = ClientReply{}
		err = connectionPoint.connection.Call("RaftNode.ClientAddToLog", theArgs, &theReply)
		if err != nil {
			log.Fatal("RPC error:", err)
		}

		// This next part is optional:
		// You can keep checking if the leader is correct or not
		// If you want to do that, then I recommend switching the if condition above to a loop
		// Keep trying until leader is found

		// In this code, I am choosing to simply return an error if the leader is not found after the second attempt
		if theReply.WrongLeader {
			return errors.New("Couldn't get to leader")
		}
	}
	return nil
}

func ReadValue(variableName string, dataValue *string) error {
	// Note the following realistic setup:
	// The client is supposed to send their commands to the leader node only
	// However, the client has no idea at any given moment in time who is the leader
	// So, the client randomly chooses a raft node to connect to (done in main)
	// If the call results in a FoundLeader = False, the client needs to connect to the right leader at that time and resend the request

	theArgs := ClientArguments{
		VariableName: variableName,
		CommandType:  "R",
	}
	theReply := ClientReply{}

	// Make the RPC with appropriate arguments
	// TODO: You should add the corresponding RPC func to your RaftNode implementation
	err := connectionPoint.connection.Call("RaftNode.ClientAddToLog", theArgs, &theReply)
	if err != nil {
		log.Fatal("RPC error:", err)
	}

	// First check if node was indeed the current leader
	if theReply.WrongLeader {
		// If not, renew connection object
		// Connect to the indicated leader node
		client, err := netRPC.DialHTTP("tcp", theReply.LeaderAddress)
		if err != nil {
			log.Fatal("Problem with dialing:", err)
		}
		// Save new connection information
		connectionPoint = SystemConnection{client, theReply.LeaderAddress}

		// Try the read command again
		// Make the RPC with appropriate arguments
		theReply = ClientReply{}
		err = connectionPoint.connection.Call("RaftNode.ClientAddToLog", theArgs, &theReply)
		if err != nil {
			log.Fatal("RPC error:", err)
		}

		// This next part is optional:
		// You can keep checking if the leader is correct or not
		// If you want to do that, then I recommend switching the if condition above to a loop
		// Keep trying until leader is found

		// In this code, I am choosing to simply return an error if the leader is not found after the second attempt
		if theReply.WrongLeader {
			return errors.New("Couldn't get to leader")
		}

		// If you get to this point in the code, it means that the read operation was successful
		*dataValue = theReply.DataValue
	}
	return nil
}

func connectToNode(fileName string) {
	// --- Read the IP:port info from the cluster configuration file
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Choose a random cluster node to connect to
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	IPAddress := lines[r.Intn(len(lines))]
	log.Println("Chose %s from cluster", IPAddress)

	// Connect to the chosen node
	client, err := netRPC.DialHTTP("tcp", IPAddress)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// Save connection information
	connectionPoint = SystemConnection{client, IPAddress}
}

func main() {
	arguments := os.Args
	// The only value sent should be the cluster file
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Set up a connection to a random node in the cluster
	connectToNode(arguments[1])

	// Here y"ou can add code to test your system with a sequence of read and write operations
	thevaL := "HI\n"
	err := WriteValue("x", &thevaL)
	fmt.Println(err)
}
