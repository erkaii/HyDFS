package main

import (
	"HyDFS/failuredetector"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
)

const (
	REP_NUM      = 3
	MAX_FILE_NUM = 1000
	MAX_SERVER   = 10
)

type File struct {
	filename string // Gives the path to local file on the server
}

type FileServer struct {
	aliveml   *failuredetector.MembershipList
	pred_list [REP_NUM]int
	files     [MAX_FILE_NUM]File
	id        int
}

func FileServerInit(ml *failuredetector.MembershipList, id int) *FileServer {
	return &FileServer{
		id:      id,
		files:   [MAX_FILE_NUM]File{},
		aliveml: ml,
	}
}

// Common functionalities shared by server and client
func HashKey(input string) int {
	// Create a new SHA256 hash
	hash := sha256.New()
	// Write the input string as bytes to the hash
	hash.Write([]byte(input))
	// Get the resulting hash as a byte slice
	hashedBytes := hash.Sum(nil)
	// Convert the hash bytes to a hexadecimal string
	hashString := hex.EncodeToString(hashedBytes)

	// Convert the hex string to a big.Int
	bigIntHash := new(big.Int)
	bigIntHash.SetString(hashString, 16)

	// Mod the big.Int by 1000 and add 1 to map to the range [1, 1000]
	result := bigIntHash.Mod(bigIntHash, big.NewInt(1000)).Int64() + 1

	return int(result)
}

func id_to_domain(id int) string {
	return "fa24-cs425-68" + fmt.Sprintf("%02d", id) + ".cs.illinois.edu"
}

// Client side funcs
func CreateFileClient(localfilename string, HyDFSfilename string, myDomain string) error {

	//server_id := HashKey(HyDFSfilename)
	server_id := 3

	// Iterate through the possible server ids to find the server to send request to
	for i := 0; i < MAX_SERVER; i++ {
		// Make use of Ping feature from failure detector
		s := failuredetector.NewSender(id_to_domain(server_id), failuredetector.PingPort, myDomain)
		fmt.Println(id_to_domain(server_id))
		err := s.Ping(failuredetector.Timeout)
		if err == nil { // Ping succeeded
			break
		}

		server_id = server_id%MAX_SERVER + 1 // I know this looks suspicious
		if i == MAX_SERVER-1 {
			return errors.New("No server alive!")
		}
	}

	// Send a create request to the communication server
	client, err := rpc.DialHTTP("tcp", id_to_domain(server_id)+":3333")
	if err != nil {
		log.Fatal("Client dialing:", err)
	}

	args := &LR_files{localfilename, HyDFSfilename}
	var reply string
	err = client.Call("FService.CreateFile", args, &reply)
	if err != nil {
		log.Fatal("fservice error:", err)
	}

	fmt.Println("Triggering rpc and the remote filename is", reply)

	return nil
}

// Server side funcs
func FileServerLaunch(fs *FileServer) {
	fservice := new(FService)
	rpc.Register(fservice)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", ":3333")
	if err != nil {
		log.Fatal("Fileserver listen error:", err)
	}
	go http.Serve(l, nil)
}

type LR_files struct {
	Local, Remote string
}

type FService int

func (t *FService) CreateFile(args *LR_files, reply *string) error {
	*reply = args.Remote
	return nil
}
