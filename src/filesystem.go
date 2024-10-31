package main

import (
	"HyDFS/failuredetector"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	REP_NUM    = 3
	MAX_SERVER = 10
)

type File struct {
	filename string // Gives the path to local file on the server
}

type FileServer struct {
	aliveml   *failuredetector.MembershipList
	pred_list [REP_NUM]int
	files     map[string]File
	id        int
}

func FileServerInit(ml *failuredetector.MembershipList, id int) *FileServer {
	return &FileServer{
		id:      id,
		files:   make(map[string]File),
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

func findRange(lints []int, k int) int {
	n := len(lints)
	if n == 0 || k < 1 || k > 1000 {
		return -1 // Return -1 for invalid input
	}

	// Calculate the range size
	rangeSize := 1000 / n

	// Determine which range `k` falls into
	for i := 0; i < n; i++ {
		lowerBound := i*rangeSize + 1
		upperBound := (i + 1) * rangeSize
		if i == n-1 {
			upperBound = 1000 // Ensure last range includes 1000
		}
		if k >= lowerBound && k <= upperBound {
			return lints[i]
		}
	}
	return -1 // If `k` is out of range, though unlikely
}

// Hash the filename to determine which server will handle the file.
// Ping servers to find one that is alive and reachable.
// Make an RPC call to check if the file already exists on the server.
// If the file does not exist, upload the file using an HTTP POST request.
// Receive confirmation from the server that the file was successfully uploaded.

// Client side funcs
func CreateFileClient(localfilename string, HyDFSfilename string, myDomain string) error {

	//server_id := HashKey(HyDFSfilename)
	// list of servers for consistent hashing
	list := make([]int, MAX_SERVER)
	for i := 0; i < MAX_SERVER; i++ {
		list[i] = i + 1
	}
	//findrange finds the server id on which the file will be created and stored; mapping file on to a node on ring
	// this server id acts as introducer, which checks if I'm the one to handle this request or some else should do it.this is done in search Files.
	server_id := findRange(list, HashKey(HyDFSfilename))

	// Iterate through the possible server ids to find the server to send request to
	// server_id may not be alive, so you first ping ideal server_id, if alive ok. else, you know the files exist in the server_id's successor in the ring, so you ping that server_id in next iteration
	for i := 0; i < MAX_SERVER; i++ {
		// Make use of Ping feature from failure detector
		//ping ideal server id
		s := failuredetector.NewSender(id_to_domain(server_id), failuredetector.PingPort, myDomain)
		fmt.Println(id_to_domain(server_id))
		err := s.Ping(failuredetector.Timeout)
		//if ideal server is alive then proceed to file tcp connection and search if file exist
		if err == nil { // Ping succeeded
			break
		}
		//else find the successor of ideal server to seaerh for file in the next iteration.
		server_id = server_id%MAX_SERVER + 1 // I know this looks suspicious
		if i == MAX_SERVER-1 {
			return errors.New("No server alive!")
		}
	}

	client, err := rpc.DialHTTP("tcp", id_to_domain(server_id)+":3333")
	if err != nil {
		log.Fatal("Client dialing:", err)
	}

	args := &LR_files{localfilename, HyDFSfilename}
	var vm_id int
	err = client.Call("FService.SearchFile", args, &vm_id)
	if err != nil {
		return err
	}
	// to indicate server_id handles the file and it already has the file
	if vm_id == -1 {
		return errors.New("File already exists!")
	}
	//  if server_id isnt the one to handle file, it gives different result so dial the correct server to prcoess file
	if vm_id != server_id {
		client, err = rpc.DialHTTP("tcp", id_to_domain(vm_id)+":3333")
		if err != nil {
			log.Fatal("Client dialing:", err)
		}

		// args = &LR_files{localfilename, HyDFSfilename}
		err = client.Call("FService.SearchFile", args, &vm_id)
		if err != nil {
			return err
		}

		if vm_id == -1 {
			return errors.New("File already exists!")
		}
	}
	//if server_id is the real server to prcoess the file and it does not have file yet, then we should upload file through http
	// Now start writing to the file.
	fmt.Println("Yes, you can write to vm", vm_id)

	// Open the local file to read its content
	fileContent, err := os.ReadFile(localfilename)
	if err != nil {
		fmt.Println("Failed to open local file!")
		return fmt.Errorf("failed to read local file: %v", err)
	}

	// Prepare HTTP POST request
	url := fmt.Sprintf("http://%s:8080/upload", id_to_domain(vm_id))
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(fileContent))
	if err != nil {
		fmt.Println("Failed to create HTTP request!")
		return fmt.Errorf("failed to create HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("HyDFSfilename", HyDFSfilename)

	// Send the HTTP request
	clienthttp := &http.Client{}
	resp, err := clienthttp.Do(req)
	if err != nil {
		fmt.Println("Failed to send HTTP request")
		return fmt.Errorf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// Check response from the server
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("Error from server:", string(body))
		return fmt.Errorf("server error: %s", string(body))
	}

	fmt.Println("File successfully uploaded to server.")

	return nil
}

func GetFileClient(localfilename string, HyDFSfilename string, myDomain string) error {

	//server_id := HashKey(HyDFSfilename)
	list := make([]int, MAX_SERVER)
	for i := 0; i < MAX_SERVER; i++ {
		list[i] = i + 1
	}
	server_id := findRange(list, HashKey(HyDFSfilename))

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

	client, err := rpc.DialHTTP("tcp", id_to_domain(server_id)+":3333")
	if err != nil {
		log.Fatal("Client dialing:", err)
	}

	args := &LR_files{localfilename, HyDFSfilename}
	var vm_id int
	err = client.Call("FService.SearchFile", args, &vm_id)
	if err != nil {
		return err
	}

	if vm_id == -1 {
		return errors.New("File already exists!")
	}

	if vm_id != server_id {
		client, err = rpc.DialHTTP("tcp", id_to_domain(vm_id)+":3333")
		if err != nil {
			log.Fatal("Client dialing:", err)
		}

		// args = &LR_files{localfilename, HyDFSfilename}
		err = client.Call("FService.SearchFile", args, &vm_id)
		if err != nil {
			return err
		}

		if vm_id == -1 {
			return errors.New("File already exists!")
		}
	}

	// Now start writing to the file.
	fmt.Println("Yes, you can write to vm", vm_id)

	// Open the local file to read its content
	fileContent, err := os.ReadFile(localfilename)
	if err != nil {
		fmt.Println("Failed to open local file!")
		return fmt.Errorf("failed to read local file: %v", err)
	}

	// Prepare HTTP POST request
	url := fmt.Sprintf("http://%s:8080/upload", id_to_domain(vm_id))
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(fileContent))
	if err != nil {
		fmt.Println("Failed to create HTTP request!")
		return fmt.Errorf("failed to create HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("HyDFSfilename", HyDFSfilename)

	// Send the HTTP request
	clienthttp := &http.Client{}
	resp, err := clienthttp.Do(req)
	if err != nil {
		fmt.Println("Failed to send HTTP request")
		return fmt.Errorf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// Check response from the server
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("Error from server:", string(body))
		return fmt.Errorf("server error: %s", string(body))
	}

	fmt.Println("File successfully uploaded to server.")

	return nil
}

// Server side funcs
func FileServerLaunch(fs *FileServer) {
	fservice := &FService{fs: fs}
	rpc.Register(fservice)
	rpc.HandleHTTP()

	// HTTP file upload handler
	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
			return
		}

		// Retrieve the target filename from the headers
		remoteFilename := r.Header.Get("HyDFSfilename")
		if remoteFilename == "" {
			http.Error(w, "HyDFSfilename header missing", http.StatusBadRequest)
			return
		}

		// Read the file content from the request body
		fileContent, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read file content", http.StatusInternalServerError)
			return
		}

		// Write the content to a file with the specified name
		err = os.WriteFile(remoteFilename, fileContent, 0644)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to write file: %v", err), http.StatusInternalServerError)
			return
		}

		fmt.Fprintln(w, "File successfully received and saved.")
	})

	l, err := net.Listen("tcp", ":3333")
	if err != nil {
		log.Fatal("Fileserver listen error:", err)
	}
	go http.Serve(l, nil)

	// HTTP server for file uploads on port 8080
	log.Fatal(http.ListenAndServe(":8080", nil))
}

type LR_files struct {
	Local, Remote string
}

type FService struct {
	fs *FileServer
}

// server_id validates if it is the one to handle the file or someone else in the full membership list
func (t *FService) SearchFile(args *LR_files, reply *int) error {
	//hashing of the alive members in the list
	*reply = findRange(t.fs.aliveml.Alive_Ids(), HashKey(args.Remote))
	//if server_id is the one to process the file -validation
	if *reply == t.fs.id {
		_, exists := t.fs.files[args.Remote]
		//if file exists, do not allow another file write
		if exists {
			*reply = -1
		} else {
			//proceed to accept upload of the file.
			t.fs.files[args.Remote] = File{filename: args.Remote}
		}
	}
	return nil
}
