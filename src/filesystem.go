package main

import (
	"HyDFS/failuredetector"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	REP_NUM          = 3
	MAX_SERVER       = 10
	HTTP_PORT        = "4444"
	FILE_PATH_PREFIX = "../files/server/"
)

type File struct {
	filename string // Gives the path to local file on the server
}

type FileServer struct {
	aliveml            *failuredetector.MembershipList
	pred_list          [REP_NUM]int
	succ_list          [REP_NUM]int
	p_files            map[string]File
	r_files            map[string]File
	id                 int
	online             bool
	Mutex              sync.RWMutex
	coord_append_queue map[string]int
}

func FileServerInit(ml *failuredetector.MembershipList, id int) *FileServer {
	return &FileServer{
		id:                 id,
		p_files:            make(map[string]File),
		r_files:            make(map[string]File),
		aliveml:            ml,
		pred_list:          [REP_NUM]int{0},
		succ_list:          [REP_NUM]int{0},
		online:             false,
		coord_append_queue: make(map[string]int),
	}
}

func hashKey(input string) int {
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

func findServerByfileID(ids []int, fileID int) int {
	server_id := -1
	min := 1000
	for _, i := range ids {
		if (i*100+1000-fileID)%1000 < min {
			server_id = i
			min = (i*100 + 1000 - fileID) % 1000
		}
	}
	return server_id
}

func id_to_domain(id int) string {
	return "fa24-cs425-68" + fmt.Sprintf("%02d", id) + ".cs.illinois.edu"
}

// Maintenance Thread
func Maintenance(fs *FileServer) {
	for {
		fs.Mutex.Lock()
		if len(fs.aliveml.Alive_Ids()) == MAX_SERVER {
			fs.online = true
		}
		if !fs.online {
			fs.Mutex.Unlock()
			time.Sleep(time.Second)
			continue
		}
		fs.Mutex.Unlock()

		time.Sleep(time.Second)
	}
}

// ------------------------- HTTP Handler -------------------------//
// Function to start HTTP server
func HTTPServer(fs *FileServer) {

	http.HandleFunc("/", fs.httpHandleSlash)                // Handle slash request (used when client search coordinator servers)
	http.HandleFunc("/create", fs.httpHandleCreate)         // Handle file creation requests
	http.HandleFunc("/membership", fs.httpHandleMembership) // Return ids of online servers
	http.HandleFunc("/online", fs.httpHandleOnline)         // Return YES/NO to indicate online/offline
	http.HandleFunc("/append", fs.httpHandleAppend)

	fmt.Println("Starting HTTP server on :" + HTTP_PORT)
	log.Fatal(http.ListenAndServe(":"+HTTP_PORT, nil))
}

// HTTP handler functions
func (fs *FileServer) httpHandleCreate(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req map[string]string
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Access file1 and file2 directly from the map
		_, localExists := req["local"]
		hydfs, hydfsExists := req["hydfs"]
		if !localExists || !hydfsExists {
			http.Error(w, "Missing localfilename or HyDFSfilename in request", http.StatusBadRequest)
			return
		}

		// Find out the primary server of the HyDFS file
		fileID := hashKey(hydfs)
		responsible_server_id := findServerByfileID(fs.aliveml.Alive_Ids(), fileID)
		if responsible_server_id == -1 {
			log.Println("Invalid findServerByfileID result in httpHandleCreate")
			http.Error(w, "Rejected due to server internal error", http.StatusBadRequest)
			return
		}

		// Check if allowed to create
		if true {
			// Write the request into a cache
			fs.Mutex.Lock()
			defer fs.Mutex.Unlock()

			fs.coord_append_queue[hydfs] = responsible_server_id

			fmt.Fprintf(w, "Authorized")
		} else {
			http.Error(w, "Rejected, file "+hydfs+" already exists", http.StatusBadRequest)
		}
		return
	case http.MethodPut:
		filename := r.URL.Query().Get("filename")
		if filename == "" {
			http.Error(w, "HyDFS Filename not specified", http.StatusBadRequest)
			return
		}

		// Read the file content from the request body
		fileContent, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read file content from request", http.StatusInternalServerError)
			return
		}

		fs.Mutex.Lock()
		responsible_server_id, exist := fs.coord_append_queue[filename]
		fs.Mutex.Unlock()

		if !exist {
			http.Error(w, "Invalid upload, file creation not allowed", http.StatusBadRequest)
			return
		}
		// Create a new request to the external server
		url := fmt.Sprintf("http://%s:%s/append?filename=%s", id_to_domain(responsible_server_id), HTTP_PORT, filename)
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(fileContent))
		if err != nil {
			http.Error(w, "Failed to create request to external server", http.StatusInternalServerError)
			return
		}

		// Send the request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "Failed to send request to external server", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Check if the external server responded successfully
		if resp.StatusCode != http.StatusOK {
			http.Error(w, "External server error: "+resp.Status, resp.StatusCode)
			return
		}

		fmt.Fprint(w, "File uploaded to external server "+id_to_domain(responsible_server_id)+" successfully")
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleSlash(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Write([]byte{})
		return
	case http.MethodPost:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleMembership(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ids := fs.aliveml.Alive_Ids()
		message := "No member in the file system??"
		if len(ids) != 0 {
			strs := make([]string, len(ids))
			for i, num := range ids {
				strs[i] = strconv.Itoa(num) // Convert each int to string
			}
			message = strings.Join(strs, ", ")
		}
		w.Write([]byte(message))
		return
	case http.MethodPost:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleOnline(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		fs.Mutex.Lock()
		defer fs.Mutex.Unlock()
		if fs.online {
			w.Write([]byte("Yes"))
		} else {
			w.Write([]byte("No"))
		}
		return
	case http.MethodPost:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleAppend(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		// Get filename from query parameters
		filename := r.URL.Query().Get("filename")
		if filename == "" {
			http.Error(w, "Filename not specified", http.StatusBadRequest)
			return
		}

		// Open the file in append mode, or create it if it doesn't exist
		file, err := os.OpenFile(FILE_PATH_PREFIX+filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			http.Error(w, "Failed to open or create file", http.StatusInternalServerError)
			return
		}
		defer file.Close()

		// Write the received content to the file
		_, err = io.Copy(file, r.Body)
		if err != nil {
			http.Error(w, "Failed to write content to file", http.StatusInternalServerError)
			return
		}

		// Respond to confirm the operation was successful
		fmt.Fprint(w, "File content appended successfully")
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
